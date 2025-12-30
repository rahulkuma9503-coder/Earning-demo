import os
import logging
import asyncio
import sys
from datetime import datetime
from typing import List, Dict, Optional
import json
import threading
import atexit
from dotenv import load_dotenv

from telegram import (
    Update, 
    InlineKeyboardButton, 
    InlineKeyboardMarkup,
    BotCommand
)
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    MessageHandler,
    ContextTypes,
    filters
)
from telegram.constants import ParseMode
import pymongo
from pymongo import MongoClient, errors
from concurrent.futures import ThreadPoolExecutor

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Configuration
BOT_TOKEN = os.getenv('BOT_TOKEN')
ADMIN_IDS = list(map(int, os.getenv('ADMIN_IDS', '').split(','))) if os.getenv('ADMIN_IDS') else []
PORT = int(os.getenv('PORT', 8080))
MONGODB_URI = os.getenv('MONGODB_URI', 'mongodb://localhost:27017/')

# Environment variable for initial channels - properly parsed
INITIAL_CHANNELS_ENV = os.getenv('INITIAL_CHANNELS', '')
if INITIAL_CHANNELS_ENV:
    # Split by comma and clean up empty entries
    INITIAL_CHANNELS = [cid.strip() for cid in INITIAL_CHANNELS_ENV.split(',') if cid.strip()]
else:
    INITIAL_CHANNELS = []

logger.info(f"📢 Initial channels from env: {INITIAL_CHANNELS}")
logger.info(f"🌐 MongoDB URI configured: {bool(MONGODB_URI)}")

# Global variables for database
mongo_client = None
channels_collection = None
users_collection = None
referrals_collection = None
pending_referrals_collection = None  # NEW: For tracking pending referrals

# Thread pool for blocking operations
executor = ThreadPoolExecutor(max_workers=10)

def init_database():
    """Initialize MongoDB connection"""
    global mongo_client, channels_collection, users_collection, referrals_collection, pending_referrals_collection
    
    if not MONGODB_URI:
        logger.warning("⚠️ MONGODB_URI not set. Using file-based storage.")
        return False
    
    try:
        logger.info(f"🔗 Attempting to connect to MongoDB...")
        
        # Check if URI contains SRV format (mongodb+srv://)
        if "mongodb+srv://" in MONGODB_URI:
            # For SRV connections, we need to handle differently
            logger.info("📡 Using MongoDB SRV connection")
            mongo_client = MongoClient(
                MONGODB_URI,
                serverSelectionTimeoutMS=10000,
                connectTimeoutMS=30000,
                socketTimeoutMS=30000,
                retryWrites=True,
                w="majority"
            )
        else:
            # Standard MongoDB connection
            logger.info("📡 Using standard MongoDB connection")
            mongo_client = MongoClient(
                MONGODB_URI,
                serverSelectionTimeoutMS=10000,
                connectTimeoutMS=30000,
                socketTimeoutMS=30000,
                maxPoolSize=50
            )
        
        # Test connection
        logger.info("🔄 Testing MongoDB connection...")
        mongo_client.server_info()
        
        db = mongo_client.get_database('telegram_referral_bot')
        
        # Initialize collections
        channels_collection = db['channels']
        users_collection = db['users']
        referrals_collection = db['referrals']
        pending_referrals_collection = db['pending_referrals']  # NEW
        
        # Create indexes
        users_collection.create_index('user_id', unique=True)
        channels_collection.create_index('chat_id', unique=True)
        referrals_collection.create_index([('referrer_id', 1), ('referred_id', 1)], unique=True)
        pending_referrals_collection.create_index('referred_id', unique=True)  # NEW
        pending_referrals_collection.create_index('referrer_id')  # NEW
        pending_referrals_collection.create_index('created_at', expireAfterSeconds=604800)  # Auto-delete after 7 days
        
        logger.info("✅ MongoDB connected successfully")
        return True
        
    except errors.ServerSelectionTimeoutError as e:
        logger.error(f"❌ MongoDB connection timeout: {e}")
        logger.warning("📁 Using file-based storage as fallback")
        return False
    except errors.ConnectionFailure as e:
        logger.error(f"❌ MongoDB connection failed: {e}")
        logger.warning("📁 Using file-based storage as fallback")
        return False
    except Exception as e:
        logger.error(f"❌ MongoDB error: {e}")
        logger.warning("📁 Using file-based storage as fallback")
        return False

# Initialize database
db_connected = init_database()

class Storage:
    """Storage manager with MongoDB and file fallback"""
    
    @staticmethod
    async def save_channels(channels: List[Dict]):
        """Save channels to storage asynchronously"""
        try:
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(executor, Storage._save_channels_sync, channels)
        except Exception as e:
            logger.error(f"Error saving channels: {e}")
    
    @staticmethod
    def _save_channels_sync(channels: List[Dict]):
        """Synchronous save channels"""
        try:
            if mongo_client is not None and channels_collection is not None:
                # Clear and insert all channels
                channels_collection.delete_many({})
                if channels:
                    channels_collection.insert_many(channels)
            else:
                # Fallback to file
                with open('channels_backup.json', 'w') as f:
                    json.dump(channels, f, default=str)
        except Exception as e:
            logger.error(f"Error in sync save_channels: {e}")
    
    @staticmethod
    async def load_channels() -> List[Dict]:
        """Load channels from storage asynchronously"""
        try:
            loop = asyncio.get_event_loop()
            return await loop.run_in_executor(executor, Storage._load_channels_sync)
        except Exception as e:
            logger.error(f"Error loading channels: {e}")
            return []
    
    @staticmethod
    def _load_channels_sync() -> List[Dict]:
        """Synchronous load channels"""
        try:
            if mongo_client is not None and channels_collection is not None:
                # Load from MongoDB
                channels = list(channels_collection.find({}, {'_id': 0}))
                return channels
            else:
                # Fallback from file
                if os.path.exists('channels_backup.json'):
                    with open('channels_backup.json', 'r') as f:
                        return json.load(f)
                return []
        except Exception as e:
            logger.error(f"Error in sync load_channels: {e}")
            return []
    
    @staticmethod
    async def save_users(users: Dict):
        """Save users to storage asynchronously"""
        try:
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(executor, Storage._save_users_sync, users)
        except Exception as e:
            logger.error(f"Error saving users: {e}")
    
    @staticmethod
    def _save_users_sync(users: Dict):
        """Synchronous save users"""
        try:
            if mongo_client is not None and users_collection is not None:
                # Update or insert each user
                for user_id, user_data in users.items():
                    users_collection.update_one(
                        {'user_id': int(user_id)},
                        {'$set': user_data},
                        upsert=True
                    )
            else:
                # Fallback to file
                with open('users_backup.json', 'w') as f:
                    json.dump(users, f, default=str)
        except Exception as e:
            logger.error(f"Error in sync save_users: {e}")
    
    @staticmethod
    async def load_users() -> Dict:
        """Load users from storage asynchronously"""
        try:
            loop = asyncio.get_event_loop()
            return await loop.run_in_executor(executor, Storage._load_users_sync)
        except Exception as e:
            logger.error(f"Error loading users: {e}")
            return {}
    
    @staticmethod
    def _load_users_sync() -> Dict:
        """Synchronous load users"""
        try:
            if mongo_client is not None and users_collection is not None:
                # Load from MongoDB
                users = {}
                cursor = users_collection.find({})
                for user in cursor:
                    user_id = user.get('user_id')
                    if user_id:
                        # Remove MongoDB _id field
                        user_dict = {k: v for k, v in user.items() if k != '_id'}
                        users[str(user_id)] = user_dict
                return users
            else:
                # Fallback from file
                if os.path.exists('users_backup.json'):
                    with open('users_backup.json', 'r') as f:
                        return json.load(f)
                return {}
        except Exception as e:
            logger.error(f"Error in sync load_users: {e}")
            return {}
    
    @staticmethod
    async def save_referrals(referrals: Dict):
        """Save referrals to storage asynchronously"""
        try:
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(executor, Storage._save_referrals_sync, referrals)
        except Exception as e:
            logger.error(f"Error saving referrals: {e}")
    
    @staticmethod
    def _save_referrals_sync(referrals: Dict):
        """Synchronous save referrals"""
        try:
            if mongo_client is not None and referrals_collection is not None:
                # Clear and insert all referrals
                referrals_collection.delete_many({})
                referrals_list = []
                for referred_id, referrer_id in referrals.items():
                    referrals_list.append({
                        'referred_id': int(referred_id),
                        'referrer_id': int(referrer_id),
                        'created_at': datetime.now()
                    })
                if referrals_list:
                    referrals_collection.insert_many(referrals_list)
            else:
                # Fallback to file
                with open('referrals_backup.json', 'w') as f:
                    json.dump(referrals, f, default=str)
        except Exception as e:
            logger.error(f"Error in sync save_referrals: {e}")
    
    @staticmethod
    async def load_referrals() -> Dict:
        """Load referrals from storage asynchronously"""
        try:
            loop = asyncio.get_event_loop()
            return await loop.run_in_executor(executor, Storage._load_referrals_sync)
        except Exception as e:
            logger.error(f"Error loading referrals: {e}")
            return {}
    
    @staticmethod
    def _load_referrals_sync() -> Dict:
        """Synchronous load referrals"""
        try:
            if mongo_client is not None and referrals_collection is not None:
                # Load from MongoDB
                referrals = {}
                cursor = referrals_collection.find({})
                for ref in cursor:
                    referred_id = ref.get('referred_id')
                    referrer_id = ref.get('referrer_id')
                    if referred_id and referrer_id:
                        referrals[str(referred_id)] = str(referrer_id)
                return referrals
            else:
                # Fallback from file
                if os.path.exists('referrals_backup.json'):
                    with open('referrals_backup.json', 'r') as f:
                        return json.load(f)
                return {}
        except Exception as e:
            logger.error(f"Error in sync load_referrals: {e}")
            return {}
    
    # NEW: Pending referrals storage methods
    @staticmethod
    async def save_pending_referral(referrer_id: int, referred_id: int):
        """Save pending referral asynchronously"""
        try:
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(executor, Storage._save_pending_referral_sync, referrer_id, referred_id)
        except Exception as e:
            logger.error(f"Error saving pending referral: {e}")
    
    @staticmethod
    def _save_pending_referral_sync(referrer_id: int, referred_id: int):
        """Synchronous save pending referral"""
        try:
            if mongo_client is not None and pending_referrals_collection is not None:
                pending_referrals_collection.update_one(
                    {'referred_id': referred_id},
                    {'$set': {
                        'referrer_id': referrer_id,
                        'referred_id': referred_id,
                        'created_at': datetime.now()
                    }},
                    upsert=True
                )
            else:
                # Fallback to file
                pending_referrals = {}
                if os.path.exists('pending_referrals_backup.json'):
                    with open('pending_referrals_backup.json', 'r') as f:
                        pending_referrals = json.load(f)
                pending_referrals[str(referred_id)] = referrer_id
                with open('pending_referrals_backup.json', 'w') as f:
                    json.dump(pending_referrals, f, default=str)
        except Exception as e:
            logger.error(f"Error in sync save_pending_referral: {e}")
    
    @staticmethod
    async def remove_pending_referral(referred_id: int):
        """Remove pending referral asynchronously"""
        try:
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(executor, Storage._remove_pending_referral_sync, referred_id)
        except Exception as e:
            logger.error(f"Error removing pending referral: {e}")
    
    @staticmethod
    def _remove_pending_referral_sync(referred_id: int):
        """Synchronous remove pending referral"""
        try:
            if mongo_client is not None and pending_referrals_collection is not None:
                pending_referrals_collection.delete_one({'referred_id': referred_id})
            else:
                # Fallback to file
                if os.path.exists('pending_referrals_backup.json'):
                    with open('pending_referrals_backup.json', 'r') as f:
                        pending_referrals = json.load(f)
                    if str(referred_id) in pending_referrals:
                        del pending_referrals[str(referred_id)]
                        with open('pending_referrals_backup.json', 'w') as f:
                            json.dump(pending_referrals, f, default=str)
        except Exception as e:
            logger.error(f"Error in sync remove_pending_referral: {e}")
    
    @staticmethod
    async def get_pending_referrer(referred_id: int) -> Optional[int]:
        """Get pending referrer ID asynchronously"""
        try:
            loop = asyncio.get_event_loop()
            return await loop.run_in_executor(executor, Storage._get_pending_referrer_sync, referred_id)
        except Exception as e:
            logger.error(f"Error getting pending referrer: {e}")
            return None
    
    @staticmethod
    def _get_pending_referrer_sync(referred_id: int) -> Optional[int]:
        """Synchronous get pending referrer ID"""
        try:
            if mongo_client is not None and pending_referrals_collection is not None:
                pending = pending_referrals_collection.find_one({'referred_id': referred_id})
                if pending:
                    return pending.get('referrer_id')
                return None
            else:
                # Fallback to file
                if os.path.exists('pending_referrals_backup.json'):
                    with open('pending_referrals_backup.json', 'r') as f:
                        pending_referrals = json.load(f)
                    return pending_referrals.get(str(referred_id))
                return None
        except Exception as e:
            logger.error(f"Error in sync get_pending_referrer: {e}")
            return None

class DataManager:
    """Manage all data with storage persistence"""
    
    def __init__(self):
        self.channels = []
        self.users = {}
        self.referrals = {}
        self._lock = threading.Lock()  # Use threading lock for sync operations
        
        # Load data synchronously during initialization
        self._load_all_data_sync()
        
        # Initialize channels from environment variable
        self.init_channels_from_env()
        
        # Backup data on exit
        atexit.register(self._backup_all_data_sync)
    
    def _load_all_data_sync(self):
        """Load all data from storage synchronously"""
        logger.info("📂 Loading data from storage...")
        with self._lock:
            self.users = Storage._load_users_sync()
            self.referrals = Storage._load_referrals_sync()
        logger.info(f"✅ Loaded {len(self.users)} users, {len(self.referrals)} referrals")
    
    def init_channels_from_env(self):
        """Initialize channels from environment variable"""
        if INITIAL_CHANNELS:
            logger.info(f"📢 Initializing channels from environment variable: {INITIAL_CHANNELS}")
            valid_channels = 0
            for chat_id in INITIAL_CHANNELS:
                if chat_id and self.add_channel_from_env(chat_id):
                    valid_channels += 1
            logger.info(f"✅ Added {valid_channels} valid channels from environment")
        else:
            logger.warning("⚠️ No channels configured in INITIAL_CHANNELS environment variable")
    
    def add_channel_from_env(self, chat_id: str) -> bool:
        """Add channel from environment variable - returns True if successful"""
        try:
            if not chat_id or not isinstance(chat_id, str):
                logger.error(f"Invalid channel ID: {chat_id}")
                return False
            
            clean_id = chat_id.strip()
            
            if not clean_id:
                logger.error(f"Empty channel ID after stripping")
                return False
            
            logger.info(f"Processing channel ID: '{clean_id}'")
            
            # Format chat_id
            if clean_id.startswith('@'):
                # Username format: @username
                chat_id_str = clean_id
                logger.info(f"Channel is username format: {chat_id_str}")
            elif clean_id.startswith('-100'):
                # Channel ID format: -1001234567890
                chat_id_str = clean_id
                logger.info(f"Channel is channel ID format: {chat_id_str}")
            elif clean_id.startswith('-'):
                # Group ID format: -1234567890
                chat_id_str = clean_id
                logger.info(f"Channel is group ID format: {chat_id_str}")
            elif clean_id.isdigit() and len(clean_id) > 9:
                # Numeric channel ID without -100 prefix
                chat_id_str = f"-100{clean_id}"
                logger.info(f"Channel converted to: {chat_id_str}")
            else:
                logger.error(f"Invalid channel ID format: {clean_id}")
                return False
            
            # Check duplicate
            for channel in self.channels:
                if str(channel.get('chat_id')) == str(chat_id_str):
                    logger.info(f"Channel {chat_id_str} already exists")
                    return True
            
            # Get channel name (extract from username or use generic)
            if chat_id_str.startswith('@'):
                channel_name = chat_id_str.lstrip('@')
            else:
                channel_name = f"Channel {len(self.channels) + 1}"
            
            # Add channel
            channel = {
                'chat_id': chat_id_str,
                'name': channel_name,
                'added_at': datetime.now().isoformat()
            }
            self.channels.append(channel)
            logger.info(f"✅ Added channel: {channel_name} ({chat_id_str})")
            return True
            
        except Exception as e:
            logger.error(f"Error adding channel from env '{chat_id}': {e}")
            return False
    
    def _backup_all_data_sync(self):
        """Backup all data to storage synchronously"""
        logger.info("💾 Backing up data to storage...")
        with self._lock:
            Storage._save_channels_sync(self.channels)
            Storage._save_users_sync(self.users)
            Storage._save_referrals_sync(self.referrals)
        logger.info(f"✅ Data backed up: {len(self.channels)} channels, {len(self.users)} users, {len(self.referrals)} referrals")
    
    async def backup_all_data_async(self):
        """Backup all data to storage asynchronously"""
        logger.info("💾 Backing up data to storage (async)...")
        async with self._async_lock():
            await Storage.save_channels(self.channels)
            await Storage.save_users(self.users)
            await Storage.save_referrals(self.referrals)
        logger.info(f"✅ Data backed up (async): {len(self.channels)} channels, {len(self.users)} users, {len(self.referrals)} referrals")
    
    def _async_lock(self):
        """Create async lock for async operations"""
        class AsyncLock:
            def __init__(self, lock):
                self._lock = lock
            
            async def __aenter__(self):
                loop = asyncio.get_event_loop()
                await loop.run_in_executor(executor, self._lock.acquire)
                return self
            
            async def __aexit__(self, exc_type, exc, tb):
                loop = asyncio.get_event_loop()
                await loop.run_in_executor(executor, self._lock.release)
        
        return AsyncLock(self._lock)
    
    def get_stats(self) -> str:
        """Get data statistics - HTML format to avoid Markdown parsing issues"""
        total_balance = sum(u.get('balance', 0) for u in self.users.values())
        return (
            f"📊 <b>Database Statistics:</b>\n\n"
            f"📢 <b>Channels:</b> {len(self.channels)}\n"
            f"👥 <b>Users:</b> {len(self.users)}\n"
            f"🔗 <b>Referrals:</b> {len(self.referrals)}\n"
            f"💰 <b>Total Balance:</b> ₹{total_balance:.2f}\n"
            f"💾 <b>Storage:</b> {'✅ MongoDB' if db_connected else '📁 Local files'}"
        )

# Global data manager
data_manager = DataManager()

class ChannelManager:
    """Manage channels - Read-only from environment"""
    
    @staticmethod
    def get_channels() -> List[Dict]:
        return data_manager.channels

class UserManager:
    """Manage users with async operations"""
    
    @staticmethod
    async def get_user(user_id: int) -> Dict:
        """Get user data asynchronously"""
        user_str = str(user_id)
        
        async with data_manager._async_lock():
            if user_str in data_manager.users:
                return data_manager.users[user_str]
            
            # Create new user
            user_data = {
                'user_id': user_id,
                'balance': 0.0,
                'referral_code': f"REF{user_id}",
                'referral_count': 0,
                'total_earned': 0.0,
                'total_withdrawn': 0.0,
                'joined_at': datetime.now().isoformat(),
                'last_active': datetime.now().isoformat(),
                'transactions': [],
                'has_joined_channels': False,
                'welcome_bonus_received': False  # Track if user received welcome bonus
            }
            
            data_manager.users[user_str] = user_data
            await Storage.save_users(data_manager.users)
            return user_data
    
    @staticmethod
    async def update_user(user_id: int, updates: Dict):
        """Update user data asynchronously"""
        user_str = str(user_id)
        async with data_manager._async_lock():
            if user_str in data_manager.users:
                data_manager.users[user_str].update(updates)
                data_manager.users[user_str]['last_active'] = datetime.now().isoformat()
                await Storage.save_users(data_manager.users)
    
    @staticmethod
    async def add_transaction(user_id: int, amount: float, tx_type: str, description: str):
        """Add transaction asynchronously"""
        user = await UserManager.get_user(user_id)
        
        transaction = {
            'id': len(user.get('transactions', [])) + 1,
            'amount': amount,
            'type': tx_type,
            'description': description,
            'date': datetime.now().isoformat()
        }
        
        if 'transactions' not in user:
            user['transactions'] = []
        
        user['transactions'].append(transaction)
        
        if len(user['transactions']) > 50:
            user['transactions'] = user['transactions'][-50:]
        
        await UserManager.update_user(user_id, user)
    
    @staticmethod
    def is_referred(user_id: int) -> bool:
        """Check if user was referred"""
        user_str = str(user_id)
        return user_str in data_manager.referrals
    
    @staticmethod
    def get_referrer(user_id: int) -> Optional[int]:
        """Get referrer ID"""
        user_str = str(user_id)
        if user_str in data_manager.referrals:
            return int(data_manager.referrals[user_str])
        return None
    
    @staticmethod
    async def add_referral(referrer_id: int, referred_id: int) -> bool:
        """Add referral asynchronously - Only call after user has joined all channels"""
        if referrer_id == referred_id:
            return False
        
        referred_str = str(referred_id)
        
        # Check if already referred
        async with data_manager._async_lock():
            if referred_str in data_manager.referrals:
                logger.info(f"User {referred_id} already referred by {data_manager.referrals[referred_str]}")
                return False
            
            # Record referral
            data_manager.referrals[referred_str] = str(referrer_id)
            await Storage.save_referrals(data_manager.referrals)
        
        # Update referrer's stats
        referrer = await UserManager.get_user(referrer_id)
        new_balance = referrer.get('balance', 0) + 1.0
        
        await UserManager.update_user(referrer_id, {
            'balance': new_balance,
            'referral_count': referrer.get('referral_count', 0) + 1,
            'total_earned': referrer.get('total_earned', 0) + 1.0
        })
        
        # Add transaction
        await UserManager.add_transaction(
            referrer_id, 
            1.0, 
            'credit', 
            f'Referral bonus for user {referred_id} (joined all channels)'
        )
        
        logger.info(f"✅ New referral completed: {referrer_id} → {referred_id}")
        return True
    
    @staticmethod
    async def add_pending_referral(referrer_id: int, referred_id: int):
        """Add pending referral (when user starts with referral link but hasn't joined channels yet)"""
        await Storage.save_pending_referral(referrer_id, referred_id)
        logger.info(f"📝 Pending referral added: {referrer_id} → {referred_id}")
    
    @staticmethod
    async def get_pending_referrer(referred_id: int) -> Optional[int]:
        """Get pending referrer ID for a user"""
        return await Storage.get_pending_referrer(referred_id)
    
    @staticmethod
    async def remove_pending_referral(referred_id: int):
        """Remove pending referral"""
        await Storage.remove_pending_referral(referred_id)
        logger.info(f"🗑️ Pending referral removed for user {referred_id}")
    
    @staticmethod
    async def give_welcome_bonus(user_id: int) -> bool:
        """Give ₹1 welcome bonus to new user - returns True if bonus was given"""
        user = await UserManager.get_user(user_id)
        
        if user.get('welcome_bonus_received', False):
            return False  # Already received welcome bonus
        
        # Give welcome bonus
        new_balance = user.get('balance', 0) + 1.0
        await UserManager.update_user(user_id, {
            'balance': new_balance,
            'welcome_bonus_received': True,
            'total_earned': user.get('total_earned', 0) + 1.0
        })
        
        # Add transaction
        await UserManager.add_transaction(
            user_id,
            1.0,
            'credit',
            'Welcome bonus for joining all channels'
        )
        
        logger.info(f"✅ Welcome bonus given to user {user_id}")
        return True

async def check_channel_membership(bot, user_id: int) -> tuple:
    """Check channel membership concurrently"""
    channels = ChannelManager.get_channels()
    
    if not channels:
        logger.info("No channels configured, skipping membership check")
        return True, []
    
    tasks = []
    not_joined = []
    
    for channel in channels:
        task = asyncio.create_task(check_single_channel(bot, user_id, channel))
        tasks.append(task)
    
    # Wait for all checks to complete with timeout
    try:
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                logger.error(f"Error checking channel {channels[i]['chat_id']}: {result}")
                not_joined.append(channels[i])
            elif not result:
                not_joined.append(channels[i])
    except Exception as e:
        logger.error(f"Error in channel check: {e}")
        not_joined = channels  # Assume not joined on error
    
    logger.info(f"User {user_id} membership: joined={len(not_joined) == 0}, not_joined={len(not_joined)}")
    return len(not_joined) == 0, not_joined

async def check_single_channel(bot, user_id: int, channel: Dict) -> bool:
    """Check membership for a single channel"""
    chat_id = channel['chat_id']
    try:
        if isinstance(chat_id, str) and chat_id.lstrip('-').isdigit():
            chat_id_int = int(chat_id)
        else:
            chat_id_int = chat_id
        
        # Add timeout for chat member check
        try:
            member = await asyncio.wait_for(
                bot.get_chat_member(chat_id=chat_id_int, user_id=user_id),
                timeout=10.0
            )
            return member.status not in ['left', 'kicked']
        except asyncio.TimeoutError:
            logger.warning(f"Timeout checking {chat_id}")
            return False
        except Exception as e:
            logger.warning(f"Error checking membership for {chat_id}: {e}")
            return False
    except Exception as e:
        logger.error(f"Error checking {chat_id}: {e}")
        return False

async def get_invite_link(bot, chat_id, channel_name: str = None):
    """Get or create invite link for a chat with timeout"""
    try:
        if isinstance(chat_id, str) and chat_id.lstrip('-').isdigit():
            chat_id_int = int(chat_id)
        else:
            chat_id_int = chat_id
        
        logger.info(f"Getting invite link for {channel_name or chat_id} ({chat_id})")
        
        # Add timeout for get_chat
        try:
            chat = await asyncio.wait_for(
                bot.get_chat(chat_id_int),
                timeout=10.0
            )
        except asyncio.TimeoutError:
            logger.warning(f"Timeout getting chat {chat_id}")
            return None
        except Exception as e:
            logger.error(f"Error getting chat {chat_id}: {e}")
            # Try alternative method for usernames
            if isinstance(chat_id, str) and chat_id.startswith('@'):
                return f"https://t.me/{chat_id.lstrip('@')}"
            return None
        
        # Try to get existing invite link
        try:
            invite_link = await asyncio.wait_for(
                chat.export_invite_link(),
                timeout=10.0
            )
            logger.info(f"Got existing invite link for {channel_name or chat_id}: {invite_link[:50]}...")
            return invite_link
        except:
            # If no invite link exists, try to create one
            # Note: Bot needs to be admin with invite link permission
            try:
                invite_link = await asyncio.wait_for(
                    bot.create_chat_invite_link(
                        chat_id=chat_id_int,
                        creates_join_request=False
                    ),
                    timeout=10.0
                )
                logger.info(f"Created new invite link for {channel_name or chat_id}: {invite_link.invite_link[:50]}...")
                return invite_link.invite_link
            except Exception as e:
                logger.error(f"Failed to create invite link for {channel_name or chat_id}: {e}")
                # Fallback to username if available
                if hasattr(chat, 'username') and chat.username:
                    link = f"https://t.me/{chat.username}"
                    logger.info(f"Using username link for {channel_name or chat_id}: {link}")
                    return link
                else:
                    # For private channels/groups without username
                    logger.warning(f"No username available for private chat {channel_name or chat_id}")
                    return None
    except Exception as e:
        logger.error(f"Error getting invite link for {chat_id}: {e}")
        # Last resort fallback
        if isinstance(chat_id, str) and chat_id.startswith('@'):
            link = f"https://t.me/{chat_id.lstrip('@')}"
            logger.info(f"Using fallback username link: {link}")
            return link
        return None

async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /start command with timeout - FIXED: Referral bonus only after joining channels"""
    try:
        user = update.effective_user
        
        if not user:
            logger.error("No user found in update")
            return
        
        # Send immediate response to acknowledge command
        try:
            await update.message.reply_chat_action(action="typing")
        except:
            pass
        
        logger.info(f"📨 Start command received from user {user.id} ({user.first_name})")
        
        user_data = await UserManager.get_user(user.id)
        
        # Check for referral parameter
        args = context.args
        if args and args[0].startswith('REF'):
            referral_code = args[0]
            logger.info(f"Referral code detected: {referral_code}")
            
            # Skip if user was already referred
            if UserManager.is_referred(user.id):
                await update.message.reply_text(
                    "⚠️ You have already been referred before. "
                    "Referral bonus only works for new users."
                )
            else:
                # Find referrer by code
                referrer_found = None
                # Use lock for thread-safe access
                async with data_manager._async_lock():
                    for user_id_str, user_data_item in data_manager.users.items():
                        if user_data_item.get('referral_code') == referral_code:
                            referrer_found = int(user_id_str)
                            break
                
                if referrer_found and referrer_found != user.id:
                    # Check if already has pending referral
                    existing_pending = await UserManager.get_pending_referrer(user.id)
                    
                    if existing_pending:
                        if existing_pending == referrer_found:
                            # Don't show message about pending referral to user
                            pass
                        else:
                            # Don't show message about another pending referral
                            pass
                    else:
                        # Store as PENDING referral (not completed yet)
                        await UserManager.add_pending_referral(referrer_found, user.id)
                        
                        # REMOVED: Don't notify referrer about pending referral
                        # asyncio.create_task(
                        #     notify_referrer_pending(context.bot, referrer_found, user)
                        # )
                        
                        # Show simple message to user
                        await update.message.reply_text(
                            f"📝 **Referral Link Accepted!**\n\n"
                            f"You will earn rewards after joining all channels.\n"
                            f"Join all channels below and click 'Verify Join' to continue."
                        )
                elif referrer_found == user.id:
                    await update.message.reply_text(
                        "❌ You cannot use your own referral link!"
                    )
        
        # Check channel membership with timeout
        try:
            has_joined, not_joined = await asyncio.wait_for(
                check_channel_membership(context.bot, user.id),
                timeout=30.0
            )
            
            logger.info(f"Channel check: has_joined={has_joined}, not_joined={len(not_joined)}")
            
            if not has_joined and not_joined:
                await show_join_buttons(update, context, not_joined)
            else:
                # User has joined all channels
                await UserManager.update_user(user.id, {'has_joined_channels': True})
                
                # Give welcome bonus if not already received
                welcome_bonus_given = await UserManager.give_welcome_bonus(user.id)
                
                # Check if user has a pending referral to complete
                pending_referrer = await UserManager.get_pending_referrer(user.id)
                if pending_referrer and not UserManager.is_referred(user.id):
                    # Complete the referral now that user has joined all channels
                    is_new_referral = await UserManager.add_referral(pending_referrer, user.id)
                    
                    if is_new_referral:
                        # Remove pending referral
                        await UserManager.remove_pending_referral(user.id)
                        
                        # Notify referrer about COMPLETED referral
                        asyncio.create_task(
                            notify_referrer_completed(context.bot, pending_referrer, user)
                        )
                        
                        message = "✅ **Successfully Verified!**\n\n"
                        if welcome_bonus_given:
                            message += "🎉 **Welcome Bonus:** ₹1.00 credited to your account!\n"
                        message += f"🎉 **Referral Bonus:** ₹1.00 credited to user {pending_referrer}!\n\n"
                        message += "Now you can access all bot features."
                        
                        await update.message.reply_text(message)
                    else:
                        message = "✅ **Successfully Verified!**\n\n"
                        if welcome_bonus_given:
                            message += "🎉 **Welcome Bonus:** ₹1.00 credited to your account!\n\n"
                        message += "Now you can access all bot features."
                        
                        await update.message.reply_text(message)
                else:
                    message = "✅ **Successfully Verified!**\n\n"
                    if welcome_bonus_given:
                        message += "🎉 **Welcome Bonus:** ₹1.00 credited to your account!\n\n"
                    message += "Now you can access all bot features."
                    
                    await update.message.reply_text(message)
                
                await show_main_menu(update, context)
                
        except asyncio.TimeoutError:
            logger.warning(f"Timeout checking channels for user {user.id}")
            await update.message.reply_text(
                "⏳ Checking channel membership is taking longer than expected. "
                "Please try again in a moment."
            )
            # Show menu anyway to prevent blocking
            await show_main_menu(update, context)
            
    except Exception as e:
        logger.error(f"Error in start_command: {e}", exc_info=True)
        try:
            await update.message.reply_text(
                "❌ An error occurred. Please try again or contact admin."
            )
        except:
            pass

async def notify_referrer_completed(bot, referrer_id: int, referred_user):
    """Notify referrer about COMPLETED referral (user has joined all channels)"""
    try:
        await bot.send_message(
            chat_id=referrer_id,
            text=f"🎉 **Referral Completed!**\n\n"
                 f"User {referred_user.first_name} (ID: {referred_user.id}) "
                 f"has joined all required channels and you have earned ₹1.00!\n\n"
                 f"💰 Your new balance: ₹{(await UserManager.get_user(referrer_id))['balance']:.2f}"
        )
    except Exception as e:
        logger.error(f"Failed to notify referrer about completed referral: {e}")

async def show_join_buttons(update: Update, context: ContextTypes.DEFAULT_TYPE, not_joined: List[Dict]):
    """Show join buttons for channels - FIXED VERSION"""
    try:
        user = update.effective_user
        
        if not not_joined:
            logger.info("No channels to join, showing main menu")
            await show_main_menu(update, context)
            return
        
        keyboard = []
        successful_buttons = 0
        
        logger.info(f"Showing join buttons for {len(not_joined)} channels")
        
        # Get all invite links concurrently
        link_tasks = []
        for channel in not_joined:
            chat_id = channel['chat_id']
            channel_name = channel.get('name', 'Join Channel')
            
            logger.info(f"Getting invite link for: {channel_name} ({chat_id})")
            
            task = asyncio.create_task(get_invite_link(context.bot, chat_id, channel_name))
            link_tasks.append((task, channel_name, chat_id))
        
        # Process results as they become available
        for task, channel_name, chat_id in link_tasks:
            try:
                invite_link = await asyncio.wait_for(task, timeout=10.0)
                if invite_link:
                    logger.info(f"✅ Got invite link for {channel_name}")
                    keyboard.append([
                        InlineKeyboardButton(f"📢 {channel_name}", url=invite_link)
                    ])
                    successful_buttons += 1
                else:
                    logger.warning(f"❌ No invite link for {channel_name} ({chat_id})")
                    # Don't show button if no invite link - just skip
                    continue
            except (asyncio.TimeoutError, Exception) as e:
                logger.warning(f"⚠️ Timeout/error getting invite link for {channel_name}: {e}")
                # Skip this channel if we can't get invite link
                continue
        
        # Only show verify button if we have at least one join button
        if keyboard:
            keyboard.append([
                InlineKeyboardButton("✅ Verify Join", callback_data="verify_join")
            ])
            
            message_text = (
                f"👋 Welcome {user.first_name}!\n\n"
                f"To use this bot, you need to join {len(not_joined)} channel(s).\n"
                f"After joining all channels, click 'Verify Join' below.\n\n"
                f"🎁 **Special Offer:** Get ₹1 welcome bonus after joining all channels!"
            )
            
            if update.callback_query:
                try:
                    await update.callback_query.message.reply_text(
                        message_text,
                        reply_markup=InlineKeyboardMarkup(keyboard),
                        parse_mode=ParseMode.MARKDOWN
                    )
                except:
                    await update.callback_query.edit_message_text(
                        message_text,
                        reply_markup=InlineKeyboardMarkup(keyboard),
                        parse_mode=ParseMode.MARKDOWN
                    )
            else:
                await update.message.reply_text(
                    message_text,
                    reply_markup=InlineKeyboardMarkup(keyboard),
                    parse_mode=ParseMode.MARKDOWN
                )
        else:
            # No valid invite links - show error
            logger.error("No valid invite links found for any channel")
            error_message = (
                f"⚠️ **Unable to get invite links**\n\n"
                f"Sorry, we couldn't get invite links for any required channels.\n"
                f"Please contact the admin for assistance.\n\n"
                f"Configured channels: {len(data_manager.channels)}"
            )
            
            if update.callback_query:
                await update.callback_query.message.reply_text(error_message)
            else:
                await update.message.reply_text(error_message)
                
            # Still show main menu so user can access other features
            await show_main_menu(update, context)
            
    except Exception as e:
        logger.error(f"Error in show_join_buttons: {e}", exc_info=True)
        try:
            await update.message.reply_text(
                "Error showing join buttons. Please contact admin or try again later."
            )
        except:
            pass

async def no_invite_link_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle no invite link callback"""
    query = update.callback_query
    await query.answer("Please contact the admin to add you manually.", show_alert=True)

async def verify_join_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle verify join button callback - FIXED: Complete pending referrals after verification"""
    try:
        query = update.callback_query
        await query.answer()
        
        user = update.effective_user
        
        # Check membership with timeout
        try:
            has_joined, not_joined = await asyncio.wait_for(
                check_channel_membership(context.bot, user.id),
                timeout=20.0
            )
            
            if has_joined:
                await UserManager.update_user(user.id, {'has_joined_channels': True})
                
                # Give welcome bonus if not already received
                welcome_bonus_given = await UserManager.give_welcome_bonus(user.id)
                
                # Check if user has a pending referral to complete
                pending_referrer = await UserManager.get_pending_referrer(user.id)
                if pending_referrer and not UserManager.is_referred(user.id):
                    # Complete the referral now that user has joined all channels
                    is_new_referral = await UserManager.add_referral(pending_referrer, user.id)
                    
                    if is_new_referral:
                        # Remove pending referral
                        await UserManager.remove_pending_referral(user.id)
                        
                        # Notify referrer about COMPLETED referral
                        asyncio.create_task(
                            notify_referrer_completed(context.bot, pending_referrer, user)
                        )
                        
                        message = "✅ **Successfully Verified!**\n\n"
                        if welcome_bonus_given:
                            message += "🎉 **Welcome Bonus:** ₹1.00 credited to your account!\n"
                        message += f"🎉 **Referral Bonus:** ₹1.00 credited to user {pending_referrer}!\n\n"
                        message += "Now you can access all bot features."
                        
                        await query.edit_message_text(message)
                    else:
                        message = "✅ **Successfully Verified!**\n\n"
                        if welcome_bonus_given:
                            message += "🎉 **Welcome Bonus:** ₹1.00 credited to your account!\n\n"
                        message += "Now you can access all bot features."
                        
                        await query.edit_message_text(message)
                else:
                    message = "✅ **Successfully Verified!**\n\n"
                    if welcome_bonus_given:
                        message += "🎉 **Welcome Bonus:** ₹1.00 credited to your account!\n\n"
                    message += "Now you can access all bot features."
                    
                    await query.edit_message_text(message)
                
                await show_main_menu_callback(update, context)
            else:
                # Show updated join buttons
                await show_join_buttons(update, context, not_joined)
                
        except asyncio.TimeoutError:
            await query.answer("Verification is taking too long. Please try again.", show_alert=True)
            
    except Exception as e:
        logger.error(f"Error in verify_join_callback: {e}")
        try:
            await query.answer("Error verifying join. Please try again.", show_alert=True)
        except:
            pass

async def show_main_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show main menu to user"""
    try:
        user = update.effective_user
        user_data = await UserManager.get_user(user.id)
        
        welcome_bonus_note = ""
        if user_data.get('welcome_bonus_received', False):
            welcome_bonus_note = "🎁 Welcome bonus received!\n\n"
        
        message = (
            f"👋 Welcome back, {user.first_name}!\n\n"
            f"{welcome_bonus_note}"
            f"💰 Balance: ₹{user_data.get('balance', 0):.2f}\n"
            f"👥 Referrals: {user_data.get('referral_count', 0)}\n\n"
            f"📊 Total Earned: ₹{user_data.get('total_earned', 0):.2f}\n"
            f"🏦 Total Withdrawn: ₹{user_data.get('total_withdrawn', 0):.2f}\n\n"
            f"Your Referral Code: `{user_data.get('referral_code', '')}`"
        )
        
        keyboard = [
            [InlineKeyboardButton("💰 Balance", callback_data="balance"),
             InlineKeyboardButton("📤 Withdraw", callback_data="withdraw")],
            [InlineKeyboardButton("📜 History", callback_data="history"),
             InlineKeyboardButton("👥 Referrals", callback_data="referrals")],
            [InlineKeyboardButton("🔗 Invite Link", callback_data="invite_link")]
        ]
        
        # Add admin button if user is admin
        if user.id in ADMIN_IDS:
            keyboard.append([InlineKeyboardButton("👑 Admin Panel", callback_data="admin_panel")])
        
        keyboard.append([InlineKeyboardButton("🔄 Refresh", callback_data="refresh")])
        
        if update.callback_query:
            await update.callback_query.edit_message_text(
                text=message,
                reply_markup=InlineKeyboardMarkup(keyboard),
                parse_mode=ParseMode.MARKDOWN
            )
        else:
            await update.message.reply_text(
                text=message,
                reply_markup=InlineKeyboardMarkup(keyboard),
                parse_mode=ParseMode.MARKDOWN
            )
            
    except Exception as e:
        logger.error(f"Error in show_main_menu: {e}")

async def show_main_menu_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle main menu callback"""
    await show_main_menu(update, context)

async def balance_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle balance button callback"""
    try:
        query = update.callback_query
        await query.answer()
        
        user = update.effective_user
        user_data = await UserManager.get_user(user.id)
        
        message = (
            f"💰 **Your Balance**\n\n"
            f"• Available Balance: ₹{user_data.get('balance', 0):.2f}\n"
            f"• Total Earned: ₹{user_data.get('total_earned', 0):.2f}\n"
            f"• Total Withdrawn: ₹{user_data.get('total_withdrawn', 0):.2f}\n"
            f"• Referral Count: {user_data.get('referral_count', 0)}\n\n"
            f"💸 Withdraw using: `/withdraw <amount> <method>`"
        )
        
        keyboard = [
            [InlineKeyboardButton("📤 Withdraw", callback_data="withdraw"),
             InlineKeyboardButton("🔙 Back", callback_data="back_to_main")]
        ]
        
        await query.edit_message_text(
            text=message,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode=ParseMode.MARKDOWN
        )
    except Exception as e:
        logger.error(f"Error in balance_callback: {e}")

async def withdraw_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /withdraw command"""
    try:
        user = update.effective_user
        
        if not user:
            return
        
        # Get command arguments
        args = context.args
        if not args or len(args) < 2:
            await update.message.reply_text(
                "💰 **Withdrawal Request**\n\n"
                "Usage: `/withdraw <amount> <method>`\n"
                "Example: `/withdraw 50 upi`\n\n"
                "Available methods: UPI, Bank Transfer\n"
                "Minimum withdrawal: ₹10.00",
                parse_mode=ParseMode.MARKDOWN
            )
            return
        
        try:
            amount = float(args[0])
            method = args[1].lower()
            
            if amount < 10:
                await update.message.reply_text("❌ Minimum withdrawal amount is ₹10.00")
                return
            
            user_data = await UserManager.get_user(user.id)
            
            if user_data.get('balance', 0) < amount:
                await update.message.reply_text(f"❌ Insufficient balance. You have ₹{user_data.get('balance', 0):.2f}")
                return
            
            # Update user balance
            new_balance = user_data.get('balance', 0) - amount
            total_withdrawn = user_data.get('total_withdrawn', 0) + amount
            
            await UserManager.update_user(user.id, {
                'balance': new_balance,
                'total_withdrawn': total_withdrawn
            })
            
            # Add transaction
            await UserManager.add_transaction(
                user.id,
                -amount,
                'withdrawal',
                f'Withdrawal via {method}'
            )
            
            # Notify admin
            admin_message = (
                f"📤 **New Withdrawal Request**\n\n"
                f"👤 User: {user.first_name} (ID: {user.id})\n"
                f"💰 Amount: ₹{amount:.2f}\n"
                f"📝 Method: {method}\n"
                f"🏦 New Balance: ₹{new_balance:.2f}"
            )
            
            for admin_id in ADMIN_IDS:
                try:
                    await context.bot.send_message(chat_id=admin_id, text=admin_message)
                except Exception as e:
                    logger.error(f"Failed to notify admin {admin_id}: {e}")
            
            await update.message.reply_text(
                f"✅ **Withdrawal Request Submitted!**\n\n"
                f"💰 Amount: ₹{amount:.2f}\n"
                f"📝 Method: {method}\n"
                f"🏦 New Balance: ₹{new_balance:.2f}\n\n"
                f"📋 Your request has been sent to the admin for processing.\n"
                f"You will be contacted soon for payment details."
            )
            
        except ValueError:
            await update.message.reply_text("❌ Invalid amount. Please enter a valid number.")
            
    except Exception as e:
        logger.error(f"Error in withdraw_command: {e}")
        await update.message.reply_text("❌ An error occurred. Please try again.")

async def withdraw_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle withdraw button callback"""
    try:
        query = update.callback_query
        await query.answer()
        
        user = update.effective_user
        user_data = await UserManager.get_user(user.id)
        
        message = (
            f"📤 **Withdrawal**\n\n"
            f"💰 Available: ₹{user_data.get('balance', 0):.2f}\n"
            f"📝 Minimum: ₹10.00\n\n"
            f"Usage: `/withdraw <amount> <method>`\n"
            f"Example: `/withdraw 50 upi`\n\n"
            f"Available methods: UPI, Bank Transfer"
        )
        
        keyboard = [
            [InlineKeyboardButton("💰 Check Balance", callback_data="balance"),
             InlineKeyboardButton("🔙 Back", callback_data="back_to_main")]
        ]
        
        await query.edit_message_text(
            text=message,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode=ParseMode.MARKDOWN
        )
    except Exception as e:
        logger.error(f"Error in withdraw_callback: {e}")

async def history_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle history button callback"""
    try:
        query = update.callback_query
        await query.answer()
        
        user = update.effective_user
        user_data = await UserManager.get_user(user.id)
        
        transactions = user_data.get('transactions', [])
        
        if not transactions:
            message = "📜 No transactions yet."
        else:
            # Show last 10 transactions
            recent_tx = transactions[-10:]
            tx_list = []
            for tx in reversed(recent_tx):
                sign = "+" if tx.get('type') == 'credit' else "-"
                tx_list.append(f"{sign}₹{tx.get('amount', 0):.2f} - {tx.get('description', '')} ({tx.get('date', '')[:10]})")
            
            message = "📜 **Recent Transactions**\n\n" + "\n".join(tx_list)
        
        keyboard = [
            [InlineKeyboardButton("🔙 Back", callback_data="back_to_main")]
        ]
        
        await query.edit_message_text(
            text=message,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode=ParseMode.MARKDOWN
        )
    except Exception as e:
        logger.error(f"Error in history_callback: {e}")

async def referrals_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle referrals button callback"""
    try:
        query = update.callback_query
        await query.answer()
        
        user = update.effective_user
        user_data = await UserManager.get_user(user.id)
        
        referral_code = user_data.get('referral_code', f"REF{user.id}")
        referral_count = user_data.get('referral_count', 0)
        referral_earnings = user_data.get('total_earned', 0)
        
        message = (
            f"👥 **Your Referrals**\n\n"
            f"• Referral Code: `{referral_code}`\n"
            f"• Total Referrals: {referral_count}\n"
            f"• Earned from Referrals: ₹{referral_earnings:.2f}\n\n"
            f"**How it works:**\n"
            f"1. Share your referral link\n"
            f"2. When someone joins via your link AND joins all channels\n"
            f"3. You earn ₹1.00 per successful referral\n\n"
            f"Share: https://t.me/{context.bot.username}?start={referral_code}"
        )
        
        keyboard = [
            [InlineKeyboardButton("🔗 Share Link", callback_data="invite_link")],
            [InlineKeyboardButton("🔙 Back", callback_data="back_to_main")]
        ]
        
        await query.edit_message_text(
            text=message,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode=ParseMode.MARKDOWN
        )
    except Exception as e:
        logger.error(f"Error in referrals_callback: {e}")

async def invite_link_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle invite link button callback"""
    try:
        query = update.callback_query
        await query.answer()
        
        user = update.effective_user
        user_data = await UserManager.get_user(user.id)
        
        referral_code = user_data.get('referral_code', f"REF{user.id}")
        invite_link = f"https://t.me/{context.bot.username}?start={referral_code}"
        
        message = (
            f"🔗 **Your Referral Link**\n\n"
            f"Share this link to earn ₹1.00 for each new user who:\n"
            f"1. Clicks your link\n"
            f"2. Joins all required channels\n\n"
            f"**Link:**\n`{invite_link}`"
        )
        
        keyboard = [
            [InlineKeyboardButton("📤 Share", url=f"tg://msg_url?url={invite_link}&text=Join this bot to earn money! Get ₹1 welcome bonus!")],
            [InlineKeyboardButton("🔙 Back", callback_data="back_to_main")]
        ]
        
        await query.edit_message_text(
            text=message,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode=ParseMode.MARKDOWN
        )
    except Exception as e:
        logger.error(f"Error in invite_link_callback: {e}")

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /help command"""
    await update.message.reply_text(
        "🤖 **Bot Help**\n\n"
        "**Available Commands:**\n"
        "/start - Start the bot\n"
        "/withdraw <amount> <method> - Withdraw money\n"
        "/help - Show this help\n\n"
        "**How to Earn:**\n"
        "1. Get ₹1 welcome bonus after joining all channels\n"
        "2. Share your referral link\n"
        "3. Earn ₹1.00 when someone joins via your link AND completes all channel joins\n"
        "4. Minimum withdrawal: ₹10.00\n\n"
        "**Note:** Referral bonuses are credited after users join all required channels!",
        parse_mode=ParseMode.MARKDOWN
    )

async def restart_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /restart command (admin only)"""
    user = update.effective_user
    if user.id not in ADMIN_IDS:
        await update.message.reply_text("❌ Admin only")
        return
    
    await update.message.reply_text("🔄 Bot restarting...")
    os.execv(sys.executable, [sys.executable] + sys.argv)

async def backup_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /backup command (admin only)"""
    user = update.effective_user
    if user.id not in ADMIN_IDS:
        await update.message.reply_text("❌ Admin only")
        return
    
    await data_manager.backup_all_data_async()
    await update.message.reply_text("✅ Data backed up successfully")

async def stats_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /stats command (admin only)"""
    user = update.effective_user
    if user.id not in ADMIN_IDS:
        await update.message.reply_text("❌ Admin only")
        return
    
    stats = data_manager.get_stats()
    await update.message.reply_text(stats, parse_mode=ParseMode.HTML)

async def list_channels_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /listchannels command (admin only) - READ ONLY"""
    user = update.effective_user
    if user.id not in ADMIN_IDS:
        await update.message.reply_text("❌ Admin only")
        return
    
    channels = ChannelManager.get_channels()
    if not channels:
        message = "📢 No channels configured"
    else:
        channel_list = []
        for i, channel in enumerate(channels, 1):
            channel_list.append(f"{i}. {channel.get('name', 'Channel')} - `{channel.get('chat_id')}`")
        
        message = f"📢 **Configured Channels ({len(channels)})**\n\n" + "\n".join(channel_list)
    
    await update.message.reply_text(message, parse_mode=ParseMode.MARKDOWN)

async def broadcast_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /broadcast command (admin only)"""
    user = update.effective_user
    if user.id not in ADMIN_IDS:
        await update.message.reply_text("❌ Admin only")
        return
    
    args = context.args
    if not args:
        await update.message.reply_text("Usage: /broadcast <message>")
        return
    
    message = " ".join(args)
    
    # Confirmation keyboard
    keyboard = [
        [InlineKeyboardButton("✅ Confirm", callback_data=f"admin_broadcast_confirm_{message[:50]}"),
         InlineKeyboardButton("❌ Cancel", callback_data="admin_panel")]
    ]
    
    await update.message.reply_text(
        f"📢 **Broadcast Confirmation**\n\n"
        f"Message: {message}\n\n"
        f"Will be sent to {len(data_manager.users)} users.\n"
        f"Are you sure?",
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode=ParseMode.MARKDOWN
    )

async def admin_panel_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin panel - FIXED to avoid Markdown parsing errors"""
    try:
        query = update.callback_query
        await query.answer()
        
        user = update.effective_user
        
        if user.id not in ADMIN_IDS:
            await query.answer("❌ Admin only", show_alert=True)
            return
        
        stats = data_manager.get_stats()
        
        # Use HTML parsing to avoid Markdown issues
        message = (
            f"<b>👑 Admin Panel</b>\n\n"
            f"{stats}\n\n"
            "<b>Commands:</b>\n"
            "• <code>/listchannels</code> - View channels (read-only)\n"
            "• <code>/broadcast &lt;message&gt;</code> - Broadcast\n"
            "• <code>/restart</code> - Restart options\n"
            "• <code>/backup</code> - Backup data\n"
            "• <code>/stats</code> - Show statistics\n\n"
            "<b>ℹ️ Channel Configuration:</b>\n"
            "Channels are configured via INITIAL_CHANNELS environment variable."
        )
        
        keyboard = [
            [InlineKeyboardButton("📢 View Channels", callback_data="admin_channels")],
            [InlineKeyboardButton("📊 Stats", callback_data="admin_stats")],
            [InlineKeyboardButton("💾 Backup", callback_data="admin_backup")],
            [InlineKeyboardButton("🔄 Restart", callback_data="admin_restart")],
            [InlineKeyboardButton("🔙 Back", callback_data="back_to_main")]
        ]
        
        await query.edit_message_text(
            text=message,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode=ParseMode.HTML  # Changed to HTML
        )
    except Exception as e:
        logger.error(f"Error in admin_panel_callback: {e}")
        # Fallback with simpler message
        try:
            await query.edit_message_text(
                text="👑 Admin Panel\n\nClick the buttons below to manage the bot.",
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
        except:
            pass

async def admin_channels_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle admin channels callback"""
    try:
        query = update.callback_query
        await query.answer()
        
        user = update.effective_user
        if user.id not in ADMIN_IDS:
            await query.answer("❌ Admin only", show_alert=True)
            return
        
        channels = ChannelManager.get_channels()
        
        if not channels:
            message = "📢 No channels configured"
        else:
            channel_list = []
            for i, channel in enumerate(channels, 1):
                status = "✅" if channel.get('active', True) else "❌"
                channel_list.append(f"{i}. {status} {channel.get('name', 'Channel')} - `{channel.get('chat_id')}`")
            
            message = f"📢 **Configured Channels ({len(channels)})**\n\n" + "\n\n".join(channel_list)
        
        keyboard = [
            [InlineKeyboardButton("🔙 Back", callback_data="admin_panel")]
        ]
        
        await query.edit_message_text(
            text=message,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode=ParseMode.MARKDOWN
        )
    except Exception as e:
        logger.error(f"Error in admin_channels_callback: {e}")

async def admin_handle_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle admin callback queries"""
    query = update.callback_query
    await query.answer()
    
    data = query.data
    
    if data.startswith("admin_broadcast_confirm_"):
        # Handle broadcast confirmation
        message = data.replace("admin_broadcast_confirm_", "")
        
        # Get full message from context if truncated
        if message.endswith("..."):
            # In a real implementation, you'd store the full message somewhere
            await query.edit_message_text("⚠️ Message too long, please send shorter broadcast.")
            return
        
        await query.edit_message_text("📢 Broadcasting to users...")
        
        success = 0
        failed = 0
        
        for user_id_str in data_manager.users:
            try:
                await context.bot.send_message(
                    chat_id=int(user_id_str),
                    text=f"📢 **Broadcast Message**\n\n{message}"
                )
                success += 1
            except:
                failed += 1
        
        await query.edit_message_text(
            f"✅ **Broadcast Complete**\n\n"
            f"✅ Successful: {success}\n"
            f"❌ Failed: {failed}\n"
            f"📊 Total: {success + failed} users"
        )
    
    elif data == "admin_stats":
        stats = data_manager.get_stats()
        await query.edit_message_text(stats, parse_mode=ParseMode.HTML)
    
    elif data == "admin_backup":
        await data_manager.backup_all_data_async()
        await query.edit_message_text("✅ Data backed up successfully")
    
    elif data == "admin_restart":
        keyboard = [
            [InlineKeyboardButton("🔄 Soft Restart", callback_data="admin_restart_soft"),
             InlineKeyboardButton("🔙 Cancel", callback_data="admin_panel")]
        ]
        await query.edit_message_text(
            "🔄 **Restart Options**\n\n"
            "Soft Restart: Reload data without stopping bot",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
    elif data == "admin_restart_soft":
        # Reload data from storage
        data_manager._load_all_data_sync()
        await query.edit_message_text("✅ Data reloaded successfully")
    
    elif data == "admin_panel":
        await admin_panel_callback(update, context)

async def confirm_reset_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle confirm reset callback"""
    query = update.callback_query
    await query.answer()
    
    await query.edit_message_text("🔄 Reset functionality is not implemented in this version.")

async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Log errors and handle them gracefully"""
    logger.error(f"Exception while handling an update: {context.error}", exc_info=context.error)
    
    # Try to notify user about error
    if update and update.effective_message:
        try:
            await update.effective_message.reply_text(
                "❌ An error occurred. Please try again later."
            )
        except:
            pass

# Simple HTTP server for Render
def run_http_server():
    """Run HTTP server for health checks"""
    from http.server import HTTPServer, BaseHTTPRequestHandler
    
    class HealthHandler(BaseHTTPRequestHandler):
        def do_GET(self):
            self.send_response(200)
            self.send_header('Content-type', 'text/plain')
            self.end_headers()
            response = f"Bot is running\nUsers: {len(data_manager.users)}\nChannels: {len(data_manager.channels)}\nStorage: {'MongoDB' if db_connected else 'Local files'}"
            self.wfile.write(response.encode())
        
        def log_message(self, format, *args):
            pass
    
    try:
        server = HTTPServer(('0.0.0.0', PORT), HealthHandler)
        logger.info(f"✅ HTTP server running on port {PORT}")
        server.serve_forever()
    except Exception as e:
        logger.error(f"❌ HTTP server failed: {e}")

def main():
    """Main function to start the bot"""
    if not BOT_TOKEN:
        logger.error("❌ BOT_TOKEN not set")
        print("ERROR: Please set BOT_TOKEN environment variable")
        return
    
    # Check MongoDB URI for common issues
    if MONGODB_URI and "mongodb+srv://" in MONGODB_URI:
        logger.info("ℹ️ Using MongoDB SRV connection - make sure DNS is properly configured")
    
    # Start HTTP server for Render health checks
    http_thread = threading.Thread(target=run_http_server, daemon=True)
    http_thread.start()
    
    # Create bot application with improved configuration
    application = (
        Application.builder()
        .token(BOT_TOKEN)
        .connect_timeout(30.0)
        .read_timeout(30.0)
        .write_timeout(30.0)
        .pool_timeout(30.0)
        .build()
    )
    
    # Add error handler
    application.add_error_handler(error_handler)
    
    # Add command handlers
    application.add_handler(CommandHandler("start", start_command))
    application.add_handler(CommandHandler("withdraw", withdraw_command))
    application.add_handler(CommandHandler("help", help_command))
    application.add_handler(CommandHandler("restart", restart_command))
    application.add_handler(CommandHandler("backup", backup_command))
    application.add_handler(CommandHandler("stats", stats_command))
    
    # Admin commands (read-only for channels)
    application.add_handler(CommandHandler("listchannels", list_channels_command))
    application.add_handler(CommandHandler("broadcast", broadcast_command))
    
    # Callback handlers
    application.add_handler(CallbackQueryHandler(verify_join_callback, pattern="^verify_join$"))
    application.add_handler(CallbackQueryHandler(no_invite_link_callback, pattern="^no_invite_link$"))
    application.add_handler(CallbackQueryHandler(show_main_menu_callback, pattern="^back_to_main$"))
    application.add_handler(CallbackQueryHandler(show_main_menu_callback, pattern="^refresh$"))
    application.add_handler(CallbackQueryHandler(balance_callback, pattern="^balance$"))
    application.add_handler(CallbackQueryHandler(withdraw_callback, pattern="^withdraw$"))
    application.add_handler(CallbackQueryHandler(history_callback, pattern="^history$"))
    application.add_handler(CallbackQueryHandler(referrals_callback, pattern="^referrals$"))
    application.add_handler(CallbackQueryHandler(invite_link_callback, pattern="^invite_link$"))
    application.add_handler(CallbackQueryHandler(admin_panel_callback, pattern="^admin_panel$"))
    application.add_handler(CallbackQueryHandler(admin_channels_callback, pattern="^admin_channels$"))
    application.add_handler(CallbackQueryHandler(admin_handle_callback, pattern="^admin_"))
    application.add_handler(CallbackQueryHandler(confirm_reset_callback, pattern="^confirm_reset$"))
    
    # Try to get bot info
    try:
        bot_info = application.bot.get_me()
        bot_username = bot_info.username
    except Exception as e:
        logger.warning(f"Could not fetch bot username: {e}")
        bot_username = "unknown"
    
    # Start bot
    logger.info("🤖 Bot is starting...")
    print("=" * 50)
    print(f"✅ Bot started successfully!")
    print(f"🤖 Bot username: @{bot_username}")
    print(f"👑 Admin IDs: {ADMIN_IDS}")
    print(f"📢 Channels configured: {len(data_manager.channels)}")
    if data_manager.channels:
        for i, channel in enumerate(data_manager.channels, 1):
            print(f"  {i}. {channel.get('name', 'Channel')} - {channel.get('chat_id', 'N/A')}")
    print(f"👥 Users: {len(data_manager.users)}")
    print(f"🔗 Referrals: {len(data_manager.referrals)}")
    print(f"🌐 HTTP Server: http://0.0.0.0:{PORT}")
    print(f"💾 Storage: {'✅ MongoDB' if db_connected else '📁 Local files (MongoDB connection failed)'}")
    print("=" * 50)
    print("📝 Available commands:")
    print("• /start - Start the bot")
    print("• /withdraw <amount> <method> - Withdraw money")
    print("• /help - Show help")
    if ADMIN_IDS:
        print("👑 Admin commands:")
        print("• /listchannels - View configured channels (read-only)")
        print("• /stats - Show statistics")
    print("\n✅ Bot is now ready to handle multiple users simultaneously!")
    print("\n🎁 NEW: ₹1 welcome bonus for all new users after joining channels!")
    print("⚠️ IMPORTANT: Referral bonuses are now ONLY credited AFTER users join all channels!")
    print("📝 Pending referrals are not shown to anyone (only tracked internally)")
    
    if not db_connected:
        print("\n⚠️ WARNING: MongoDB connection failed. Using local file storage.")
        print("   This is OK for testing, but for production fix MongoDB connection.")
        print("   Check your MONGODB_URI environment variable.")
    
    try:
        # Run bot with long polling and handle updates concurrently
        application.run_polling(
            allowed_updates=Update.ALL_TYPES,
            drop_pending_updates=True,
            close_loop=False
        )
    except KeyboardInterrupt:
        logger.info("Bot stopped by user")
    except Exception as e:
        logger.error(f"Bot stopped with error: {e}")
        print(f"❌ Bot stopped: {e}")
    finally:
        # Cleanup
        executor.shutdown(wait=True)
        if mongo_client:
            mongo_client.close()

if __name__ == '__main__':
    main()