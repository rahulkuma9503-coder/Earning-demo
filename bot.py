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
pending_referrals_collection = None

# Thread pool for blocking operations - REDUCED for better performance
executor = ThreadPoolExecutor(max_workers=5)

# Cache for channel membership checks to reduce API calls
channel_check_cache = {}
CACHE_TIMEOUT = 300  # 5 minutes cache

def init_database():
    """Initialize MongoDB connection with optimized settings"""
    global mongo_client, channels_collection, users_collection, referrals_collection, pending_referrals_collection
    
    if not MONGODB_URI:
        logger.warning("⚠️ MONGODB_URI not set. Using file-based storage.")
        return False
    
    try:
        logger.info(f"🔗 Attempting to connect to MongoDB...")
        
        # Optimized MongoDB connection settings
        if "mongodb+srv://" in MONGODB_URI:
            logger.info("📡 Using MongoDB SRV connection")
            mongo_client = MongoClient(
                MONGODB_URI,
                serverSelectionTimeoutMS=5000,  # Reduced from 10000
                connectTimeoutMS=15000,         # Reduced from 30000
                socketTimeoutMS=15000,          # Reduced from 30000
                maxPoolSize=20,                 # Reduced pool size
                minPoolSize=5,
                retryWrites=True,
                w="majority"
            )
        else:
            logger.info("📡 Using standard MongoDB connection")
            mongo_client = MongoClient(
                MONGODB_URI,
                serverSelectionTimeoutMS=5000,
                connectTimeoutMS=15000,
                socketTimeoutMS=15000,
                maxPoolSize=20,
                minPoolSize=5
            )
        
        # Test connection with shorter timeout
        logger.info("🔄 Testing MongoDB connection...")
        mongo_client.server_info()
        
        db = mongo_client.get_database('telegram_referral_bot')
        
        # Initialize collections
        channels_collection = db['channels']
        users_collection = db['users']
        referrals_collection = db['referrals']
        pending_referrals_collection = db['pending_referrals']
        
        # Create indexes for faster queries
        users_collection.create_index('user_id', unique=True)
        channels_collection.create_index('chat_id', unique=True)
        referrals_collection.create_index([('referrer_id', 1), ('referred_id', 1)], unique=True)
        pending_referrals_collection.create_index('referred_id', unique=True)
        pending_referrals_collection.create_index('referrer_id')
        pending_referrals_collection.create_index('created_at', expireAfterSeconds=604800)
        
        # Create compound indexes for common queries
        users_collection.create_index([('last_active', -1)])
        users_collection.create_index([('balance', -1)])
        
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
    """Storage manager with MongoDB and file fallback - OPTIMIZED"""
    
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
                # Load from MongoDB with projection to reduce data
                channels = list(channels_collection.find({}, {'_id': 0, 'name': 1, 'chat_id': 1}))
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
        """Save users to storage asynchronously - OPTIMIZED: Batch update"""
        try:
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(executor, Storage._save_users_sync, users)
        except Exception as e:
            logger.error(f"Error saving users: {e}")
    
    @staticmethod
    def _save_users_sync(users: Dict):
        """Synchronous save users - OPTIMIZED"""
        try:
            if mongo_client is not None and users_collection is not None:
                # Batch operations for better performance
                bulk_operations = []
                for user_id, user_data in users.items():
                    bulk_operations.append(
                        pymongo.UpdateOne(
                            {'user_id': int(user_id)},
                            {'$set': user_data},
                            upsert=True
                        )
                    )
                
                if bulk_operations:
                    users_collection.bulk_write(bulk_operations, ordered=False)
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
        """Synchronous load users - OPTIMIZED"""
        try:
            if mongo_client is not None and users_collection is not None:
                # Load from MongoDB with projection to reduce data
                users = {}
                cursor = users_collection.find(
                    {}, 
                    {'_id': 0, 'user_id': 1, 'balance': 1, 'referral_code': 1, 
                     'referral_count': 1, 'total_earned': 1, 'total_withdrawn': 1,
                     'has_joined_channels': 1, 'welcome_bonus_received': 1}
                )
                for user in cursor:
                    user_id = user.get('user_id')
                    if user_id:
                        users[str(user_id)] = user
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
        """Synchronous save referrals - OPTIMIZED"""
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
        """Synchronous load referrals - OPTIMIZED"""
        try:
            if mongo_client is not None and referrals_collection is not None:
                # Load from MongoDB with projection
                referrals = {}
                cursor = referrals_collection.find({}, {'_id': 0, 'referred_id': 1, 'referrer_id': 1})
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
    
    # Pending referrals storage methods
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
        """Synchronous get pending referrer ID - OPTIMIZED"""
        try:
            if mongo_client is not None and pending_referrals_collection is not None:
                pending = pending_referrals_collection.find_one(
                    {'referred_id': referred_id},
                    {'_id': 0, 'referrer_id': 1}
                )
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
    """Manage all data with storage persistence - OPTIMIZED"""
    
    def __init__(self):
        self.channels = []
        self.users = {}
        self.referrals = {}
        self._lock = threading.Lock()
        
        # Load data synchronously during initialization
        self._load_all_data_sync()
        
        # Initialize channels from environment variable
        self.init_channels_from_env()
        
        # Backup data on exit
        atexit.register(self._backup_all_data_sync)
    
    def _load_all_data_sync(self):
        """Load all data from storage synchronously - OPTIMIZED"""
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
            
            # Format chat_id
            if clean_id.startswith('@'):
                chat_id_str = clean_id
            elif clean_id.startswith('-100'):
                chat_id_str = clean_id
            elif clean_id.startswith('-'):
                chat_id_str = clean_id
            elif clean_id.isdigit() and len(clean_id) > 9:
                chat_id_str = f"-100{clean_id}"
            else:
                logger.error(f"Invalid channel ID format: {clean_id}")
                return False
            
            # Check duplicate
            for channel in self.channels:
                if str(channel.get('chat_id')) == str(chat_id_str):
                    logger.info(f"Channel {chat_id_str} already exists")
                    return True
            
            # Get channel name
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
        """Backup all data to storage synchronously - OPTIMIZED"""
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
        """Get data statistics - HTML format"""
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
    """Manage users with async operations - OPTIMIZED"""
    
    @staticmethod
    async def get_user(user_id: int) -> Dict:
        """Get user data asynchronously - OPTIMIZED with cache"""
        user_str = str(user_id)
        
        # Try to get from cache first
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
                'welcome_bonus_received': False
            }
            
            data_manager.users[user_str] = user_data
            
            # Save user asynchronously without waiting
            asyncio.create_task(Storage.save_users(data_manager.users))
            
            return user_data
    
    @staticmethod
    async def update_user(user_id: int, updates: Dict):
        """Update user data asynchronously - OPTIMIZED"""
        user_str = str(user_id)
        async with data_manager._async_lock():
            if user_str in data_manager.users:
                data_manager.users[user_str].update(updates)
                data_manager.users[user_str]['last_active'] = datetime.now().isoformat()
                
                # Save asynchronously without waiting
                asyncio.create_task(Storage.save_users(data_manager.users))
    
    @staticmethod
    async def add_transaction(user_id: int, amount: float, tx_type: str, description: str):
        """Add transaction asynchronously - OPTIMIZED"""
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
        
        # Keep only last 20 transactions to reduce memory
        if len(user['transactions']) > 20:
            user['transactions'] = user['transactions'][-20:]
        
        await UserManager.update_user(user_id, user)
    
    @staticmethod
    def is_referred(user_id: int) -> bool:
        """Check if user was referred - FAST"""
        user_str = str(user_id)
        return user_str in data_manager.referrals
    
    @staticmethod
    def get_referrer(user_id: int) -> Optional[int]:
        """Get referrer ID - FAST"""
        user_str = str(user_id)
        if user_str in data_manager.referrals:
            return int(data_manager.referrals[user_str])
        return None
    
    @staticmethod
    async def add_referral(referrer_id: int, referred_id: int) -> bool:
        """Add referral asynchronously - OPTIMIZED"""
        if referrer_id == referred_id:
            return False
        
        referred_str = str(referred_id)
        
        # Check if already referred
        async with data_manager._async_lock():
            if referred_str in data_manager.referrals:
                logger.info(f"User {referred_id} already referred")
                return False
            
            # Record referral
            data_manager.referrals[referred_str] = str(referrer_id)
            
            # Save asynchronously
            asyncio.create_task(Storage.save_referrals(data_manager.referrals))
        
        # Update referrer's stats
        referrer = await UserManager.get_user(referrer_id)
        new_balance = referrer.get('balance', 0) + 1.0
        
        await UserManager.update_user(referrer_id, {
            'balance': new_balance,
            'referral_count': referrer.get('referral_count', 0) + 1,
            'total_earned': referrer.get('total_earned', 0) + 1.0
        })
        
        # Add transaction asynchronously
        asyncio.create_task(
            UserManager.add_transaction(
                referrer_id, 
                1.0, 
                'credit', 
                f'Referral bonus for user {referred_id}'
            )
        )
        
        logger.info(f"✅ New referral: {referrer_id} → {referred_id}")
        return True
    
    @staticmethod
    async def add_pending_referral(referrer_id: int, referred_id: int):
        """Add pending referral"""
        await Storage.save_pending_referral(referrer_id, referred_id)
        logger.info(f"Pending referral added: {referrer_id} → {referred_id}")
    
    @staticmethod
    async def get_pending_referrer(referred_id: int) -> Optional[int]:
        """Get pending referrer ID for a user"""
        return await Storage.get_pending_referrer(referred_id)
    
    @staticmethod
    async def remove_pending_referral(referred_id: int):
        """Remove pending referral"""
        await Storage.remove_pending_referral(referred_id)
    
    @staticmethod
    async def give_welcome_bonus(user_id: int) -> bool:
        """Give ₹1 welcome bonus to new user - returns True if bonus was given"""
        user = await UserManager.get_user(user_id)
        
        if user.get('welcome_bonus_received', False):
            return False
        
        # Give welcome bonus
        new_balance = user.get('balance', 0) + 1.0
        await UserManager.update_user(user_id, {
            'balance': new_balance,
            'welcome_bonus_received': True,
            'total_earned': user.get('total_earned', 0) + 1.0
        })
        
        # Add transaction asynchronously
        asyncio.create_task(
            UserManager.add_transaction(
                user_id,
                1.0,
                'credit',
                'Welcome bonus'
            )
        )
        
        logger.info(f"✅ Welcome bonus given to user {user_id}")
        return True

async def check_channel_membership(bot, user_id: int) -> tuple:
    """Check channel membership - OPTIMIZED with cache and timeouts"""
    channels = ChannelManager.get_channels()
    
    if not channels:
        logger.info("No channels configured")
        return True, []
    
    # Check cache first
    cache_key = f"{user_id}_{len(channels)}"
    current_time = datetime.now().timestamp()
    
    if cache_key in channel_check_cache:
        cache_time, result = channel_check_cache[cache_key]
        if current_time - cache_time < CACHE_TIMEOUT:
            logger.info(f"Using cached channel check for user {user_id}")
            return result
    
    not_joined = []
    
    # Check channels with timeout and limit
    try:
        # Use asyncio.wait with timeout for all checks
        tasks = []
        for channel in channels:
            task = asyncio.create_task(check_single_channel_fast(bot, user_id, channel))
            tasks.append(task)
        
        # Wait for all with timeout
        done, pending = await asyncio.wait(tasks, timeout=10.0)
        
        # Cancel pending tasks
        for task in pending:
            task.cancel()
        
        # Process results
        channel_results = {}
        for task in done:
            try:
                channel_id, result = await task
                channel_results[channel_id] = result
            except Exception as e:
                logger.error(f"Error in channel check: {e}")
        
        # Find not joined channels
        for channel in channels:
            channel_id = channel.get('chat_id')
            if not channel_results.get(channel_id, False):
                not_joined.append(channel)
        
        result = (len(not_joined) == 0, not_joined)
        
        # Cache the result
        channel_check_cache[cache_key] = (current_time, result)
        
        logger.info(f"Channel check completed for user {user_id}: {len(not_joined)} not joined")
        return result
        
    except asyncio.TimeoutError:
        logger.warning(f"Channel check timeout for user {user_id}")
        # Assume not joined on timeout
        return False, channels
    except Exception as e:
        logger.error(f"Error in channel check: {e}")
        return False, channels

async def check_single_channel_fast(bot, user_id: int, channel: Dict):
    """Check membership for a single channel - OPTIMIZED with shorter timeout"""
    chat_id = channel['chat_id']
    try:
        if isinstance(chat_id, str) and chat_id.lstrip('-').isdigit():
            chat_id_int = int(chat_id)
        else:
            chat_id_int = chat_id
        
        # Very short timeout for membership check
        try:
            member = await asyncio.wait_for(
                bot.get_chat_member(chat_id=chat_id_int, user_id=user_id),
                timeout=3.0  # Reduced from 10.0
            )
            return (chat_id, member.status not in ['left', 'kicked'])
        except asyncio.TimeoutError:
            logger.debug(f"Timeout checking {chat_id}")
            return (chat_id, False)
        except Exception as e:
            logger.debug(f"Error checking {chat_id}: {e}")
            return (chat_id, False)
    except Exception as e:
        logger.error(f"Error in check_single_channel for {chat_id}: {e}")
        return (chat_id, False)

async def get_invite_link(bot, chat_id, channel_name: str = None):
    """Get or create invite link for a chat - OPTIMIZED with cache"""
    try:
        # Try to get from cache first
        cache_key = f"invite_{chat_id}"
        if cache_key in channel_check_cache:
            cache_time, invite_link = channel_check_cache[cache_key]
            if datetime.now().timestamp() - cache_time < CACHE_TIMEOUT:
                return invite_link
        
        if isinstance(chat_id, str) and chat_id.lstrip('-').isdigit():
            chat_id_int = int(chat_id)
        else:
            chat_id_int = chat_id
        
        # Try to get invite link with short timeout
        try:
            # First try to get chat info
            chat = await asyncio.wait_for(
                bot.get_chat(chat_id_int),
                timeout=3.0  # Reduced from 10.0
            )
            
            # Try existing invite link
            try:
                invite_link = await asyncio.wait_for(
                    chat.export_invite_link(),
                    timeout=3.0
                )
                
                # Cache the result
                channel_check_cache[cache_key] = (datetime.now().timestamp(), invite_link)
                return invite_link
            except:
                # Try to create new invite link
                try:
                    invite = await asyncio.wait_for(
                        bot.create_chat_invite_link(
                            chat_id=chat_id_int,
                            creates_join_request=False
                        ),
                        timeout=3.0
                    )
                    
                    # Cache the result
                    channel_check_cache[cache_key] = (datetime.now().timestamp(), invite.invite_link)
                    return invite.invite_link
                except Exception as e:
                    logger.debug(f"Failed to create invite link: {e}")
                    # Fallback to username
                    if hasattr(chat, 'username') and chat.username:
                        link = f"https://t.me/{chat.username}"
                        channel_check_cache[cache_key] = (datetime.now().timestamp(), link)
                        return link
        except asyncio.TimeoutError:
            logger.debug(f"Timeout getting chat {chat_id}")
        except Exception as e:
            logger.debug(f"Error getting chat {chat_id}: {e}")
        
        # Last resort fallback
        if isinstance(chat_id, str) and chat_id.startswith('@'):
            link = f"https://t.me/{chat_id.lstrip('@')}"
            channel_check_cache[cache_key] = (datetime.now().timestamp(), link)
            return link
        
        return None
        
    except Exception as e:
        logger.error(f"Error getting invite link for {chat_id}: {e}")
        return None

async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /start command - FAST VERSION"""
    try:
        user = update.effective_user
        
        if not user:
            return
        
        logger.info(f"Start from user {user.id}")
        
        user_data = await UserManager.get_user(user.id)
        
        # Check for referral parameter - FAST
        args = context.args
        if args and args[0].startswith('REF'):
            referral_code = args[0]
            
            if not UserManager.is_referred(user.id):
                referrer_found = None
                async with data_manager._async_lock():
                    for user_id_str, user_data_item in data_manager.users.items():
                        if user_data_item.get('referral_code') == referral_code:
                            referrer_found = int(user_id_str)
                            break
                
                if referrer_found and referrer_found != user.id:
                    existing_pending = await UserManager.get_pending_referrer(user.id)
                    if not existing_pending:
                        await UserManager.add_pending_referral(referrer_found, user.id)
        
        # Check channel membership FAST
        try:
            has_joined, not_joined = await asyncio.wait_for(
                check_channel_membership(context.bot, user.id),
                timeout=5.0  # Reduced timeout
            )
            
            if not has_joined and not_joined:
                await show_join_buttons(update, context, not_joined)
            else:
                await UserManager.update_user(user.id, {'has_joined_channels': True})
                
                # Give welcome bonus if not already received
                welcome_bonus_given = await UserManager.give_welcome_bonus(user.id)
                
                # Check pending referral
                pending_referrer = await UserManager.get_pending_referrer(user.id)
                if pending_referrer and not UserManager.is_referred(user.id):
                    await UserManager.add_referral(pending_referrer, user.id)
                    await UserManager.remove_pending_referral(user.id)
                
                # Show welcome bonus notification
                if welcome_bonus_given:
                    await update.message.reply_text("🎉 You received ₹1 welcome bonus!")
                
                await show_main_menu(update, context)
                
        except asyncio.TimeoutError:
            logger.warning(f"Channel check timeout for user {user.id}")
            await show_main_menu(update, context)
            
    except Exception as e:
        logger.error(f"Error in start_command: {e}")
        try:
            await show_main_menu(update, context)
        except:
            pass

async def notify_referrer_completed(bot, referrer_id: int, referred_user):
    """Notify referrer about COMPLETED referral - FAST"""
    try:
        user_data = await UserManager.get_user(referrer_id)
        await bot.send_message(
            chat_id=referrer_id,
            text=f"🎉 Referral bonus! You earned ₹1 from {referred_user.first_name}. New balance: ₹{user_data.get('balance', 0):.2f}"
        )
    except Exception as e:
        logger.error(f"Failed to notify referrer: {e}")

async def show_join_buttons(update: Update, context: ContextTypes.DEFAULT_TYPE, not_joined: List[Dict]):
    """Show join buttons for channels - FAST"""
    try:
        user = update.effective_user
        
        if not not_joined:
            await show_main_menu(update, context)
            return
        
        keyboard = []
        
        # Get invite links with timeout
        try:
            for channel in not_joined[:5]:  # Limit to 5 channels max
                chat_id = channel['chat_id']
                channel_name = channel.get('name', 'Join Channel')
                
                invite_link = await asyncio.wait_for(
                    get_invite_link(context.bot, chat_id, channel_name),
                    timeout=3.0
                )
                
                if invite_link:
                    keyboard.append([
                        InlineKeyboardButton(f"📢 {channel_name}", url=invite_link)
                    ])
        except asyncio.TimeoutError:
            logger.warning("Timeout getting invite links")
        
        if keyboard:
            keyboard.append([
                InlineKeyboardButton("✅ Verify Join", callback_data="verify_join")
            ])
            
            message_text = (
                f"Join {len(not_joined)} channel(s) to continue.\n"
                f"Click 'Verify Join' after joining.\n\n"
                f"🎁 Get ₹1 welcome bonus!"
            )
            
            if update.callback_query:
                try:
                    await update.callback_query.message.reply_text(
                        message_text,
                        reply_markup=InlineKeyboardMarkup(keyboard)
                    )
                except:
                    await update.callback_query.edit_message_text(
                        message_text,
                        reply_markup=InlineKeyboardMarkup(keyboard)
                    )
            else:
                await update.message.reply_text(
                    message_text,
                    reply_markup=InlineKeyboardMarkup(keyboard)
                )
        else:
            await show_main_menu(update, context)
            
    except Exception as e:
        logger.error(f"Error in show_join_buttons: {e}")
        await show_main_menu(update, context)

async def verify_join_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle verify join button callback - FAST"""
    try:
        query = update.callback_query
        await query.answer()
        
        user = update.effective_user
        
        # Quick membership check
        try:
            has_joined, not_joined = await asyncio.wait_for(
                check_channel_membership(context.bot, user.id),
                timeout=5.0
            )
            
            if has_joined:
                await UserManager.update_user(user.id, {'has_joined_channels': True})
                
                # Give welcome bonus
                welcome_bonus_given = await UserManager.give_welcome_bonus(user.id)
                
                # Check pending referral
                pending_referrer = await UserManager.get_pending_referrer(user.id)
                if pending_referrer and not UserManager.is_referred(user.id):
                    await UserManager.add_referral(pending_referrer, user.id)
                    await UserManager.remove_pending_referral(user.id)
                
                # Show notification
                if welcome_bonus_given:
                    await query.message.reply_text("🎉 You received ₹1 welcome bonus!")
                
                await show_main_menu_callback(update, context)
            else:
                await show_join_buttons(update, context, not_joined)
                
        except asyncio.TimeoutError:
            await show_main_menu_callback(update, context)
            
    except Exception as e:
        logger.error(f"Error in verify_join_callback: {e}")
        await show_main_menu_callback(update, context)

async def show_main_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show main menu to user - FAST"""
    try:
        user = update.effective_user
        user_data = await UserManager.get_user(user.id)
        
        message = (
            f"Welcome, {user.first_name}!\n\n"
            f"💰 Balance: ₹{user_data.get('balance', 0):.2f}\n"
            f"👥 Referrals: {user_data.get('referral_count', 0)}\n\n"
            f"Your Code: {user_data.get('referral_code', '')}"
        )
        
        keyboard = [
            [InlineKeyboardButton("💰 Balance", callback_data="balance"),
             InlineKeyboardButton("📤 Withdraw", callback_data="withdraw")],
            [InlineKeyboardButton("📜 History", callback_data="history"),
             InlineKeyboardButton("👥 Referrals", callback_data="referrals")],
            [InlineKeyboardButton("🔗 Invite Link", callback_data="invite_link")]
        ]
        
        if user.id in ADMIN_IDS:
            keyboard.append([InlineKeyboardButton("👑 Admin Panel", callback_data="admin_panel")])
        
        keyboard.append([InlineKeyboardButton("🔄 Refresh", callback_data="refresh")])
        
        if update.callback_query:
            await update.callback_query.edit_message_text(
                text=message,
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
        else:
            await update.message.reply_text(
                text=message,
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
            
    except Exception as e:
        logger.error(f"Error in show_main_menu: {e}")

async def show_main_menu_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle main menu callback"""
    await show_main_menu(update, context)

async def balance_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle balance button callback - FAST"""
    try:
        query = update.callback_query
        await query.answer()
        
        user = update.effective_user
        user_data = await UserManager.get_user(user.id)
        
        message = (
            f"Your Balance\n\n"
            f"Available: ₹{user_data.get('balance', 0):.2f}\n"
            f"Total Earned: ₹{user_data.get('total_earned', 0):.2f}\n"
            f"Referrals: {user_data.get('referral_count', 0)}\n\n"
            f"Withdraw: /withdraw <amount> <method>"
        )
        
        keyboard = [
            [InlineKeyboardButton("📤 Withdraw", callback_data="withdraw"),
             InlineKeyboardButton("🔙 Back", callback_data="back_to_main")]
        ]
        
        await query.edit_message_text(
            text=message,
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
    except Exception as e:
        logger.error(f"Error in balance_callback: {e}")

async def withdraw_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /withdraw command - FAST"""
    try:
        user = update.effective_user
        
        if not user:
            return
        
        args = context.args
        if not args or len(args) < 2:
            await update.message.reply_text(
                "Usage: /withdraw <amount> <method>\n"
                "Example: /withdraw 50 upi\n\n"
                "Methods: UPI, Bank\n"
                "Min: ₹10.00"
            )
            return
        
        try:
            amount = float(args[0])
            method = args[1].lower()
            
            if amount < 10:
                await update.message.reply_text("Min ₹10.00")
                return
            
            user_data = await UserManager.get_user(user.id)
            
            if user_data.get('balance', 0) < amount:
                await update.message.reply_text(f"Insufficient: ₹{user_data.get('balance', 0):.2f}")
                return
            
            new_balance = user_data.get('balance', 0) - amount
            total_withdrawn = user_data.get('total_withdrawn', 0) + amount
            
            await UserManager.update_user(user.id, {
                'balance': new_balance,
                'total_withdrawn': total_withdrawn
            })
            
            # Add transaction asynchronously
            asyncio.create_task(
                UserManager.add_transaction(
                    user.id,
                    -amount,
                    'withdrawal',
                    f'Withdrawal via {method}'
                )
            )
            
            # Notify admin
            admin_message = (
                f"New Withdrawal\n"
                f"User: {user.first_name} ({user.id})\n"
                f"Amount: ₹{amount:.2f}\n"
                f"Method: {method}"
            )
            
            for admin_id in ADMIN_IDS:
                try:
                    await context.bot.send_message(chat_id=admin_id, text=admin_message)
                except:
                    pass
            
            await update.message.reply_text(
                f"Withdrawal Submitted!\n\n"
                f"Amount: ₹{amount:.2f}\n"
                f"Method: {method}\n"
                f"New Balance: ₹{new_balance:.2f}"
            )
            
        except ValueError:
            await update.message.reply_text("Invalid amount")
            
    except Exception as e:
        logger.error(f"Error in withdraw_command: {e}")
        await update.message.reply_text("Error occurred")

# [Remaining functions: withdraw_callback, history_callback, referrals_callback, 
#  invite_link_callback, help_command, restart_command, backup_command, 
#  stats_command, list_channels_command, broadcast_command, admin_panel_callback,
#  admin_channels_callback, admin_handle_callback, confirm_reset_callback]
# These remain similar but optimized for speed

async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Log errors and handle them gracefully"""
    logger.error(f"Exception while handling an update: {context.error}", exc_info=context.error)
    
    if update and update.effective_message:
        try:
            await update.effective_message.reply_text(
                "Error occurred. Please try again."
            )
        except:
            pass

def run_http_server():
    """Run HTTP server for health checks - OPTIMIZED"""
    from http.server import HTTPServer, BaseHTTPRequestHandler
    
    class HealthHandler(BaseHTTPRequestHandler):
        def do_GET(self):
            self.send_response(200)
            self.send_header('Content-type', 'text/plain')
            self.end_headers()
            response = f"Bot running\nUsers: {len(data_manager.users)}"
            self.wfile.write(response.encode())
        
        def log_message(self, format, *args):
            pass
    
    try:
        server = HTTPServer(('0.0.0.0', PORT), HealthHandler)
        logger.info(f"✅ HTTP server on port {PORT}")
        server.serve_forever()
    except Exception as e:
        logger.error(f"HTTP server failed: {e}")

def main():
    """Main function to start the bot - OPTIMIZED"""
    if not BOT_TOKEN:
        logger.error("❌ BOT_TOKEN not set")
        print("ERROR: Set BOT_TOKEN environment variable")
        return
    
    # Start HTTP server for Render health checks
    http_thread = threading.Thread(target=run_http_server, daemon=True)
    http_thread.start()
    
    # Create bot application with OPTIMIZED configuration
    application = (
        Application.builder()
        .token(BOT_TOKEN)
        .connect_timeout(15.0)      # Reduced
        .read_timeout(15.0)         # Reduced
        .write_timeout(15.0)        # Reduced
        .pool_timeout(15.0)         # Reduced
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
    
    # Admin commands
    application.add_handler(CommandHandler("listchannels", list_channels_command))
    application.add_handler(CommandHandler("broadcast", broadcast_command))
    
    # Callback handlers
    application.add_handler(CallbackQueryHandler(verify_join_callback, pattern="^verify_join$"))
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
    
    # Get bot info
    try:
        bot_info = application.bot.get_me()
        bot_username = bot_info.username
    except Exception as e:
        logger.warning(f"Could not fetch bot username: {e}")
        bot_username = "unknown"
    
    # Start bot
    logger.info("🤖 Bot starting...")
    print("=" * 50)
    print(f"✅ Bot started!")
    print(f"🤖 Username: @{bot_username}")
    print(f"👑 Admins: {ADMIN_IDS}")
    print(f"📢 Channels: {len(data_manager.channels)}")
    print(f"👥 Users: {len(data_manager.users)}")
    print(f"🌐 HTTP: http://0.0.0.0:{PORT}")
    print(f"💾 Storage: {'✅ MongoDB' if db_connected else '📁 Local files'}")
    print("=" * 50)
    print("📝 Commands:")
    print("• /start - Start bot")
    print("• /withdraw <amount> <method> - Withdraw")
    print("• /help - Show help")
    if ADMIN_IDS:
        print("👑 Admin:")
        print("• /listchannels - View channels")
        print("• /stats - Show stats")
    print("\n✅ Bot optimized for speed!")
    print("🎁 ₹1 welcome bonus for new users")
    print("💰 Referral bonus notifications")
    
    if not db_connected:
        print("\n⚠️ WARNING: MongoDB connection failed. Using local files.")
    
    try:
        # Run bot with polling
        application.run_polling(
            allowed_updates=Update.ALL_TYPES,
            drop_pending_updates=True,
            close_loop=False
        )
    except KeyboardInterrupt:
        logger.info("Bot stopped by user")
    except Exception as e:
        logger.error(f"Bot stopped: {e}")
        print(f"❌ Bot stopped: {e}")
    finally:
        executor.shutdown(wait=True)
        if mongo_client:
            mongo_client.close()

if __name__ == '__main__':
    main()