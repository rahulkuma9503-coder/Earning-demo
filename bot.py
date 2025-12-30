import os
import logging
import asyncio
import sys
from datetime import datetime
from typing import List, Dict, Optional
import json
from dotenv import load_dotenv

from telegram import (
    Update, 
    InlineKeyboardButton, 
    InlineKeyboardMarkup
)
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    ContextTypes
)
from telegram.constants import ParseMode
from motor.motor_asyncio import AsyncIOMotorClient

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

# Environment variable for initial channels
INITIAL_CHANNELS_ENV = os.getenv('INITIAL_CHANNELS', '')
if INITIAL_CHANNELS_ENV:
    INITIAL_CHANNELS = [cid.strip() for cid in INITIAL_CHANNELS_ENV.split(',') if cid.strip()]
else:
    INITIAL_CHANNELS = []

logger.info(f"📢 Initial channels from env: {INITIAL_CHANNELS}")

# Global variables for async database
mongo_client = None
db = None
channels_collection = None
users_collection = None
referrals_collection = None
pending_referrals_collection = None

# Cache for frequent operations
user_cache = {}
CACHE_TTL = 300

async def init_database():
    """Initialize MongoDB connection asynchronously"""
    global mongo_client, db, channels_collection, users_collection, referrals_collection, pending_referrals_collection
    
    if not MONGODB_URI:
        logger.warning("⚠️ MONGODB_URI not set. Using file-based storage.")
        return False
    
    try:
        logger.info(f"🔗 Attempting to connect to MongoDB...")
        
        # Use async MongoDB driver
        mongo_client = AsyncIOMotorClient(
            MONGODB_URI,
            serverSelectionTimeoutMS=10000,
            connectTimeoutMS=30000,
            socketTimeoutMS=30000,
            maxPoolSize=100
        )
        
        # Test connection
        await mongo_client.admin.command('ping')
        
        db = mongo_client['telegram_referral_bot']
        
        # Initialize collections
        channels_collection = db['channels']
        users_collection = db['users']
        referrals_collection = db['referrals']
        pending_referrals_collection = db['pending_referrals']
        
        # Create indexes asynchronously
        await users_collection.create_index('user_id', unique=True)
        await channels_collection.create_index('chat_id', unique=True)
        await referrals_collection.create_index([('referrer_id', 1), ('referred_id', 1)], unique=True)
        await pending_referrals_collection.create_index('referred_id', unique=True)
        await pending_referrals_collection.create_index('referrer_id')
        await pending_referrals_collection.create_index('created_at', expireAfterSeconds=604800)
        
        logger.info("✅ MongoDB connected successfully")
        return True
        
    except Exception as e:
        logger.error(f"❌ MongoDB error: {e}")
        logger.warning("📁 Using file-based storage as fallback")
        return False

class Storage:
    """Async storage manager with MongoDB"""
    
    @staticmethod
    async def save_channels(channels: List[Dict]):
        """Save channels to storage asynchronously"""
        try:
            if channels_collection is not None:
                await channels_collection.delete_many({})
                if channels:
                    await channels_collection.insert_many(channels)
            else:
                with open('channels_backup.json', 'w') as f:
                    json.dump(channels, f, default=str)
        except Exception as e:
            logger.error(f"Error saving channels: {e}")
    
    @staticmethod
    async def load_channels() -> List[Dict]:
        """Load channels from storage asynchronously"""
        try:
            if channels_collection is not None:
                cursor = channels_collection.find({}, {'_id': 0})
                return await cursor.to_list(length=None)
            else:
                if os.path.exists('channels_backup.json'):
                    with open('channels_backup.json', 'r') as f:
                        return json.load(f)
                return []
        except Exception as e:
            logger.error(f"Error loading channels: {e}")
            return []
    
    @staticmethod
    async def save_user(user_id: int, user_data: Dict):
        """Save single user to storage asynchronously"""
        try:
            if users_collection is not None:
                await users_collection.update_one(
                    {'user_id': user_id},
                    {'$set': user_data},
                    upsert=True
                )
                user_cache[user_id] = (user_data, datetime.now())
            else:
                users = {}
                if os.path.exists('users_backup.json'):
                    with open('users_backup.json', 'r') as f:
                        users = json.load(f)
                users[str(user_id)] = user_data
                with open('users_backup.json', 'w') as f:
                    json.dump(users, f, default=str)
        except Exception as e:
            logger.error(f"Error saving user {user_id}: {e}")
    
    @staticmethod
    async def get_user(user_id: int) -> Optional[Dict]:
        """Get user data asynchronously with caching"""
        if user_id in user_cache:
            user_data, timestamp = user_cache[user_id]
            if (datetime.now() - timestamp).seconds < CACHE_TTL:
                return user_data
        
        try:
            if users_collection is not None:
                user = await users_collection.find_one({'user_id': user_id}, {'_id': 0})
                if user:
                    user_cache[user_id] = (user, datetime.now())
                return user
            else:
                if os.path.exists('users_backup.json'):
                    with open('users_backup.json', 'r') as f:
                        users = json.load(f)
                    return users.get(str(user_id))
                return None
        except Exception as e:
            logger.error(f"Error loading user {user_id}: {e}")
            return None
    
    @staticmethod
    async def get_all_users() -> Dict:
        """Get all users asynchronously"""
        try:
            if users_collection is not None:
                users = {}
                cursor = users_collection.find({})
                async for user in cursor:
                    user_id = user.get('user_id')
                    if user_id:
                        user_dict = {k: v for k, v in user.items() if k != '_id'}
                        users[str(user_id)] = user_dict
                return users
            else:
                if os.path.exists('users_backup.json'):
                    with open('users_backup.json', 'r') as f:
                        return json.load(f)
                return {}
        except Exception as e:
            logger.error(f"Error loading all users: {e}")
            return {}
    
    @staticmethod
    async def save_referrals(referrals: Dict):
        """Save referrals to storage asynchronously"""
        try:
            if referrals_collection is not None:
                await referrals_collection.delete_many({})
                referrals_list = []
                for referred_id, referrer_id in referrals.items():
                    referrals_list.append({
                        'referred_id': int(referred_id),
                        'referrer_id': int(referrer_id),
                        'created_at': datetime.now()
                    })
                if referrals_list:
                    await referrals_collection.insert_many(referrals_list)
            else:
                with open('referrals_backup.json', 'w') as f:
                    json.dump(referrals, f, default=str)
        except Exception as e:
            logger.error(f"Error saving referrals: {e}")
    
    @staticmethod
    async def load_referrals() -> Dict:
        """Load referrals from storage asynchronously"""
        try:
            if referrals_collection is not None:
                referrals = {}
                cursor = referrals_collection.find({})
                async for ref in cursor:
                    referred_id = ref.get('referred_id')
                    referrer_id = ref.get('referrer_id')
                    if referred_id and referrer_id:
                        referrals[str(referred_id)] = str(referrer_id)
                return referrals
            else:
                if os.path.exists('referrals_backup.json'):
                    with open('referrals_backup.json', 'r') as f:
                        return json.load(f)
                return {}
        except Exception as e:
            logger.error(f"Error loading referrals: {e}")
            return {}
    
    @staticmethod
    async def save_pending_referral(referrer_id: int, referred_id: int):
        """Save pending referral asynchronously"""
        try:
            if pending_referrals_collection is not None:
                await pending_referrals_collection.update_one(
                    {'referred_id': referred_id},
                    {'$set': {
                        'referrer_id': referrer_id,
                        'referred_id': referred_id,
                        'created_at': datetime.now()
                    }},
                    upsert=True
                )
            else:
                pending_referrals = {}
                if os.path.exists('pending_referrals_backup.json'):
                    with open('pending_referrals_backup.json', 'r') as f:
                        pending_referrals = json.load(f)
                pending_referrals[str(referred_id)] = referrer_id
                with open('pending_referrals_backup.json', 'w') as f:
                    json.dump(pending_referrals, f, default=str)
        except Exception as e:
            logger.error(f"Error saving pending referral: {e}")
    
    @staticmethod
    async def remove_pending_referral(referred_id: int):
        """Remove pending referral asynchronously"""
        try:
            if pending_referrals_collection is not None:
                await pending_referrals_collection.delete_one({'referred_id': referred_id})
            else:
                if os.path.exists('pending_referrals_backup.json'):
                    with open('pending_referrals_backup.json', 'r') as f:
                        pending_referrals = json.load(f)
                    if str(referred_id) in pending_referrals:
                        del pending_referrals[str(referred_id)]
                        with open('pending_referrals_backup.json', 'w') as f:
                            json.dump(pending_referrals, f, default=str)
        except Exception as e:
            logger.error(f"Error removing pending referral: {e}")
    
    @staticmethod
    async def get_pending_referrer(referred_id: int) -> Optional[int]:
        """Get pending referrer ID asynchronously"""
        try:
            if pending_referrals_collection is not None:
                pending = await pending_referrals_collection.find_one({'referred_id': referred_id})
                if pending:
                    return pending.get('referrer_id')
                return None
            else:
                if os.path.exists('pending_referrals_backup.json'):
                    with open('pending_referrals_backup.json', 'r') as f:
                        pending_referrals = json.load(f)
                    return pending_referrals.get(str(referred_id))
                return None
        except Exception as e:
            logger.error(f"Error getting pending referrer: {e}")
            return None

class DataManager:
    """Manage all data with async storage"""
    
    def __init__(self):
        self.channels = []
        self.users = {}
        self.referrals = {}
        self._lock = asyncio.Lock()
        
    async def initialize(self):
        """Initialize data asynchronously"""
        logger.info("📂 Loading data from storage...")
        
        load_tasks = [
            self._load_channels(),
            self._load_users(),
            self._load_referrals()
        ]
        
        results = await asyncio.gather(*load_tasks, return_exceptions=True)
        
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                logger.error(f"Error loading data type {i}: {result}")
        
        logger.info(f"✅ Loaded {len(self.channels)} channels, {len(self.users)} users, {len(self.referrals)} referrals")
        await self.init_channels_from_env()
    
    async def _load_channels(self):
        """Load channels asynchronously"""
        self.channels = await Storage.load_channels()
    
    async def _load_users(self):
        """Load users asynchronously"""
        self.users = await Storage.get_all_users()
    
    async def _load_referrals(self):
        """Load referrals asynchronously"""
        self.referrals = await Storage.load_referrals()
    
    async def init_channels_from_env(self):
        """Initialize channels from environment variable"""
        if INITIAL_CHANNELS:
            logger.info(f"📢 Initializing channels from environment variable: {INITIAL_CHANNELS}")
            valid_channels = 0
            for chat_id in INITIAL_CHANNELS:
                if chat_id and await self.add_channel_from_env(chat_id):
                    valid_channels += 1
            logger.info(f"✅ Added {valid_channels} valid channels from environment")
        else:
            logger.warning("⚠️ No channels configured in INITIAL_CHANNELS environment variable")
    
    async def add_channel_from_env(self, chat_id: str) -> bool:
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
                chat_id_str = clean_id
                logger.info(f"Channel is username format: {chat_id_str}")
            elif clean_id.startswith('-100'):
                chat_id_str = clean_id
                logger.info(f"Channel is channel ID format: {chat_id_str}")
            elif clean_id.startswith('-'):
                chat_id_str = clean_id
                logger.info(f"Channel is group ID format: {chat_id_str}")
            elif clean_id.isdigit() and len(clean_id) > 9:
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
    
    async def backup_all_data(self):
        """Backup all data to storage asynchronously"""
        logger.info("💾 Backing up data to storage...")
        async with self._lock:
            await Storage.save_channels(self.channels)
            await Storage.save_referrals(self.referrals)
        logger.info(f"✅ Data backed up: {len(self.channels)} channels, {len(self.referrals)} referrals")
    
    def get_stats(self) -> str:
        """Get data statistics"""
        total_balance = sum(u.get('balance', 0) for u in self.users.values())
        return (
            f"📊 <b>Database Statistics:</b>\n\n"
            f"📢 <b>Channels:</b> {len(self.channels)}\n"
            f"👥 <b>Users:</b> {len(self.users)}\n"
            f"🔗 <b>Referrals:</b> {len(self.referrals)}\n"
            f"💰 <b>Total Balance:</b> ₹{total_balance:.2f}\n"
            f"💾 <b>Storage:</b> {'✅ MongoDB' if mongo_client else '📁 Local files'}"
        )

# Global data manager
data_manager = DataManager()

class ChannelManager:
    """Manage channels"""
    
    @staticmethod
    def get_channels() -> List[Dict]:
        return data_manager.channels

class UserManager:
    """Manage users with async operations"""
    
    @staticmethod
    async def get_user(user_id: int) -> Dict:
        """Get user data asynchronously with caching"""
        user_str = str(user_id)
        
        # Check in-memory cache first
        if user_str in data_manager.users:
            return data_manager.users[user_str]
        
        # Check database
        user_data = await Storage.get_user(user_id)
        
        if user_data:
            # Update cache
            data_manager.users[user_str] = user_data
            return user_data
        
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
        
        # Save to storage
        await Storage.save_user(user_id, user_data)
        
        # Update cache
        data_manager.users[user_str] = user_data
        
        return user_data
    
    @staticmethod
    async def update_user(user_id: int, updates: Dict):
        """Update user data asynchronously"""
        user_str = str(user_id)
        
        # Get current user data
        user_data = await UserManager.get_user(user_id)
        
        # Apply updates
        user_data.update(updates)
        user_data['last_active'] = datetime.now().isoformat()
        
        # Save to storage
        await Storage.save_user(user_id, user_data)
        
        # Update cache
        data_manager.users[user_str] = user_data
    
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
        """Add referral asynchronously"""
        if referrer_id == referred_id:
            return False
        
        referred_str = str(referred_id)
        
        # Check if already referred
        if referred_str in data_manager.referrals:
            logger.info(f"User {referred_id} already referred by {data_manager.referrals[referred_str]}")
            return False
        
        # Record referral
        data_manager.referrals[referred_str] = str(referrer_id)
        
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
            f'Referral bonus for user {referred_id}'
        )
        
        # Save referrals to storage
        await Storage.save_referrals(data_manager.referrals)
        
        logger.info(f"✅ New referral completed: {referrer_id} → {referred_id}")
        return True
    
    @staticmethod
    async def add_pending_referral(referrer_id: int, referred_id: int):
        """Add pending referral"""
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
        """Give ₹1 welcome bonus to new user"""
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
    """Check channel membership concurrently with semaphore for rate limiting"""
    channels = ChannelManager.get_channels()
    
    if not channels:
        logger.info("No channels configured, skipping membership check")
        return True, []
    
    # Use semaphore to limit concurrent requests
    semaphore = asyncio.Semaphore(5)
    
    async def check_single_channel_with_semaphore(channel):
        async with semaphore:
            return await check_single_channel(bot, user_id, channel)
    
    tasks = [check_single_channel_with_semaphore(channel) for channel in channels]
    
    # Use asyncio.gather with timeout
    try:
        results = await asyncio.gather(*tasks, return_exceptions=True)
        not_joined = []
        
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                logger.error(f"Error checking channel {channels[i]['chat_id']}: {result}")
                not_joined.append(channels[i])
            elif not result:
                not_joined.append(channels[i])
        
        logger.info(f"User {user_id} membership: joined={len(not_joined) == 0}, not_joined={len(not_joined)}")
        return len(not_joined) == 0, not_joined
    
    except Exception as e:
        logger.error(f"Error in channel check: {e}")
        return False, channels

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
                timeout=5.0
            )
            return member.status not in ['left', 'kicked']
        except asyncio.TimeoutError:
            logger.warning(f"Timeout checking {chat_id}")
            return False
        except Exception as e:
            if "user not found" in str(e).lower():
                return False
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
        
        # Add timeout
        try:
            chat = await asyncio.wait_for(
                bot.get_chat(chat_id_int),
                timeout=5.0
            )
        except asyncio.TimeoutError:
            logger.warning(f"Timeout getting chat {chat_id}")
            return None
        
        # Try to get existing invite link
        try:
            invite_link = await asyncio.wait_for(
                chat.export_invite_link(),
                timeout=5.0
            )
            logger.info(f"Got existing invite link for {channel_name or chat_id}")
            return invite_link
        except:
            # If no invite link exists, try to create one
            try:
                invite_link = await asyncio.wait_for(
                    bot.create_chat_invite_link(
                        chat_id=chat_id_int,
                        creates_join_request=False
                    ),
                    timeout=5.0
                )
                logger.info(f"Created new invite link for {channel_name or chat_id}")
                return invite_link.invite_link
            except Exception as e:
                logger.error(f"Failed to create invite link: {e}")
                # Fallback to username if available
                if hasattr(chat, 'username') and chat.username:
                    return f"https://t.me/{chat.username}"
                return None
    except Exception as e:
        logger.error(f"Error getting invite link for {chat_id}: {e}")
        return None

async def notify_referrer_completed(bot, referrer_id: int, referred_user):
    """Notify referrer about COMPLETED referral"""
    try:
        user_data = await UserManager.get_user(referrer_id)
        await bot.send_message(
            chat_id=referrer_id,
            text=f"🎉 Referral bonus! You earned ₹1 from {referred_user.first_name}. New balance: ₹{user_data.get('balance', 0):.2f}"
        )
    except Exception as e:
        logger.error(f"Failed to notify referrer: {e}")

# ==================== COMMAND HANDLERS ====================

async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /start command"""
    try:
        user = update.effective_user
        
        if not user:
            return
        
        logger.info(f"📨 Start command from user {user.id}")
        
        # Get user data (async)
        await UserManager.get_user(user.id)
        
        # Check for referral parameter
        args = context.args
        if args and args[0].startswith('REF'):
            referral_code = args[0]
            logger.info(f"Referral code detected: {referral_code}")
            
            if not UserManager.is_referred(user.id):
                # Find referrer by code
                referrer_found = None
                for user_id_str, user_data_item in data_manager.users.items():
                    if user_data_item.get('referral_code') == referral_code:
                        referrer_found = int(user_id_str)
                        break
                
                if referrer_found and referrer_found != user.id:
                    # Store as pending referral
                    existing_pending = await UserManager.get_pending_referrer(user.id)
                    if not existing_pending:
                        await UserManager.add_pending_referral(referrer_found, user.id)
        
        # Check channel membership with timeout
        try:
            has_joined, not_joined = await asyncio.wait_for(
                check_channel_membership(context.bot, user.id),
                timeout=15.0
            )
            
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
                    # Complete the referral
                    is_new_referral = await UserManager.add_referral(pending_referrer, user.id)
                    
                    if is_new_referral:
                        await UserManager.remove_pending_referral(user.id)
                        # Notify referrer
                        asyncio.create_task(
                            notify_referrer_completed(context.bot, pending_referrer, user)
                        )
                
                # Show welcome bonus notification if given
                if welcome_bonus_given:
                    await update.message.reply_text("🎉 You received ₹1 welcome bonus!")
                
                # Show main menu
                await show_main_menu(update, context)
                
        except asyncio.TimeoutError:
            logger.warning(f"Timeout checking channels for user {user.id}")
            await show_main_menu(update, context)
            
    except Exception as e:
        logger.error(f"Error in start_command: {e}", exc_info=True)
        try:
            await show_main_menu(update, context)
        except:
            pass

async def show_join_buttons(update: Update, context: ContextTypes.DEFAULT_TYPE, not_joined: List[Dict]):
    """Show join buttons for channels"""
    try:
        user = update.effective_user
        
        if not not_joined:
            await show_main_menu(update, context)
            return
        
        keyboard = []
        
        # Get all invite links concurrently
        link_tasks = []
        for channel in not_joined:
            chat_id = channel['chat_id']
            channel_name = channel.get('name', 'Join Channel')
            
            task = asyncio.create_task(get_invite_link(context.bot, chat_id, channel_name))
            link_tasks.append((task, channel_name, chat_id))
        
        # Process results
        for task, channel_name, chat_id in link_tasks:
            try:
                invite_link = await asyncio.wait_for(task, timeout=5.0)
                if invite_link:
                    keyboard.append([
                        InlineKeyboardButton(f"📢 {channel_name}", url=invite_link)
                    ])
            except:
                continue
        
        # Only show verify button if we have at least one join button
        if keyboard:
            keyboard.append([
                InlineKeyboardButton("✅ Verify Join", callback_data="verify_join")
            ])
            
            message_text = (
                f"Welcome {user.first_name}!\n\n"
                f"Join {len(not_joined)} channel(s) to continue.\n"
                f"After joining, click 'Verify Join'.\n\n"
                f"🎁 Get ₹1 welcome bonus after joining!"
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

async def show_main_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show main menu to user"""
    try:
        user = update.effective_user
        user_data = await UserManager.get_user(user.id)
        
        message = (
            f"Welcome, {user.first_name}!\n\n"
            f"💰 Balance: ₹{user_data.get('balance', 0):.2f}\n"
            f"👥 Referrals: {user_data.get('referral_count', 0)}\n"
            f"📊 Total Earned: ₹{user_data.get('total_earned', 0):.2f}\n\n"
            f"Your Referral Code: {user_data.get('referral_code', '')}"
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

# ==================== CALLBACK HANDLERS ====================

async def show_main_menu_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await show_main_menu(update, context)

async def verify_join_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user = update.effective_user
    
    try:
        has_joined, not_joined = await asyncio.wait_for(
            check_channel_membership(context.bot, user.id),
            timeout=15.0
        )
        
        if has_joined:
            await UserManager.update_user(user.id, {'has_joined_channels': True})
            welcome_bonus_given = await UserManager.give_welcome_bonus(user.id)
            
            if welcome_bonus_given:
                await query.message.reply_text("🎉 You received ₹1 welcome bonus!")
            
            await show_main_menu(update, context)
        else:
            await show_join_buttons(update, context, not_joined)
            
    except asyncio.TimeoutError:
        await show_main_menu(update, context)
    except Exception as e:
        logger.error(f"Error in verify_join_callback: {e}")
        await show_main_menu(update, context)

async def balance_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user = update.effective_user
    user_data = await UserManager.get_user(user.id)
    
    await query.edit_message_text(
        text=f"💰 Your Balance: ₹{user_data.get('balance', 0):.2f}\n\nUse /withdraw <amount> <method> to withdraw.",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data="back_to_main")]])
    )

async def withdraw_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    await query.edit_message_text(
        text="📤 Withdrawal\n\nUsage: /withdraw <amount> <method>\nExample: /withdraw 50 upi\n\nMinimum: ₹10.00\nMethods: UPI, Bank Transfer",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data="back_to_main")]])
    )

async def history_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user = update.effective_user
    user_data = await UserManager.get_user(user.id)
    
    transactions = user_data.get('transactions', [])
    if not transactions:
        message = "📜 No transactions yet."
    else:
        recent_tx = transactions[-10:]
        tx_list = []
        for tx in reversed(recent_tx):
            sign = "+" if tx.get('type') == 'credit' else "-"
            tx_list.append(f"{sign}₹{tx.get('amount', 0):.2f} - {tx.get('description', '')}")
        
        message = "📜 Recent Transactions\n\n" + "\n".join(tx_list)
    
    await query.edit_message_text(
        text=message,
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data="back_to_main")]])
    )

async def referrals_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user = update.effective_user
    user_data = await UserManager.get_user(user.id)
    
    referral_code = user_data.get('referral_code', f"REF{user.id}")
    invite_link = f"https://t.me/{context.bot.username}?start={referral_code}"
    
    message = (
        f"👥 Your Referrals\n\n"
        f"Referral Code: {referral_code}\n"
        f"Total Referrals: {user_data.get('referral_count', 0)}\n"
        f"Earned from Referrals: ₹{user_data.get('total_earned', 0):.2f}\n\n"
        f"Share this link to earn ₹1.00 per referral:\n{invite_link}"
    )
    
    keyboard = [
        [InlineKeyboardButton("📤 Share", url=f"tg://msg_url?url={invite_link}&text=Join this bot to earn money!")],
        [InlineKeyboardButton("🔙 Back", callback_data="back_to_main")]
    ]
    
    await query.edit_message_text(
        text=message,
        reply_markup=InlineKeyboardMarkup(keyboard)
    )

async def invite_link_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user = update.effective_user
    user_data = await UserManager.get_user(user.id)
    
    referral_code = user_data.get('referral_code', f"REF{user.id}")
    invite_link = f"https://t.me/{context.bot.username}?start={referral_code}"
    
    await query.edit_message_text(
        text=f"🔗 Your Referral Link\n\n{invite_link}\n\nShare this link to earn ₹1.00 per successful referral!",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data="back_to_main")]])
    )

async def admin_panel_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    user = update.effective_user
    if user.id not in ADMIN_IDS:
        await query.answer("Admin only", show_alert=True)
        return
    
    stats = data_manager.get_stats()
    
    message = f"👑 Admin Panel\n\n{stats}"
    
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
        parse_mode=ParseMode.HTML
    )

async def admin_channels_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    user = update.effective_user
    if user.id not in ADMIN_IDS:
        await query.answer("Admin only", show_alert=True)
        return
    
    channels = ChannelManager.get_channels()
    
    if not channels:
        message = "No channels configured"
    else:
        channel_list = []
        for i, channel in enumerate(channels, 1):
            channel_list.append(f"{i}. {channel.get('name', 'Channel')} - {channel.get('chat_id')}")
        
        message = f"Configured Channels ({len(channels)})\n\n" + "\n".join(channel_list)
    
    await query.edit_message_text(
        text=message,
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data="admin_panel")]])
    )

async def admin_handle_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    data = query.data
    
    if data == "admin_stats":
        stats = data_manager.get_stats()
        await query.edit_message_text(
            text=stats,
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data="admin_panel")]]),
            parse_mode=ParseMode.HTML
        )
    
    elif data == "admin_backup":
        await data_manager.backup_all_data()
        await query.edit_message_text(
            text="✅ Data backed up successfully",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data="admin_panel")]])
        )
    
    elif data == "admin_restart":
        await query.edit_message_text(
            text="🔄 Bot restarting...",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data="admin_panel")]])
        )
        # In production, you would restart the bot process here

# ==================== OTHER COMMAND HANDLERS ====================

async def withdraw_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /withdraw command"""
    try:
        user = update.effective_user
        
        if not user:
            return
        
        args = context.args
        if not args or len(args) < 2:
            await update.message.reply_text(
                "Usage: /withdraw <amount> <method>\n"
                "Example: /withdraw 50 upi\n\n"
                "Minimum: ₹10.00\n"
                "Methods: UPI, Bank Transfer"
            )
            return
        
        try:
            amount = float(args[0])
            method = args[1].lower()
            
            if amount < 10:
                await update.message.reply_text("Minimum withdrawal amount is ₹10.00")
                return
            
            user_data = await UserManager.get_user(user.id)
            
            if user_data.get('balance', 0) < amount:
                await update.message.reply_text(f"Insufficient balance. You have ₹{user_data.get('balance', 0):.2f}")
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
                f"New Withdrawal Request\n\n"
                f"User: {user.first_name} (ID: {user.id})\n"
                f"Amount: ₹{amount:.2f}\n"
                f"Method: {method}\n"
                f"New Balance: ₹{new_balance:.2f}"
            )
            
            for admin_id in ADMIN_IDS:
                try:
                    await context.bot.send_message(chat_id=admin_id, text=admin_message)
                except Exception as e:
                    logger.error(f"Failed to notify admin {admin_id}: {e}")
            
            await update.message.reply_text(
                f"Withdrawal Request Submitted!\n\n"
                f"Amount: ₹{amount:.2f}\n"
                f"Method: {method}\n"
                f"New Balance: ₹{new_balance:.2f}\n\n"
                f"Your request has been sent to admin for processing."
            )
            
        except ValueError:
            await update.message.reply_text("Invalid amount. Please enter a valid number.")
            
    except Exception as e:
        logger.error(f"Error in withdraw_command: {e}")
        await update.message.reply_text("An error occurred. Please try again.")

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /help command"""
    await update.message.reply_text(
        "🤖 Bot Help\n\n"
        "Commands:\n"
        "/start - Start the bot\n"
        "/withdraw <amount> <method> - Withdraw money\n"
        "/help - Show this help\n\n"
        "How to Earn:\n"
        "1. Get ₹1 welcome bonus after joining channels\n"
        "2. Share your referral link\n"
        "3. Earn ₹1.00 per successful referral\n"
        "4. Minimum withdrawal: ₹10.00"
    )

async def restart_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /restart command (admin only)"""
    user = update.effective_user
    if user.id not in ADMIN_IDS:
        await update.message.reply_text("Admin only")
        return
    
    await update.message.reply_text("Bot restarting...")
    # In production, implement actual restart logic

async def backup_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /backup command (admin only)"""
    user = update.effective_user
    if user.id not in ADMIN_IDS:
        await update.message.reply_text("Admin only")
        return
    
    await data_manager.backup_all_data()
    await update.message.reply_text("✅ Data backed up successfully")

async def stats_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /stats command (admin only)"""
    user = update.effective_user
    if user.id not in ADMIN_IDS:
        await update.message.reply_text("Admin only")
        return
    
    stats = data_manager.get_stats()
    await update.message.reply_text(stats, parse_mode=ParseMode.HTML)

async def list_channels_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /listchannels command (admin only)"""
    user = update.effective_user
    if user.id not in ADMIN_IDS:
        await update.message.reply_text("Admin only")
        return
    
    channels = ChannelManager.get_channels()
    if not channels:
        message = "No channels configured"
    else:
        channel_list = []
        for i, channel in enumerate(channels, 1):
            channel_list.append(f"{i}. {channel.get('name', 'Channel')} - {channel.get('chat_id')}")
        
        message = f"Configured Channels ({len(channels)})\n\n" + "\n".join(channel_list)
    
    await update.message.reply_text(message)

async def broadcast_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /broadcast command (admin only)"""
    user = update.effective_user
    if user.id not in ADMIN_IDS:
        await update.message.reply_text("Admin only")
        return
    
    args = context.args
    if not args:
        await update.message.reply_text("Usage: /broadcast <message>")
        return
    
    message = " ".join(args)
    await update.message.reply_text(f"Broadcast feature would send: {message}")

async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Log errors and handle them gracefully"""
    logger.error(f"Exception while handling an update: {context.error}", exc_info=context.error)
    
    if update and update.effective_message:
        try:
            await update.effective_message.reply_text("An error occurred. Please try again later.")
        except:
            pass

# ==================== MAIN FUNCTION ====================

def main():
    """Main function to start the bot - compatible with Render"""
    if not BOT_TOKEN:
        logger.error("❌ BOT_TOKEN not set")
        return
    
    # Create bot application
    application = (
        Application.builder()
        .token(BOT_TOKEN)
        .concurrent_updates(True)
        .connection_pool_size(100)
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
    
    # Initialize everything asynchronously
    async def initialize_app():
        # Initialize database
        await init_database()
        # Initialize data manager
        await data_manager.initialize()
        
        # Get bot info
        try:
            bot_info = await application.bot.get_me()
            bot_username = bot_info.username
        except Exception as e:
            logger.warning(f"Could not fetch bot username: {e}")
            bot_username = "unknown"
        
        logger.info("🤖 Bot is starting...")
        print("=" * 50)
        print(f"✅ Bot started successfully!")
        print(f"🤖 Bot username: @{bot_username}")
        print(f"👑 Admin IDs: {ADMIN_IDS}")
        print(f"📢 Channels configured: {len(data_manager.channels)}")
        print(f"👥 Users loaded: {len(data_manager.users)}")
        print(f"🔗 Referrals: {len(data_manager.referrals)}")
        print(f"💾 Storage: {'✅ MongoDB' if mongo_client else '📁 Local files'}")
        print("=" * 50)
        print("✅ Bot is now ready to handle multiple users concurrently!")
        
        return application
    
    # Run the bot with polling
    try:
        # Start the async initialization and polling
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        # Initialize the app
        app = loop.run_until_complete(initialize_app())
        
        # Start polling
        print("🔄 Starting bot polling...")
        app.run_polling(
            allowed_updates=Update.ALL_TYPES,
            drop_pending_updates=True,
            close_loop=False
        )
        
    except KeyboardInterrupt:
        logger.info("Bot stopped by user")
    except Exception as e:
        logger.error(f"Bot stopped with error: {e}")
        print(f"❌ Bot stopped: {e}")

if __name__ == '__main__':
    main()