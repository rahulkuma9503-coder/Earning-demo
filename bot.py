import os
import logging
import asyncio
from datetime import datetime
from typing import List, Dict
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
INITIAL_CHANNELS = os.getenv('INITIAL_CHANNELS', '').split(',') if os.getenv('INITIAL_CHANNELS') else []

# Global variables for database
mongo_client = None
channels_collection = None
users_collection = None
referrals_collection = None

# Thread pool for blocking operations
executor = ThreadPoolExecutor(max_workers=10)

def init_database():
    """Initialize MongoDB connection"""
    global mongo_client, channels_collection, users_collection, referrals_collection
    
    if not MONGODB_URI:
        logger.warning("⚠️ MONGODB_URI not set. Using file-based storage.")
        return False
    
    try:
        mongo_client = MongoClient(
            MONGODB_URI, 
            serverSelectionTimeoutMS=5000,
            maxPoolSize=50,  # Increased connection pool
            connectTimeoutMS=30000,
            socketTimeoutMS=30000
        )
        # Test connection
        mongo_client.server_info()
        
        db = mongo_client.get_database('telegram_referral_bot')
        
        # Initialize collections
        channels_collection = db['channels']
        users_collection = db['users']
        referrals_collection = db['referrals']
        
        # Create indexes
        users_collection.create_index('user_id', unique=True)
        channels_collection.create_index('chat_id', unique=True)
        referrals_collection.create_index([('referrer_id', 1), ('referred_id', 1)], unique=True)
        
        logger.info("✅ MongoDB connected successfully")
        return True
        
    except errors.ServerSelectionTimeoutError:
        logger.error("❌ MongoDB connection timeout. Using file-based storage.")
        return False
    except errors.ConnectionFailure:
        logger.error("❌ MongoDB connection failed. Using file-based storage.")
        return False
    except Exception as e:
        logger.error(f"❌ MongoDB error: {e}. Using file-based storage.")
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

class DataManager:
    """Manage all data with storage persistence"""
    
    def __init__(self):
        self.channels = []
        self.users = {}
        self.referrals = {}
        self._lock = asyncio.Lock()
        asyncio.create_task(self.async_init())
    
    async def async_init(self):
        """Async initialization"""
        await self.load_all_data()
        self.init_channels_from_env()
        
        # Backup data on exit
        atexit.register(lambda: asyncio.run(self.backup_all_data()))
    
    async def load_all_data(self):
        """Load all data from storage asynchronously"""
        logger.info("📂 Loading data from storage...")
        async with self._lock:
            self.channels = await Storage.load_channels()
            self.users = await Storage.load_users()
            self.referrals = await Storage.load_referrals()
        logger.info(f"✅ Loaded {len(self.channels)} channels, {len(self.users)} users, {len(self.referrals)} referrals")
    
    def init_channels_from_env(self):
        """Initialize channels from environment variable"""
        if INITIAL_CHANNELS:
            logger.info(f"Initializing channels from environment variable: {INITIAL_CHANNELS}")
            for chat_id in INITIAL_CHANNELS:
                if chat_id.strip():
                    self.add_channel_from_env(chat_id.strip())
    
    def add_channel_from_env(self, chat_id: str):
        """Add channel from environment variable"""
        try:
            clean_id = chat_id.strip()
            
            # Format chat_id
            if clean_id.startswith('@'):
                chat_id_str = clean_id
            else:
                if clean_id.startswith('-'):
                    chat_id_str = clean_id
                elif clean_id.isdigit() or (clean_id.startswith('100') and len(clean_id) > 9):
                    chat_id_str = f"-{clean_id.lstrip('-')}"
                else:
                    logger.error(f"Invalid channel ID format: {chat_id}")
                    return
            
            # Check duplicate
            for channel in self.channels:
                if str(channel.get('chat_id')) == str(chat_id_str):
                    logger.info(f"Channel {chat_id_str} already exists")
                    return
            
            # Add channel
            channel = {
                'chat_id': chat_id_str,
                'name': f"Channel {len(self.channels) + 1}",
                'added_at': datetime.now().isoformat()
            }
            self.channels.append(channel)
            logger.info(f"✅ Added channel: {chat_id_str}")
        except Exception as e:
            logger.error(f"Error adding channel from env: {e}")
    
    async def backup_all_data(self):
        """Backup all data to storage asynchronously"""
        logger.info("💾 Backing up data to storage...")
        async with self._lock:
            await Storage.save_channels(self.channels)
            await Storage.save_users(self.users)
            await Storage.save_referrals(self.referrals)
        logger.info(f"✅ Data backed up: {len(self.channels)} channels, {len(self.users)} users, {len(self.referrals)} referrals")
    
    def get_stats(self) -> str:
        """Get data statistics"""
        total_balance = sum(u.get('balance', 0) for u in self.users.values())
        return (
            f"📊 **Database Statistics:**\n\n"
            f"📢 Channels: {len(self.channels)}\n"
            f"👥 Users: {len(self.users)}\n"
            f"🔗 Referrals: {len(self.referrals)}\n"
            f"💰 Total Balance: ₹{total_balance:.2f}\n"
            f"💾 Storage: {'✅ MongoDB' if db_connected else '📁 Local files'}"
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
        
        async with data_manager._lock:
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
                'has_joined_channels': False
            }
            
            data_manager.users[user_str] = user_data
            await Storage.save_users(data_manager.users)
            return user_data
    
    @staticmethod
    async def update_user(user_id: int, updates: Dict):
        """Update user data asynchronously"""
        user_str = str(user_id)
        async with data_manager._lock:
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
    def get_referrer(user_id: int) -> int:
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
        async with data_manager._lock:
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
            f'Referral bonus for user {referred_id}'
        )
        
        logger.info(f"New referral: {referrer_id} → {referred_id}")
        return True

async def check_channel_membership(bot, user_id: int) -> tuple:
    """Check channel membership concurrently"""
    channels = ChannelManager.get_channels()
    
    if not channels:
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
        logger.error(f"Error checking {chat_id}: {e}")
        return False

async def get_invite_link(bot, chat_id):
    """Get or create invite link for a chat with timeout"""
    try:
        if isinstance(chat_id, str) and chat_id.lstrip('-').isdigit():
            chat_id_int = int(chat_id)
        else:
            chat_id_int = chat_id
        
        # Add timeout for get_chat
        try:
            chat = await asyncio.wait_for(
                bot.get_chat(chat_id_int),
                timeout=10.0
            )
        except asyncio.TimeoutError:
            logger.warning(f"Timeout getting chat {chat_id}")
            return None
        
        # Try to get existing invite link
        try:
            invite_link = await asyncio.wait_for(
                chat.export_invite_link(),
                timeout=10.0
            )
            return invite_link
        except:
            # Try to create one
            try:
                invite_link = await asyncio.wait_for(
                    bot.create_chat_invite_link(
                        chat_id=chat_id_int,
                        creates_join_request=False
                    ),
                    timeout=10.0
                )
                return invite_link.invite_link
            except Exception as e:
                logger.error(f"Failed to create invite link: {e}")
                if hasattr(chat, 'username') and chat.username:
                    return f"https://t.me/{chat.username}"
                else:
                    return f"https://t.me/c/{str(chat_id_int).replace('-100', '')}"
    except Exception as e:
        logger.error(f"Error getting invite link for {chat_id}: {e}")
        if isinstance(chat_id, str) and chat_id.startswith('@'):
            return f"https://t.me/{chat_id.lstrip('@')}"
        return None

async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /start command with timeout"""
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
                async with data_manager._lock:
                    for user_id_str, user_data_item in data_manager.users.items():
                        if user_data_item.get('referral_code') == referral_code:
                            referrer_found = int(user_id_str)
                            break
                
                if referrer_found and referrer_found != user.id:
                    is_new_referral = await UserManager.add_referral(referrer_found, user.id)
                    
                    if is_new_referral:
                        # Notify referrer (non-blocking)
                        asyncio.create_task(notify_referrer(context.bot, referrer_found, user))
                        
                        await update.message.reply_text(
                            f"✅ **Referral Accepted!**\n\n"
                            f"You were referred by user {referrer_found}.\n"
                            f"They earned ₹1.00 for your join!"
                        )
                    else:
                        await update.message.reply_text(
                            "⚠️ This referral link has already been used."
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
                await UserManager.update_user(user.id, {'has_joined_channels': True})
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

async def notify_referrer(bot, referrer_id: int, referred_user):
    """Notify referrer about new referral (non-blocking)"""
    try:
        await bot.send_message(
            chat_id=referrer_id,
            text=f"🎉 **New Referral!**\n\n"
                 f"You have successfully referred a new user:\n"
                 f"• Name: {referred_user.first_name}\n"
                 f"• User ID: {referred_user.id}\n"
                 f"• Bonus: ₹1.00\n\n"
                 f"💰 Your new balance: ₹{(await UserManager.get_user(referrer_id))['balance']:.2f}"
        )
    except Exception as e:
        logger.error(f"Failed to notify referrer: {e}")

async def show_join_buttons(update: Update, context: ContextTypes.DEFAULT_TYPE, not_joined: List[Dict]):
    """Show join buttons for channels"""
    try:
        user = update.effective_user
        
        keyboard = []
        link_tasks = []
        
        # Get all invite links concurrently
        for channel in not_joined:
            chat_id = channel['chat_id']
            channel_name = channel.get('name', 'Join Channel')
            
            task = asyncio.create_task(get_invite_link(context.bot, chat_id))
            link_tasks.append((task, channel_name))
        
        # Process results as they become available
        for task, channel_name in link_tasks:
            try:
                invite_link = await asyncio.wait_for(task, timeout=5.0)
                if invite_link:
                    keyboard.append([
                        InlineKeyboardButton(f"📢 {channel_name}", url=invite_link)
                    ])
                else:
                    keyboard.append([
                        InlineKeyboardButton(
                            f"🔒 {channel_name} (Contact Admin)", 
                            callback_data="no_invite_link"
                        )
                    ])
            except (asyncio.TimeoutError, Exception) as e:
                logger.warning(f"Timeout getting invite link for {channel_name}: {e}")
                keyboard.append([
                    InlineKeyboardButton(
                        f"⏳ {channel_name} (Loading...)", 
                        callback_data="no_invite_link"
                    )
                ])
        
        keyboard.append([
            InlineKeyboardButton("✅ Verify Join", callback_data="verify_join")
        ])
        
        message_text = (
            f"👋 Welcome {user.first_name}!\n\n"
            f"To use this bot, you need to join {len(not_joined)} channel(s).\n"
            f"After joining all channels, click 'Verify Join' below."
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
            
    except Exception as e:
        logger.error(f"Error in show_join_buttons: {e}")
        try:
            await update.message.reply_text("Please try joining the required channels and click /start again.")
        except:
            pass

async def no_invite_link_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle no invite link callback"""
    query = update.callback_query
    await query.answer("Please contact the admin to add you manually.", show_alert=True)

async def verify_join_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle verify join button callback"""
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
                await query.edit_message_text(
                    "✅ **Verified!** You've joined all required channels.\n\n"
                    "Now you can access all bot features."
                )
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
    """Show main menu"""
    try:
        user = update.effective_user
        user_data = await UserManager.get_user(user.id)
        
        message = (
            f"👤 **Account Overview**\n\n"
            f"🆔 **User ID:** `{user.id}`\n"
            f"👤 **Name:** {user.first_name}\n"
            f"💰 **Balance:** ₹{user_data.get('balance', 0):.2f}\n"
            f"👥 **Referrals:** {user_data.get('referral_count', 0)}\n"
            f"💵 **Total Earned:** ₹{user_data.get('total_earned', 0):.2f}\n"
            f"📤 **Total Withdrawn:** ₹{user_data.get('total_withdrawn', 0):.2f}"
        )
        
        keyboard = [
            [InlineKeyboardButton("💰 Balance", callback_data="balance"),
             InlineKeyboardButton("📤 Withdraw", callback_data="withdraw")],
            [InlineKeyboardButton("📜 History", callback_data="history"),
             InlineKeyboardButton("👥 Referrals", callback_data="referrals")],
            [InlineKeyboardButton("🔗 Invite Link", callback_data="invite_link"),
             InlineKeyboardButton("🔄 Refresh", callback_data="refresh")]
        ]
        
        if user.id in ADMIN_IDS:
            keyboard.append([InlineKeyboardButton("👑 Admin", callback_data="admin_panel")])
        
        if update.callback_query:
            try:
                await update.callback_query.message.reply_text(
                    message,
                    reply_markup=InlineKeyboardMarkup(keyboard),
                    parse_mode=ParseMode.MARKDOWN
                )
            except:
                await update.callback_query.edit_message_text(
                    message,
                    reply_markup=InlineKeyboardMarkup(keyboard),
                    parse_mode=ParseMode.MARKDOWN
                )
        else:
            await update.message.reply_text(
                message,
                reply_markup=InlineKeyboardMarkup(keyboard),
                parse_mode=ParseMode.MARKDOWN
            )
            
    except Exception as e:
        logger.error(f"Error in show_main_menu: {e}")
        try:
            await update.message.reply_text("Error showing menu. Please try /start again.")
        except:
            pass

async def show_main_menu_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show main menu from callback"""
    try:
        query = update.callback_query
        await query.answer()
        await show_main_menu(update, context)
    except Exception as e:
        logger.error(f"Error in show_main_menu_callback: {e}")

# Rest of the callback handlers remain similar but with async operations...
# [Previous callback handlers like balance_callback, withdraw_callback, etc. should also be updated to use async methods]

async def balance_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show balance details"""
    try:
        query = update.callback_query
        await query.answer()
        
        user = update.effective_user
        user_data = await UserManager.get_user(user.id)
        
        message = (
            f"💰 **Balance Details**\n\n"
            f"💳 **Available:** ₹{user_data.get('balance', 0):.2f}\n"
            f"📈 **Total Earned:** ₹{user_data.get('total_earned', 0):.2f}\n"
            f"📤 **Total Withdrawn:** ₹{user_data.get('total_withdrawn', 0):.2f}\n\n"
            f"👥 **Referral Earnings:** ₹{user_data.get('referral_count', 0):.0f}\n\n"
            f"💎 **Earn more:** Share your invite link!"
        )
        
        keyboard = [[InlineKeyboardButton("🔙 Back", callback_data="back_to_main")]]
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
        user_data = await UserManager.get_user(user.id)
        
        if not context.args or len(context.args) < 2:
            await update.message.reply_text(
                "❌ **Usage:** `/withdraw <amount> <method>`\n\n"
                "**Example:** `/withdraw 50 UPI`\n"
                "**Methods:** UPI, Paytm, PhonePe, Bank",
                parse_mode=ParseMode.MARKDOWN
            )
            return
        
        try:
            amount = float(context.args[0])
            method = context.args[1].upper()
            
            if amount < 10:
                await update.message.reply_text("❌ Minimum withdrawal is ₹10")
                return
            
            if amount > user_data.get('balance', 0):
                await update.message.reply_text("❌ Insufficient balance")
                return
            
            new_balance = user_data.get('balance', 0) - amount
            await UserManager.update_user(user.id, {
                'balance': new_balance,
                'total_withdrawn': user_data.get('total_withdrawn', 0) + amount
            })
            
            await UserManager.add_transaction(
                user.id,
                -amount,
                'withdrawal',
                f'Withdrawal via {method}'
            )
            
            # Notify admin (non-blocking)
            asyncio.create_task(notify_admins_withdrawal(context.bot, user, amount, method))
            
            await update.message.reply_text(
                f"✅ **Withdrawal Requested!**\n\n"
                f"💵 **Amount:** ₹{amount:.2f}\n"
                f"📱 **Method:** {method}\n"
                f"⏳ **Status:** Pending\n"
                f"📅 **Processed within:** 24 hours\n\n"
                f"💰 **New Balance:** ₹{new_balance:.2f}",
                parse_mode=ParseMode.MARKDOWN
            )
            
        except ValueError:
            await update.message.reply_text("❌ Invalid amount")
            
    except Exception as e:
        logger.error(f"Error in withdraw_command: {e}")
        try:
            await update.message.reply_text("Error processing withdrawal. Please try again.")
        except:
            pass

async def notify_admins_withdrawal(bot, user, amount: float, method: str):
    """Notify all admins about withdrawal (non-blocking)"""
    for admin_id in ADMIN_IDS:
        try:
            await bot.send_message(
                chat_id=admin_id,
                text=f"💰 **New Withdrawal**\n\n"
                     f"👤 User: {user.first_name}\n"
                     f"🆔 ID: {user.id}\n"
                     f"💵 Amount: ₹{amount:.2f}\n"
                     f"📱 Method: {method}"
            )
        except Exception as e:
            logger.error(f"Failed to notify admin {admin_id}: {e}")

async def broadcast_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Broadcast message - admin only"""
    try:
        user = update.effective_user
        
        if user.id not in ADMIN_IDS:
            await update.message.reply_text("❌ Admin only")
            return
        
        if not context.args:
            await update.message.reply_text("❌ Usage: `/broadcast <message>`")
            return
        
        message = " ".join(context.args)
        sent_count = 0
        
        await update.message.reply_text(f"📢 Broadcasting to {len(data_manager.users)} users...")
        
        # Broadcast in batches to avoid rate limiting
        user_ids = list(data_manager.users.keys())
        batch_size = 30  # Messages per second (Telegram limit is 30 msg/sec)
        
        for i in range(0, len(user_ids), batch_size):
            batch = user_ids[i:i+batch_size]
            tasks = []
            
            for user_id_str in batch:
                task = asyncio.create_task(
                    send_broadcast_message(context.bot, int(user_id_str), message)
                )
                tasks.append(task)
            
            # Wait for batch to complete
            results = await asyncio.gather(*tasks, return_exceptions=True)
            sent_count += sum(1 for r in results if r is True)
            
            # Rate limiting: wait 1 second between batches
            if i + batch_size < len(user_ids):
                await asyncio.sleep(1)
        
        await update.message.reply_text(f"✅ Sent to {sent_count}/{len(data_manager.users)} users")
    except Exception as e:
        logger.error(f"Error in broadcast_command: {e}")

async def send_broadcast_message(bot, user_id: int, message: str) -> bool:
    """Send broadcast message to a single user"""
    try:
        await bot.send_message(
            chat_id=user_id,
            text=f"📢 **Announcement:**\n\n{message}"
        )
        return True
    except Exception as e:
        logger.debug(f"Failed to send to user {user_id}: {e}")
        return False

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
    print(f"📢 Channels: {len(data_manager.channels)}")
    print(f"👥 Users: {len(data_manager.users)}")
    print(f"🔗 Referrals: {len(data_manager.referrals)}")
    print(f"🌐 HTTP Server: http://0.0.0.0:{PORT}")
    print(f"💾 Storage: {'✅ MongoDB' if db_connected else '📁 Local files'}")
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