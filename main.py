import os
import re
import time
import logging
import aiosqlite
from datetime import datetime, timedelta, timezone
from typing import Optional

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup
from telegram.ext import (
    Application, CommandHandler, MessageHandler, CallbackQueryHandler,
    ConversationHandler, TypeHandler, ContextTypes, ApplicationHandlerStop,
    filters
)
from telegram.error import BadRequest

BOT_TOKEN = os.environ.get("BOT_TOKEN", "8439858305:AAGmLWjRCXvFFoM5QRaqOna4hSkooq-xtao")
try:
    ADMIN_IDS = [int(x.strip()) for x in os.environ.get("ADMIN_IDS", "8373846582").split(",") if x.strip()]
except:
    ADMIN_IDS = [8373846582]
DB_NAME = "bot_database.db"

# Settings keys
SETTING_GMAIL_PRICE = "gmail_price"
SETTING_REF_BONUS = "referral_bonus"
SETTING_MIN_WITHDRAW = "min_withdraw"
SETTING_MAX_WITHDRAW = "max_withdraw"
SETTING_DAILY_LIMIT = "daily_limit"
SETTING_START_MSG = "start_message"
SETTING_HELP_MSG = "help_message"
SETTING_BOT_NAME = "bot_name"
SETTING_CURRENCY_SYMBOL = "currency_symbol"
SETTING_MAINTENANCE_MODE = "maintenance_mode"
SETTING_ANTI_SPAM = "anti_spam"
SETTING_BONUS_AMOUNT = "bonus_amount"
SETTING_BONUS_COOLDOWN = "bonus_cooldown"
SETTING_RANK_ENABLED = "rank_enabled"
SETTING_FORCE_JOIN = "force_join"

DEFAULT_SETTINGS = {
    SETTING_GMAIL_PRICE: "5",
    SETTING_REF_BONUS: "2",
    SETTING_MIN_WITHDRAW: "200",
    SETTING_MAX_WITHDRAW: "0",
    SETTING_DAILY_LIMIT: "20",
    SETTING_START_MSG: "Welcome to Gmail Submit Bot!",
    SETTING_HELP_MSG: (
        "❓ Help Center\n\n"
        "📧 Send Gmail to earn balance\n"
        "💰 Withdraw when you reach minimum\n"
        "👥 Refer friends and earn bonus\n"
        "📊 Check your rank and leaderboard\n\n"
        "Admin commands:\n"
        "/admin - Open admin panel\n"
        "/set key value - Change any setting\n"
        "/user <id> - Manage user\n\n"
        "Support: @admin"
    ),
    SETTING_BOT_NAME: "Gmail Submit Bot",
    SETTING_CURRENCY_SYMBOL: "৳",
    SETTING_MAINTENANCE_MODE: "0",
    SETTING_ANTI_SPAM: "1",
    SETTING_BONUS_AMOUNT: "10",
    SETTING_BONUS_COOLDOWN: "24",
    SETTING_RANK_ENABLED: "1",
    SETTING_FORCE_JOIN: "0",
}

# Conversation states
GMAIL_INPUT = 1
GMAIL_PASSWORD = 2
WITHDRAW_AMOUNT = 3
WITHDRAW_METHOD_SELECT = 4   # new state for selecting method
WITHDRAW_ACCOUNT_DETAILS = 5 # new state for entering account details
PAYMENT_METHOD_INPUT = 6
ADMIN_TASK_DESC = 7
ADMIN_TASK_REQ = 8
ADMIN_TASK_REWARD = 9
ADMIN_BROADCAST_MSG = 10
ADMIN_CHANNEL_ID = 11
ADMIN_CHANNEL_URL = 12
ADMIN_EDIT_TASK_SELECT = 13
ADMIN_EDIT_TASK_DESC = 14
ADMIN_EDIT_TASK_REQ = 15
ADMIN_EDIT_TASK_REWARD = 16
ADMIN_EDIT_TASK_ACTIVE = 17
ADMIN_EDIT_SETTING = 18
ADMIN_ADD_PAYMENT_METHOD = 19   # for adding payment method name

user_last_message = {}
COOLDOWN_SECONDS = 1.0

logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)

# -------------------- Database Setup --------------------
async def setup_database():
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute('PRAGMA journal_mode=WAL;')
        # Create tables
        await db.execute('''
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                name TEXT,
                balance REAL DEFAULT 0,
                referrals INTEGER DEFAULT 0,
                total_gmail INTEGER DEFAULT 0,
                total_earn REAL DEFAULT 0,
                referrer_id INTEGER DEFAULT NULL,
                payment_method TEXT DEFAULT NULL,
                notification_on INTEGER DEFAULT 1,
                language TEXT DEFAULT 'en',
                is_banned INTEGER DEFAULT 0,
                last_bonus TIMESTAMP DEFAULT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        await db.execute('''
            CREATE TABLE IF NOT EXISTS gmail_accounts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                email TEXT UNIQUE,
                password TEXT,
                status TEXT DEFAULT 'Pending',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY(user_id) REFERENCES users(user_id)
            )
        ''')
        await db.execute('''
            CREATE TABLE IF NOT EXISTS withdraw_requests (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                amount REAL,
                method TEXT,                -- selected method (e.g., "Bkash")
                account_details TEXT,       -- user's account number/details
                status TEXT DEFAULT 'Pending',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY(user_id) REFERENCES users(user_id)
            )
        ''')
        await db.execute('''
            CREATE TABLE IF NOT EXISTS tasks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                description TEXT,
                required_gmails INTEGER,
                reward REAL,
                is_active INTEGER DEFAULT 1,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        await db.execute('''
            CREATE TABLE IF NOT EXISTS bonus (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                amount REAL,
                claimed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY(user_id) REFERENCES users(user_id)
            )
        ''')
        await db.execute('''
            CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY,
                value TEXT
            )
        ''')
        await db.execute('''
            CREATE TABLE IF NOT EXISTS channels (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                channel_id TEXT,
                channel_url TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        await db.execute('''
            CREATE TABLE IF NOT EXISTS history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                action TEXT,
                details TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY(user_id) REFERENCES users(user_id)
            )
        ''')
        await db.execute('''
            CREATE TABLE IF NOT EXISTS daily_tasks (
                user_id INTEGER,
                date TEXT,
                submitted_today INTEGER DEFAULT 0,
                claimed INTEGER DEFAULT 0,
                PRIMARY KEY (user_id, date),
                FOREIGN KEY(user_id) REFERENCES users(user_id)
            )
        ''')
        # New table for payment methods
        await db.execute('''
            CREATE TABLE IF NOT EXISTS payment_methods (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT UNIQUE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        # Schema migration: add missing columns
        cursor = await db.execute("PRAGMA table_info(gmail_accounts)")
        columns = [row[1] for row in await cursor.fetchall()]
        if 'password' not in columns:
            await db.execute("ALTER TABLE gmail_accounts ADD COLUMN password TEXT")
            logger.info("Added missing 'password' column to gmail_accounts")

        cursor = await db.execute("PRAGMA table_info(users)")
        columns = [row[1] for row in await cursor.fetchall()]
        required_user_cols = ['payment_method', 'notification_on', 'language', 'is_banned', 'last_bonus']
        for col in required_user_cols:
            if col not in columns:
                await db.execute(f"ALTER TABLE users ADD COLUMN {col} TEXT DEFAULT NULL")
                logger.info(f"Added missing column '{col}' to users")

        cursor = await db.execute("PRAGMA table_info(daily_tasks)")
        columns = [row[1] for row in await cursor.fetchall()]
        if 'claimed' not in columns:
            await db.execute("ALTER TABLE daily_tasks ADD COLUMN claimed INTEGER DEFAULT 0")
            logger.info("Added missing 'claimed' column to daily_tasks")

        # Add account_details column to withdraw_requests if missing
        cursor = await db.execute("PRAGMA table_info(withdraw_requests)")
        columns = [row[1] for row in await cursor.fetchall()]
        if 'account_details' not in columns:
            await db.execute("ALTER TABLE withdraw_requests ADD COLUMN account_details TEXT")
            logger.info("Added missing 'account_details' column to withdraw_requests")

        # Insert default settings
        for key, value in DEFAULT_SETTINGS.items():
            await db.execute('INSERT OR IGNORE INTO settings (key, value) VALUES (?, ?)', (key, value))

        # Insert default task
        await db.execute('''
            INSERT OR IGNORE INTO tasks (id, description, required_gmails, reward, is_active)
            VALUES (1, 'Submit 5 Gmail', 5, 20, 1)
        ''')

        # Insert default payment methods if none exist
        async with db.execute("SELECT COUNT(*) FROM payment_methods") as cursor:
            count = (await cursor.fetchone())[0]
            if count == 0:
                for method in ["Bkash", "Nagad", "Rocket"]:
                    await db.execute("INSERT OR IGNORE INTO payment_methods (name) VALUES (?)", (method,))
                logger.info("Inserted default payment methods")

        await db.commit()
    logger.info("Database setup completed.")

# -------------------- Database Helpers --------------------
async def get_user(user_id):
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute('PRAGMA journal_mode=WAL;')
        db.row_factory = aiosqlite.Row
        async with db.execute('SELECT * FROM users WHERE user_id = ?', (user_id,)) as cursor:
            return await cursor.fetchone()

async def create_user(user_id, name, referrer_id=None):
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute('PRAGMA journal_mode=WAL;')
        await db.execute('INSERT OR IGNORE INTO users (user_id, name, referrer_id) VALUES (?, ?, ?)', (user_id, name, referrer_id))
        await db.commit()

async def update_user_balance(user_id, amount, is_earn=True):
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute('PRAGMA journal_mode=WAL;')
        if is_earn:
            await db.execute('UPDATE users SET balance = balance + ?, total_earn = total_earn + ? WHERE user_id = ?', (amount, amount, user_id))
        else:
            await db.execute('UPDATE users SET balance = balance + ? WHERE user_id = ?', (amount, user_id))
        await db.commit()

async def get_user_rank(user_id):
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute('PRAGMA journal_mode=WAL;')
        async with db.execute('SELECT COUNT(*) + 1 as rank FROM users WHERE total_earn > (SELECT total_earn FROM users WHERE user_id = ?)', (user_id,)) as cursor:
            row = await cursor.fetchone()
            return row[0] if row else 1

async def get_next_rank_info(user_id):
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute('PRAGMA journal_mode=WAL;')
        async with db.execute('SELECT total_earn FROM users WHERE user_id = ?', (user_id,)) as cursor:
            user_row = await cursor.fetchone()
            if not user_row:
                return None, None
            current = user_row[0]
        async with db.execute('SELECT total_earn FROM users WHERE total_earn > ? ORDER BY total_earn ASC LIMIT 1', (current,)) as cursor:
            next_row = await cursor.fetchone()
            if not next_row:
                return None, None
            next_amount = next_row[0]
            async with db.execute('SELECT COUNT(*) + 1 FROM users WHERE total_earn > ?', (next_amount,)) as cursor:
                rank_row = await cursor.fetchone()
                next_rank = rank_row[0]
            return next_rank, next_amount - current

async def add_gmail(user_id, email, password):
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute('PRAGMA journal_mode=WAL;')
        await db.execute('INSERT INTO gmail_accounts (user_id, email, password) VALUES (?, ?, ?)', (user_id, email, password))
        await db.commit()

async def get_user_gmails(user_id):
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute('PRAGMA journal_mode=WAL;')
        db.row_factory = aiosqlite.Row
        async with db.execute('SELECT * FROM gmail_accounts WHERE user_id = ? ORDER BY id DESC LIMIT 50', (user_id,)) as cursor:
            return await cursor.fetchall()

async def check_gmail_exists(email):
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute('PRAGMA journal_mode=WAL;')
        async with db.execute('SELECT 1 FROM gmail_accounts WHERE email = ?', (email,)) as cursor:
            return await cursor.fetchone() is not None

async def get_pending_gmails():
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute('PRAGMA journal_mode=WAL;')
        db.row_factory = aiosqlite.Row
        async with db.execute('SELECT * FROM gmail_accounts WHERE status = "Pending" ORDER BY id') as cursor:
            return await cursor.fetchall()

async def get_gmail_by_id(gmail_id):
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute('PRAGMA journal_mode=WAL;')
        db.row_factory = aiosqlite.Row
        async with db.execute('SELECT * FROM gmail_accounts WHERE id = ?', (gmail_id,)) as cursor:
            return await cursor.fetchone()

async def update_gmail_status(gmail_id, status):
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute('PRAGMA journal_mode=WAL;')
        await db.execute('UPDATE gmail_accounts SET status = ? WHERE id = ?', (status, gmail_id))
        await db.commit()

async def delete_gmail(gmail_id):
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute('PRAGMA journal_mode=WAL;')
        await db.execute('DELETE FROM gmail_accounts WHERE id = ?', (gmail_id,))
        await db.commit()

async def get_pending_withdraws():
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute('PRAGMA journal_mode=WAL;')
        db.row_factory = aiosqlite.Row
        async with db.execute('SELECT * FROM withdraw_requests WHERE status = "Pending" ORDER BY id') as cursor:
            return await cursor.fetchall()

async def get_withdraw_by_id(wid):
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute('PRAGMA journal_mode=WAL;')
        db.row_factory = aiosqlite.Row
        async with db.execute('SELECT * FROM withdraw_requests WHERE id = ?', (wid,)) as cursor:
            return await cursor.fetchone()

async def update_withdraw_status(wid, status):
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute('PRAGMA journal_mode=WAL;')
        await db.execute('UPDATE withdraw_requests SET status = ? WHERE id = ?', (status, wid))
        await db.commit()

async def get_setting(key, default=None):
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute('PRAGMA journal_mode=WAL;')
        async with db.execute('SELECT value FROM settings WHERE key = ?', (key,)) as cursor:
            row = await cursor.fetchone()
            return row[0] if row else default

async def set_setting(key, value):
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute('PRAGMA journal_mode=WAL;')
        await db.execute('INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)', (key, value))
        await db.commit()

async def add_history(user_id, action, details=""):
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute('PRAGMA journal_mode=WAL;')
        await db.execute('INSERT INTO history (user_id, action, details) VALUES (?, ?, ?)', (user_id, action, details))
        await db.commit()

async def get_stats():
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute('PRAGMA journal_mode=WAL;')
        db.row_factory = aiosqlite.Row
        async with db.execute('SELECT COUNT(*) as total_users FROM users') as c:
            total_users = (await c.fetchone())['total_users']
        async with db.execute('SELECT COUNT(*) as total_gmails FROM gmail_accounts') as c:
            total_gmails = (await c.fetchone())['total_gmails']
        async with db.execute("SELECT SUM(amount) as total_withdraw FROM withdraw_requests WHERE status='Approved'") as c:
            total_withdraw = (await c.fetchone())['total_withdraw'] or 0
        async with db.execute('SELECT SUM(total_earn) as total_earn FROM users') as c:
            total_earn = (await c.fetchone())['total_earn'] or 0
        today = datetime.now(timezone.utc).date().isoformat()
        async with db.execute('SELECT COUNT(*) as today_users FROM users WHERE DATE(created_at) = ?', (today,)) as c:
            today_users = (await c.fetchone())['today_users']
        async with db.execute('SELECT COUNT(*) as today_gmails FROM gmail_accounts WHERE DATE(created_at) = ?', (today,)) as c:
            today_gmails = (await c.fetchone())['today_gmails']
        return {"total_users": total_users, "today_users": today_users, "total_gmails": total_gmails, "today_gmails": today_gmails, "total_withdraw": total_withdraw, "total_earn": total_earn}

async def get_daily_task_status(user_id):
    today = datetime.now(timezone.utc).date().isoformat()
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute('PRAGMA journal_mode=WAL;')
        db.row_factory = aiosqlite.Row
        async with db.execute('SELECT submitted_today, claimed FROM daily_tasks WHERE user_id = ? AND date = ?', (user_id, today)) as cursor:
            row = await cursor.fetchone()
            return dict(row) if row else {"submitted_today": 0, "claimed": 0}

async def increment_daily_submission(user_id):
    today = datetime.now(timezone.utc).date().isoformat()
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute('PRAGMA journal_mode=WAL;')
        await db.execute('INSERT INTO daily_tasks (user_id, date, submitted_today, claimed) VALUES (?, ?, 1, 0) ON CONFLICT(user_id, date) DO UPDATE SET submitted_today = submitted_today + 1', (user_id, today))
        await db.commit()

async def claim_daily_reward(user_id):
    today = datetime.now(timezone.utc).date().isoformat()
    tasks = await get_all_tasks()
    active_task = next((t for t in tasks if t['is_active']), None)
    if not active_task:
        return False
    required = active_task['required_gmails']
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute('PRAGMA journal_mode=WAL;')
        async with db.execute('SELECT submitted_today, claimed FROM daily_tasks WHERE user_id = ? AND date = ?', (user_id, today)) as cursor:
            row = await cursor.fetchone()
            if not row or row[1] == 1 or row[0] < required:
                return False
        await db.execute('UPDATE daily_tasks SET claimed = 1 WHERE user_id = ? AND date = ?', (user_id, today))
        await db.commit()
    return True

async def get_all_tasks():
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute('PRAGMA journal_mode=WAL;')
        db.row_factory = aiosqlite.Row
        async with db.execute('SELECT * FROM tasks ORDER BY id') as cursor:
            return await cursor.fetchall()

async def get_task_by_id(task_id):
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute('PRAGMA journal_mode=WAL;')
        db.row_factory = aiosqlite.Row
        async with db.execute('SELECT * FROM tasks WHERE id = ?', (task_id,)) as cursor:
            return await cursor.fetchone()

async def add_task(description, required, reward):
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute('PRAGMA journal_mode=WAL;')
        await db.execute('INSERT INTO tasks (description, required_gmails, reward) VALUES (?, ?, ?)', (description, required, reward))
        await db.commit()

async def update_task(task_id, description, required, reward, is_active):
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute('PRAGMA journal_mode=WAL;')
        await db.execute('UPDATE tasks SET description=?, required_gmails=?, reward=?, is_active=? WHERE id=?', (description, required, reward, is_active, task_id))
        await db.commit()

async def delete_task(task_id):
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute('PRAGMA journal_mode=WAL;')
        await db.execute('DELETE FROM tasks WHERE id=?', (task_id,))
        await db.commit()

async def get_channels():
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute('PRAGMA journal_mode=WAL;')
        db.row_factory = aiosqlite.Row
        async with db.execute('SELECT * FROM channels') as cursor:
            return await cursor.fetchall()

async def add_channel(channel_id, channel_url):
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute('PRAGMA journal_mode=WAL;')
        await db.execute('INSERT INTO channels (channel_id, channel_url) VALUES (?, ?)', (channel_id, channel_url))
        await db.commit()

async def remove_channel(channel_id):
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute('PRAGMA journal_mode=WAL;')
        await db.execute('DELETE FROM channels WHERE channel_id = ?', (channel_id,))
        await db.commit()

async def get_all_users():
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute('PRAGMA journal_mode=WAL;')
        db.row_factory = aiosqlite.Row
        async with db.execute('SELECT user_id, name, balance, is_banned, notification_on FROM users ORDER BY user_id') as cursor:
            return await cursor.fetchall()

async def ban_user(user_id):
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute('PRAGMA journal_mode=WAL;')
        await db.execute('UPDATE users SET is_banned = 1 WHERE user_id = ?', (user_id,))
        await db.commit()

async def unban_user(user_id):
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute('PRAGMA journal_mode=WAL;')
        await db.execute('UPDATE users SET is_banned = 0 WHERE user_id = ?', (user_id,))
        await db.commit()

async def delete_user(user_id):
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute('PRAGMA journal_mode=WAL;')
        await db.execute('DELETE FROM users WHERE user_id = ?', (user_id,))
        await db.commit()

async def get_daily_rank():
    today = datetime.now(timezone.utc).date().isoformat()
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute('PRAGMA journal_mode=WAL;')
        db.row_factory = aiosqlite.Row
        async with db.execute('''
            SELECT user_id, COUNT(*) as count FROM history 
            WHERE action = 'Gmail Submitted' AND DATE(created_at) = ? 
            GROUP BY user_id ORDER BY count DESC LIMIT 10
        ''', (today,)) as cursor:
            return await cursor.fetchall()

async def get_weekly_rank():
    week_ago = (datetime.now(timezone.utc) - timedelta(days=7)).isoformat()
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute('PRAGMA journal_mode=WAL;')
        db.row_factory = aiosqlite.Row
        async with db.execute('''
            SELECT user_id, COUNT(*) as count FROM history 
            WHERE action = 'Gmail Submitted' AND created_at >= ? 
            GROUP BY user_id ORDER BY count DESC LIMIT 10
        ''', (week_ago,)) as cursor:
            return await cursor.fetchall()

# ---------- Payment Methods Helpers ----------
async def get_payment_methods():
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute('PRAGMA journal_mode=WAL;')
        db.row_factory = aiosqlite.Row
        async with db.execute('SELECT * FROM payment_methods ORDER BY name') as cursor:
            return await cursor.fetchall()

async def add_payment_method(name):
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute('PRAGMA journal_mode=WAL;')
        try:
            await db.execute('INSERT INTO payment_methods (name) VALUES (?)', (name,))
            await db.commit()
            return True
        except aiosqlite.IntegrityError:
            return False

async def remove_payment_method(name):
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute('PRAGMA journal_mode=WAL;')
        await db.execute('DELETE FROM payment_methods WHERE name = ?', (name,))
        await db.commit()

# -------------------- Force Join Helper --------------------
async def check_force_join_status(user_id, context):
    force_join = await get_setting(SETTING_FORCE_JOIN, "0")
    if force_join != "1":
        return True, []
    channels = await get_channels()
    if not channels:
        return True, []
    not_joined = []
    for ch in channels:
        try:
            member = await context.bot.get_chat_member(chat_id=ch['channel_id'], user_id=user_id)
            if member.status in ['left', 'kicked']:
                not_joined.append(ch)
        except Exception as e:
            logger.error(f"Error checking channel member: {e}")
            not_joined.append(ch)
    return len(not_joined) == 0, not_joined

async def force_join_block(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    user_id = update.effective_user.id
    passed, not_joined = await check_force_join_status(user_id, context)
    if passed:
        return False
    msg = "🚫 <b>Please join our channels/groups first to use the bot.</b>\n\n"
    for ch in not_joined:
        msg += f"🔗 {ch['channel_url']}\n"
    msg += "\nAfter joining, click the button below to verify."
    keyboard = InlineKeyboardMarkup([[InlineKeyboardButton("✅ I've Joined", callback_data="force_join_verify")]])
    await update.message.reply_text(msg, parse_mode='HTML', reply_markup=keyboard)
    return True

# -------------------- Keyboards --------------------
def get_main_keyboard():
    return ReplyKeyboardMarkup([["➕ Submit Gmail", "📂 My Gmail"], ["👤 Profile", "📊 My Rank"], ["💳 Withdraw", "⚙ Settings"], ["❓ Help", "🎯 Daily Task", "🎁 Bonus"]], resize_keyboard=True)

def get_profile_keyboard():
    return ReplyKeyboardMarkup([["🏦 My Bank Rank", "📜 Activity History"], ["🔙 Back to Main Menu"]], resize_keyboard=True)

def get_rank_keyboard():
    return ReplyKeyboardMarkup([["👤 My Position"], ["📅 Daily Rank", "📆 Weekly Rank"], ["🔙 Back to Main Menu"]], resize_keyboard=True)

def get_withdraw_keyboard():
    return ReplyKeyboardMarkup([["📤 Request Withdraw", "📱 Payment Method"], ["📜 Withdraw History", "🔙 Back to Main Menu"]], resize_keyboard=True)

def get_settings_keyboard():
    return ReplyKeyboardMarkup([["💳 Payment Method", "🔔 Notifications"], ["🌐 Language", "🔙 Back to Main Menu"]], resize_keyboard=True)

def get_admin_keyboard():
    return ReplyKeyboardMarkup([
        ["👥 All Users", "📧 Gmail Manager"],
        ["💰 Income Settings", "💳 Withdraw Manager"],
        ["🏆 Rank System", "🎯 Daily Task Manager"],
        ["🎁 Bonus System", "📢 Broadcast"],
        ["📊 Statistics", "🛡 Security"],
        ["⚙ Bot Settings", "💳 Payment Methods"],   # added Payment Methods button
        ["🔙 Back to Main Menu"]
    ], resize_keyboard=True)

def get_cancel_keyboard():
    return ReplyKeyboardMarkup([["❌ Cancel"]], resize_keyboard=True)

# -------------------- Middleware --------------------
async def anti_spam_middleware(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.effective_user or not update.message:
        return
    user_id = update.effective_user.id
    if user_id in ADMIN_IDS:
        return
    maintenance = await get_setting(SETTING_MAINTENANCE_MODE, "0")
    if maintenance == "1":
        await update.message.reply_text("⚠️ Bot is under maintenance. Please try again later.")
        raise ApplicationHandlerStop()
    user = await get_user(user_id)
    if user and user['is_banned']:
        await update.message.reply_text("🚫 You are banned from using this bot.")
        raise ApplicationHandlerStop()
    anti_spam_enabled = await get_setting(SETTING_ANTI_SPAM, "1")
    if anti_spam_enabled == "1":
        current_time = time.time()
        last_time = user_last_message.get(user_id, 0)
        if current_time - last_time < COOLDOWN_SECONDS:
            raise ApplicationHandlerStop()
        user_last_message[user_id] = current_time

# -------------------- Command Handlers --------------------
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if await force_join_block(update, context):
        return
    user = update.effective_user
    db_user = await get_user(user.id)
    if not db_user:
        referrer_id = None
        if context.args and context.args[0].isdigit():
            referrer_id = int(context.args[0])
            if referrer_id == user.id:
                referrer_id = None
        await create_user(user.id, user.full_name or user.first_name, referrer_id)
        await add_history(user.id, "Registered", f"Referred by: {referrer_id if referrer_id else 'None'}")
        if referrer_id:
            ref_bonus = float(await get_setting(SETTING_REF_BONUS, "2"))
            await update_user_balance(referrer_id, ref_bonus, is_earn=True)
            async with aiosqlite.connect(DB_NAME) as db:
                await db.execute('PRAGMA journal_mode=WAL;')
                await db.execute('UPDATE users SET referrals = referrals + 1 WHERE user_id = ?', (referrer_id,))
                await db.commit()
            await add_history(referrer_id, "Referral Bonus", f"User {user.id} joined")
        db_user = await get_user(user.id)
    symbol = await get_setting(SETTING_CURRENCY_SYMBOL, "৳")
    msg = f"╭── 🤖 {await get_setting(SETTING_BOT_NAME)} ──╮\n│ 💰 Earn by submitting Gmail\n│ 👤 User: {db_user['name']}\n│ 💳 Balance: {db_user['balance']:.2f}{symbol}\n╰─────────────────────────╯"
    await update.message.reply_text(msg, reply_markup=get_main_keyboard())

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    help_msg = await get_setting(SETTING_HELP_MSG, DEFAULT_SETTINGS[SETTING_HELP_MSG])
    await update.message.reply_text(help_msg, reply_markup=get_main_keyboard())

async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Action canceled.", reply_markup=get_main_keyboard())
    return ConversationHandler.END

# -------------------- Force Join Callback --------------------
async def force_join_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if query.data != "force_join_verify":
        return
    user_id = query.from_user.id
    passed, not_joined = await check_force_join_status(user_id, context)
    if passed:
        user = await get_user(user_id)
        if not user:
            await create_user(user_id, query.from_user.full_name or query.from_user.first_name)
            user = await get_user(user_id)
        symbol = await get_setting(SETTING_CURRENCY_SYMBOL, "৳")
        msg = f"╭── 🤖 {await get_setting(SETTING_BOT_NAME)} ──╮\n│ 💰 Earn by submitting Gmail\n│ 👤 User: {user['name']}\n│ 💳 Balance: {user['balance']:.2f}{symbol}\n╰─────────────────────────╯"
        try:
            await query.edit_message_text(msg, parse_mode='HTML')
        except BadRequest as e:
            if "Message is not modified" in str(e):
                pass
            else:
                raise
        await context.bot.send_message(chat_id=user_id, text="Main Menu:", reply_markup=get_main_keyboard())
    else:
        msg = "❌ You haven't joined all required channels yet.\n\n"
        for ch in not_joined:
            msg += f"🔗 {ch['channel_url']}\n"
        msg += "\nAfter joining, click the button again."
        keyboard = InlineKeyboardMarkup([[InlineKeyboardButton("✅ I've Joined", callback_data="force_join_verify")]])
        try:
            await query.edit_message_text(msg, reply_markup=keyboard)
        except BadRequest as e:
            if "Message is not modified" in str(e):
                pass
            else:
                raise

# -------------------- Gmail Submission --------------------
async def submit_gmail_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if await force_join_block(update, context):
        return ConversationHandler.END
    await update.message.reply_text("Please send the Gmail address you want to submit:", reply_markup=get_cancel_keyboard())
    return GMAIL_INPUT

async def submit_gmail_email(update: Update, context: ContextTypes.DEFAULT_TYPE):
    email = update.message.text.strip().lower()
    if not re.match(r"[^@]+@gmail\.com$", email):
        await update.message.reply_text("❌ Invalid Gmail format. Please send a valid @gmail.com address.", reply_markup=get_cancel_keyboard())
        return GMAIL_INPUT
    if await check_gmail_exists(email):
        await update.message.reply_text("❌ This Gmail address has already been submitted.", reply_markup=get_main_keyboard())
        return ConversationHandler.END
    context.user_data['gmail_email'] = email
    await update.message.reply_text("Now send the password for this Gmail account:", reply_markup=get_cancel_keyboard())
    return GMAIL_PASSWORD

async def submit_gmail_password(update: Update, context: ContextTypes.DEFAULT_TYPE):
    password = update.message.text.strip()
    email = context.user_data.get('gmail_email')
    if not email:
        await update.message.reply_text("Error: email not found. Please start over.", reply_markup=get_main_keyboard())
        return ConversationHandler.END
    user_id = update.effective_user.id
    await add_gmail(user_id, email, password)
    await increment_daily_submission(user_id)
    await add_history(user_id, "Gmail Submitted", f"Email: {email}")
    msg = f"✅ <b>Gmail Submitted</b>\n\n📧 {email}\n🔑 Password: {'*' * len(password)}\n⏳ Status: Pending Approval"
    await update.message.reply_text(msg, parse_mode='HTML', reply_markup=get_main_keyboard())
    return ConversationHandler.END

# -------------------- Other User Handlers --------------------
async def my_gmail(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    gmails = await get_user_gmails(user_id)
    if not gmails:
        await update.message.reply_text("You haven't submitted any Gmail accounts yet.")
        return
    msg = "📂 <b>Your Gmail</b>\n\n"
    for i, g in enumerate(gmails[:20], 1):
        status_emoji = "✅" if g['status'] == "Approved" else "⏳" if g['status'] == "Pending" else "❌"
        msg += f"{i}️⃣ {g['email']}\nStatus: {status_emoji} {g['status']}\n\n"
    await update.message.reply_text(msg, parse_mode='HTML')

async def profile(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    user = await get_user(user_id)
    if not user:
        return
    symbol = await get_setting(SETTING_CURRENCY_SYMBOL, "৳")
    rank = await get_user_rank(user_id)
    msg = f"╭── 👤 User Profile ──╮\n│ 🆔 ID: <code>{user['user_id']}</code>\n│ 👤 Name: {user['name']}\n│ 📧 Gmail: {user['total_gmail']}\n│ 👥 Referrals: {user['referrals']}\n│ 💰 Balance: {user['balance']:.2f}{symbol}\n│ 🏆 Rank: #{rank}\n│ 📊 Total Earn: {user['total_earn']:.2f}{symbol}\n╰─────────────────────╯"
    await update.message.reply_text(msg, parse_mode='HTML', reply_markup=get_profile_keyboard())

async def my_bank_rank(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    user = await get_user(user_id)
    symbol = await get_setting(SETTING_CURRENCY_SYMBOL, "৳")
    rank = await get_user_rank(user_id)
    next_rank, need = await get_next_rank_info(user_id)
    if next_rank is None:
        next_text = "You are at the top!"
    else:
        next_text = f"📈 Next Rank: #{next_rank}\nNeed: {need:.2f}{symbol} more"
    msg = f"🏦 <b>Rank Details</b>\n\n👤 Name: {user['name']}\n🏆 Position: #{rank}\n💰 Earn: {user['total_earn']:.2f}{symbol}\n\n{next_text}"
    await update.message.reply_text(msg, parse_mode='HTML')

async def my_rank(update: Update, context: ContextTypes.DEFAULT_TYPE):
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute('PRAGMA journal_mode=WAL;')
        db.row_factory = aiosqlite.Row
        async with db.execute('SELECT name, total_earn FROM users ORDER BY total_earn DESC LIMIT 10') as cursor:
            top = await cursor.fetchall()
    symbol = await get_setting(SETTING_CURRENCY_SYMBOL, "৳")
    msg = "📊 <b>Leaderboard (Top Earners)</b>\n\n"
    if not top:
        msg += "No data yet."
    else:
        medals = ["🥇", "🥈", "🥉"]
        for i, row in enumerate(top, 1):
            prefix = medals[i-1] if i <= 3 else f"{i}."
            msg += f"{prefix} {row['name']} — {row['total_earn']:.2f}{symbol}\n"
    await update.message.reply_text(msg, parse_mode='HTML', reply_markup=get_rank_keyboard())

async def activity_history(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    msg = "📜 <b>Activity History</b>\n\n"
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute('PRAGMA journal_mode=WAL;')
        db.row_factory = aiosqlite.Row
        async with db.execute('SELECT * FROM history WHERE user_id = ? ORDER BY id DESC LIMIT 10', (user_id,)) as cursor:
            rows = await cursor.fetchall()
            if not rows:
                msg += "No activity yet."
            else:
                for row in rows:
                    msg += f"• {row['action']} - {row['details']}\n"
    await update.message.reply_text(msg, parse_mode='HTML')

async def daily_task_user(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    status = await get_daily_task_status(user_id)
    submitted = status["submitted_today"]
    claimed = status["claimed"]
    tasks = await get_all_tasks()
    active_task = next((t for t in tasks if t['is_active']), None)
    if not active_task:
        await update.message.reply_text("No daily task available.")
        return
    required = active_task['required_gmails']
    reward = active_task['reward']
    msg = f"📅 <b>Daily Task</b>\n\n{active_task['description']}\nReward: {reward}{await get_setting(SETTING_CURRENCY_SYMBOL, '৳')}\n\nYour progress: {submitted}/{required}"
    if submitted >= required and not claimed:
        keyboard = InlineKeyboardMarkup([[InlineKeyboardButton("🎁 Claim Reward", callback_data="claim_daily")]])
        await update.message.reply_text(msg, parse_mode='HTML', reply_markup=keyboard)
    else:
        if claimed:
            msg += "\n\n✅ You have already claimed today's reward."
        await update.message.reply_text(msg, parse_mode='HTML')

async def claim_daily_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user_id = query.from_user.id
    success = await claim_daily_reward(user_id)
    if success:
        tasks = await get_all_tasks()
        active_task = next((t for t in tasks if t['is_active']), None)
        reward = active_task['reward'] if active_task else 20
        await update_user_balance(user_id, reward, is_earn=True)
        await add_history(user_id, "Daily Task Completed", f"Reward: {reward}")
        await query.edit_message_text(f"✅ Daily task reward claimed! {reward}{await get_setting(SETTING_CURRENCY_SYMBOL, '৳')} added to your balance.")
    else:
        await query.edit_message_text("❌ Cannot claim reward. Either you haven't submitted enough Gmail today or already claimed.")

async def settings_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = f"⚙ <b>Settings</b>\n\n💳 Payment Method\n🔔 Notifications\n🌐 Language"
    await update.message.reply_text(msg, parse_mode='HTML', reply_markup=get_settings_keyboard())

async def payment_method_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Please send your payment method details (e.g., Bkash number):", reply_markup=get_cancel_keyboard())
    return PAYMENT_METHOD_INPUT

async def payment_method_save(update: Update, context: ContextTypes.DEFAULT_TYPE):
    method = update.message.text.strip()
    user_id = update.effective_user.id
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute('PRAGMA journal_mode=WAL;')
        await db.execute('UPDATE users SET payment_method = ? WHERE user_id = ?', (method, user_id))
        await db.commit()
    await add_history(user_id, "Payment Method Updated", method)
    await update.message.reply_text("✅ Payment method saved.", reply_markup=get_settings_keyboard())
    return ConversationHandler.END

async def notifications(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    user = await get_user(user_id)
    new_state = 0 if user['notification_on'] else 1
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute('PRAGMA journal_mode=WAL;')
        await db.execute('UPDATE users SET notification_on = ? WHERE user_id = ?', (new_state, user_id))
        await db.commit()
    status = "ON" if new_state else "OFF"
    await update.message.reply_text(f"🔔 Notifications turned {status}.", reply_markup=get_settings_keyboard())

async def language(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Language selection is under development.", reply_markup=get_settings_keyboard())

async def back_to_main(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data.clear()
    await update.message.reply_text("Returning to Main Menu...", reply_markup=get_main_keyboard())

async def claim_bonus(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    now = datetime.now(timezone.utc)
    bonus_amount = float(await get_setting(SETTING_BONUS_AMOUNT, "10"))
    cooldown_hours = int(await get_setting(SETTING_BONUS_COOLDOWN, "24"))
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute('PRAGMA journal_mode=WAL;')
        async with db.execute('SELECT last_bonus FROM users WHERE user_id = ?', (user_id,)) as cursor:
            row = await cursor.fetchone()
            last_bonus = row[0] if row else None
        if last_bonus:
            last = datetime.fromisoformat(last_bonus)
            if now - last < timedelta(hours=cooldown_hours):
                remaining = timedelta(hours=cooldown_hours) - (now - last)
                hours, remainder = divmod(remaining.seconds, 3600)
                minutes = remainder // 60
                await update.message.reply_text(f"⏳ Come back in {hours}h {minutes}m.")
                return
    await update_user_balance(user_id, bonus_amount, is_earn=True)
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute('PRAGMA journal_mode=WAL;')
        await db.execute('UPDATE users SET last_bonus = ? WHERE user_id = ?', (now.isoformat(), user_id))
        await db.commit()
    await add_history(user_id, "Bonus Claimed", f"Amount: {bonus_amount}")
    symbol = await get_setting(SETTING_CURRENCY_SYMBOL, "৳")
    msg = f"🎁 <b>Claim Bonus</b>\n\nYou received {bonus_amount:.2f}{symbol} bonus"
    await update.message.reply_text(msg, parse_mode='HTML')

# -------------------- Withdraw (updated) --------------------
async def withdraw_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    user = await get_user(user_id)
    if not user:
        return
    min_w = float(await get_setting(SETTING_MIN_WITHDRAW, "200"))
    max_w = float(await get_setting(SETTING_MAX_WITHDRAW, "0"))
    symbol = await get_setting(SETTING_CURRENCY_SYMBOL, "৳")
    msg = f"╭── 💳 Withdraw ──╮\n│ 💰 Balance: {user['balance']:.2f}{symbol}\n│ 📉 Min Withdraw: {min_w:.2f}{symbol}\n"
    if max_w > 0:
        msg += f"│ 📈 Max Withdraw: {max_w:.2f}{symbol}\n"
    msg += "╰──────────────────╯"
    await update.message.reply_text(msg, reply_markup=get_withdraw_keyboard())

async def withdraw_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if await force_join_block(update, context):
        return ConversationHandler.END
    user_id = update.effective_user.id
    user = await get_user(user_id)
    if not user:
        return ConversationHandler.END
    min_w = float(await get_setting(SETTING_MIN_WITHDRAW, "200"))
    if user['balance'] < min_w:
        await update.message.reply_text(f"❌ Minimum withdraw amount is {min_w:.2f}. Your balance is {user['balance']:.2f}.", reply_markup=get_withdraw_keyboard())
        return ConversationHandler.END
    await update.message.reply_text("Enter amount to withdraw:", reply_markup=get_cancel_keyboard())
    return WITHDRAW_AMOUNT

async def withdraw_amount(update: Update, context: ContextTypes.DEFAULT_TYPE):
    amount_str = update.message.text.strip()
    try:
        amount = float(amount_str)
    except ValueError:
        await update.message.reply_text("Please enter a valid number.", reply_markup=get_cancel_keyboard())
        return WITHDRAW_AMOUNT
    user_id = update.effective_user.id
    user = await get_user(user_id)
    min_w = float(await get_setting(SETTING_MIN_WITHDRAW, "200"))
    max_w = float(await get_setting(SETTING_MAX_WITHDRAW, "0"))
    if amount < min_w:
        await update.message.reply_text(f"Minimum withdraw is {min_w:.2f}.", reply_markup=get_cancel_keyboard())
        return WITHDRAW_AMOUNT
    if max_w > 0 and amount > max_w:
        await update.message.reply_text(f"Maximum withdraw per request is {max_w:.2f}.", reply_markup=get_cancel_keyboard())
        return WITHDRAW_AMOUNT
    if amount > user['balance']:
        await update.message.reply_text("Insufficient balance.", reply_markup=get_cancel_keyboard())
        return WITHDRAW_AMOUNT
    context.user_data['withdraw_amount'] = amount

    # Fetch available payment methods
    methods = await get_payment_methods()
    if not methods:
        await update.message.reply_text("❌ No payment methods available. Please contact admin.", reply_markup=get_main_keyboard())
        return ConversationHandler.END

    # Show inline keyboard with methods
    keyboard = []
    for m in methods:
        keyboard.append([InlineKeyboardButton(m['name'], callback_data=f"wmethod_{m['name']}")])
    keyboard.append([InlineKeyboardButton("❌ Cancel", callback_data="wmethod_cancel")])
    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.message.reply_text("Please select your payment method:", reply_markup=reply_markup)
    return WITHDRAW_METHOD_SELECT

async def withdraw_method_select_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data
    if data == "wmethod_cancel":
        await query.edit_message_text("Withdraw cancelled.")
        context.user_data.clear()
        return ConversationHandler.END
    # Extract method name
    method = data.replace("wmethod_", "")
    context.user_data['withdraw_method'] = method
    await query.edit_message_text(f"Selected method: {method}\n\nPlease enter your {method} account number / details:")
    return WITHDRAW_ACCOUNT_DETAILS

async def withdraw_account_details(update: Update, context: ContextTypes.DEFAULT_TYPE):
    account_details = update.message.text.strip()
    amount = context.user_data.get('withdraw_amount')
    method = context.user_data.get('withdraw_method')
    user_id = update.effective_user.id
    if not amount or not method:
        await update.message.reply_text("Error: missing information. Please start over.", reply_markup=get_main_keyboard())
        return ConversationHandler.END

    # Deduct balance and create withdraw request
    await update_user_balance(user_id, -amount, is_earn=False)
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute('PRAGMA journal_mode=WAL;')
        await db.execute('INSERT INTO withdraw_requests (user_id, amount, method, account_details) VALUES (?, ?, ?, ?)',
                         (user_id, amount, method, account_details))
        await db.commit()
    await add_history(user_id, "Withdraw Requested", f"Amount: {amount:.2f}, Method: {method}, Account: {account_details}")

    await update.message.reply_text("✅ Withdraw request submitted successfully! You will be notified when processed.", reply_markup=get_withdraw_keyboard())
    # Notify admins (optional)
    for admin_id in ADMIN_IDS:
        try:
            await context.bot.send_message(admin_id, f"🔔 New withdraw request from user {user_id} ({update.effective_user.full_name})\nAmount: {amount}\nMethod: {method}\nAccount: {account_details}")
        except:
            pass
    context.user_data.clear()
    return ConversationHandler.END

async def withdraw_history(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    symbol = await get_setting(SETTING_CURRENCY_SYMBOL, "৳")
    msg = "📜 <b>Withdraw History</b>\n\n"
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute('PRAGMA journal_mode=WAL;')
        db.row_factory = aiosqlite.Row
        async with db.execute('SELECT * FROM withdraw_requests WHERE user_id = ? ORDER BY id DESC LIMIT 10', (user_id,)) as cursor:
            rows = await cursor.fetchall()
            if not rows:
                msg += "No withdraw history."
            else:
                for row in rows:
                    msg += f"• {row['amount']:.2f}{symbol} - {row['status']} ({row['method']}: {row['account_details']})\n"
    await update.message.reply_text(msg, parse_mode='HTML')

# -------------------- Rank Handlers --------------------
async def my_position(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    rank = await get_user_rank(user_id)
    await update.message.reply_text(f"👤 Your current position: #{rank}", parse_mode='HTML')

async def daily_rank(update: Update, context: ContextTypes.DEFAULT_TYPE):
    rank_data = await get_daily_rank()
    symbol = await get_setting(SETTING_CURRENCY_SYMBOL, "৳")
    msg = "📅 <b>Daily Rank (Today's Submissions)</b>\n\n"
    if not rank_data:
        msg += "No submissions today."
    else:
        medals = ["🥇", "🥈", "🥉"]
        for i, row in enumerate(rank_data, 1):
            user = await get_user(row['user_id'])
            name = user['name'] if user else "Unknown"
            prefix = medals[i-1] if i <= 3 else f"{i}."
            msg += f"{prefix} {name} — {row['count']} Gmail\n"
    await update.message.reply_text(msg, parse_mode='HTML')

async def weekly_rank(update: Update, context: ContextTypes.DEFAULT_TYPE):
    rank_data = await get_weekly_rank()
    symbol = await get_setting(SETTING_CURRENCY_SYMBOL, "৳")
    msg = "📆 <b>Weekly Rank (Last 7 Days)</b>\n\n"
    if not rank_data:
        msg += "No submissions this week."
    else:
        medals = ["🥇", "🥈", "🥉"]
        for i, row in enumerate(rank_data, 1):
            user = await get_user(row['user_id'])
            name = user['name'] if user else "Unknown"
            prefix = medals[i-1] if i <= 3 else f"{i}."
            msg += f"{prefix} {name} — {row['count']} Gmail\n"
    await update.message.reply_text(msg, parse_mode='HTML')

# -------------------- Admin Handlers --------------------
async def admin_panel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in ADMIN_IDS:
        return
    msg = "╭── ⚙ Admin Control ──╮\n│ 👥 All Users\n│ 📧 Gmail Manager\n│ 💰 Income Settings\n│ 💳 Withdraw Manager\n│ 🏆 Rank System\n│ 🎯 Daily Task Manager\n│ 🎁 Bonus System\n│ 📢 Broadcast\n│ 📊 Statistics\n│ 🛡 Security\n│ ⚙ Bot Settings\n│ 💳 Payment Methods\n╰─────────────────────╯"
    await update.message.reply_text(msg, parse_mode='HTML', reply_markup=get_admin_keyboard())

async def admin_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in ADMIN_IDS:
        return
    stats = await get_stats()
    symbol = await get_setting(SETTING_CURRENCY_SYMBOL, "৳")
    msg = f"📊 <b>Statistics</b>\n\nTotal Users: {stats['total_users']}\nToday Users: {stats['today_users']}\nTotal Gmail: {stats['total_gmails']}\nToday Gmail: {stats['today_gmails']}\nTotal Withdraw: {stats['total_withdraw']:.2f}{symbol}\nTotal Earnings: {stats['total_earn']:.2f}{symbol}"
    await update.message.reply_text(msg, parse_mode='HTML')

async def all_users(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in ADMIN_IDS:
        return
    users = await get_all_users()
    msg = "👥 <b>All Users</b>\n\n"
    for u in users[:20]:
        banned = "🚫" if u['is_banned'] else "✅"
        msg += f"{banned} {u['name']} (<code>{u['user_id']}</code>) - {u['balance']:.2f} balance\n"
    msg += f"\nTotal: {len(users)} users"
    await update.message.reply_text(msg, parse_mode='HTML')

async def admin_user(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in ADMIN_IDS:
        return
    if not context.args:
        await update.message.reply_text("Usage: /user <user_id>")
        return
    try:
        target_id = int(context.args[0])
    except:
        await update.message.reply_text("Invalid ID")
        return
    user = await get_user(target_id)
    if not user:
        await update.message.reply_text("User not found.")
        return
    keyboard = InlineKeyboardMarkup([[InlineKeyboardButton("Ban", callback_data=f"user_ban_{target_id}"), InlineKeyboardButton("Unban", callback_data=f"user_unban_{target_id}"), InlineKeyboardButton("Delete", callback_data=f"user_delete_{target_id}")]])
    msg = f"User: {user['name']} (<code>{user['user_id']}</code>)\nBalance: {user['balance']}\nBanned: {user['is_banned']}"
    await update.message.reply_text(msg, reply_markup=keyboard, parse_mode='HTML')

async def user_management(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data.split('_')
    action = data[1]
    target_user_id = int(data[2])
    if action == "ban":
        await ban_user(target_user_id)
        await query.edit_message_text(f"User {target_user_id} banned.")
    elif action == "unban":
        await unban_user(target_user_id)
        await query.edit_message_text(f"User {target_user_id} unbanned.")
    elif action == "delete":
        await delete_user(target_user_id)
        await query.edit_message_text(f"User {target_user_id} deleted.")
    await add_history(update.effective_user.id, "Admin Action", f"{action} user {target_user_id}")

# -------------------- Gmail Manager --------------------
async def gmail_manager(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in ADMIN_IDS:
        return
    pending = await get_pending_gmails()
    if not pending:
        await update.message.reply_text("✅ No pending Gmail accounts to review.")
        return
    g = pending[0]
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ Approve", callback_data=f"gmail_approve_{g['id']}"), InlineKeyboardButton("❌ Reject", callback_data=f"gmail_reject_{g['id']}"), InlineKeyboardButton("🗑 Delete", callback_data=f"gmail_delete_{g['id']}")],
        [InlineKeyboardButton("➡️ Next", callback_data="gmail_next")]
    ])
    context.user_data['pending_gmails'] = [g['id'] for g in pending]
    context.user_data['current_gmail_index'] = 0
    await update.message.reply_text(f"📧 <b>Pending Gmail Review</b>\n\nUser ID: <code>{g['user_id']}</code>\nEmail: <code>{g['email']}</code>\nPassword: <code>{g['password']}</code>\nDate: {g['created_at']}", parse_mode='HTML', reply_markup=keyboard)

async def gmail_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if query.from_user.id not in ADMIN_IDS:
        return
    data = query.data
    if data == "gmail_next":
        pending_ids = context.user_data.get('pending_gmails', [])
        current_index = context.user_data.get('current_gmail_index', 0) + 1
        if current_index >= len(pending_ids):
            await query.edit_message_text("No more pending Gmail.")
            return
        context.user_data['current_gmail_index'] = current_index
        gmail_id = pending_ids[current_index]
        g = await get_gmail_by_id(gmail_id)
        if not g:
            await query.edit_message_text("Error loading Gmail.")
            return
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("✅ Approve", callback_data=f"gmail_approve_{g['id']}"), InlineKeyboardButton("❌ Reject", callback_data=f"gmail_reject_{g['id']}"), InlineKeyboardButton("🗑 Delete", callback_data=f"gmail_delete_{g['id']}")],
            [InlineKeyboardButton("➡️ Next", callback_data="gmail_next")]
        ])
        await query.edit_message_text(f"📧 <b>Pending Gmail Review</b>\n\nUser ID: <code>{g['user_id']}</code>\nEmail: <code>{g['email']}</code>\nPassword: <code>{g['password']}</code>\nDate: {g['created_at']}", parse_mode='HTML', reply_markup=keyboard)
        return
    parts = data.split('_')
    action = parts[1]
    gmail_id = int(parts[2])
    g = await get_gmail_by_id(gmail_id)
    if not g or g['status'] != 'Pending':
        await query.edit_message_text("This Gmail has already been processed or doesn't exist.")
        return
    if action == "approve":
        price = float(await get_setting(SETTING_GMAIL_PRICE, "5"))
        async with aiosqlite.connect(DB_NAME) as db:
            await db.execute('PRAGMA journal_mode=WAL;')
            await db.execute("UPDATE gmail_accounts SET status = 'Approved' WHERE id = ?", (gmail_id,))
            await db.execute("UPDATE users SET balance = balance + ?, total_gmail = total_gmail + 1, total_earn = total_earn + ? WHERE user_id = ?", (price, price, g['user_id']))
            await db.commit()
        await add_history(g['user_id'], "Gmail Approved", f"Email: {g['email']}, Reward: {price}")
        user = await get_user(g['user_id'])
        if user and user['notification_on']:
            try:
                await context.bot.send_message(chat_id=g['user_id'], text=f"✅ Your submitted Gmail ({g['email']}) has been approved! You received {price:.2f}{await get_setting(SETTING_CURRENCY_SYMBOL, '৳')}.")
            except Exception as e:
                logger.error(f"Error sending message: {e}")
        await query.edit_message_text(f"✅ Gmail {g['email']} approved.")
    elif action == "reject":
        await update_gmail_status(gmail_id, 'Rejected')
        await add_history(g['user_id'], "Gmail Rejected", f"Email: {g['email']}")
        user = await get_user(g['user_id'])
        if user and user['notification_on']:
            try:
                await context.bot.send_message(chat_id=g['user_id'], text=f"❌ Your submitted Gmail ({g['email']}) has been rejected.")
            except Exception as e:
                logger.error(f"Error sending message: {e}")
        await query.edit_message_text(f"❌ Gmail {g['email']} rejected.")
    elif action == "delete":
        await delete_gmail(gmail_id)
        await add_history(g['user_id'], "Gmail Deleted by Admin", f"Email: {g['email']}")
        await query.edit_message_text(f"🗑 Gmail {g['email']} deleted.")

# -------------------- Withdraw Manager (updated) --------------------
async def withdraw_manager(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in ADMIN_IDS:
        return
    pending = await get_pending_withdraws()
    if not pending:
        await update.message.reply_text("✅ No pending withdraw requests.")
        return
    w = pending[0]
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ Approve", callback_data=f"withdraw_approve_{w['id']}"), InlineKeyboardButton("❌ Reject", callback_data=f"withdraw_reject_{w['id']}")],
        [InlineKeyboardButton("➡️ Next", callback_data="withdraw_next")]
    ])
    context.user_data['pending_withdraws'] = [w['id'] for w in pending]
    context.user_data['current_withdraw_index'] = 0
    await update.message.reply_text(
        f"💳 <b>Pending Withdraw Request</b>\n\n"
        f"User ID: <code>{w['user_id']}</code>\n"
        f"Amount: <code>{w['amount']:.2f}</code>\n"
        f"Method: <code>{w['method']}</code>\n"
        f"Account Details: <code>{w['account_details']}</code>\n"
        f"Date: {w['created_at']}",
        parse_mode='HTML', reply_markup=keyboard
    )

async def withdraw_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if query.from_user.id not in ADMIN_IDS:
        return
    data = query.data
    if data == "withdraw_next":
        pending_ids = context.user_data.get('pending_withdraws', [])
        current_index = context.user_data.get('current_withdraw_index', 0) + 1
        if current_index >= len(pending_ids):
            await query.edit_message_text("No more pending withdraw requests.")
            return
        context.user_data['current_withdraw_index'] = current_index
        wid = pending_ids[current_index]
        w = await get_withdraw_by_id(wid)
        if not w:
            await query.edit_message_text("Error loading request.")
            return
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("✅ Approve", callback_data=f"withdraw_approve_{w['id']}"), InlineKeyboardButton("❌ Reject", callback_data=f"withdraw_reject_{w['id']}")],
            [InlineKeyboardButton("➡️ Next", callback_data="withdraw_next")]
        ])
        await query.edit_message_text(
            f"💳 <b>Pending Withdraw Request</b>\n\n"
            f"User ID: <code>{w['user_id']}</code>\n"
            f"Amount: <code>{w['amount']:.2f}</code>\n"
            f"Method: <code>{w['method']}</code>\n"
            f"Account Details: <code>{w['account_details']}</code>\n"
            f"Date: {w['created_at']}",
            parse_mode='HTML', reply_markup=keyboard
        )
        return
    parts = data.split('_')
    action = parts[1]
    wid = int(parts[2])
    w = await get_withdraw_by_id(wid)
    if not w or w['status'] != 'Pending':
        await query.edit_message_text("This request has already been processed.")
        return
    if action == "approve":
        await update_withdraw_status(wid, 'Approved')
        await add_history(w['user_id'], "Withdraw Approved", f"Amount: {w['amount']:.2f}, Method: {w['method']}, Account: {w['account_details']}")
        user = await get_user(w['user_id'])
        if user and user['notification_on']:
            try:
                await context.bot.send_message(
                    chat_id=w['user_id'],
                    text=f"✅ Your withdraw request for {w['amount']:.2f} has been approved and processed via {w['method']} ({w['account_details']})!"
                )
            except Exception as e:
                logger.error(f"Error sending message: {e}")
        await query.edit_message_text(f"✅ Withdraw of {w['amount']:.2f} for User {w['user_id']} approved.")
    elif action == "reject":
        await update_withdraw_status(wid, 'Rejected')
        await update_user_balance(w['user_id'], w['amount'], is_earn=False)
        await add_history(w['user_id'], "Withdraw Rejected", f"Amount: {w['amount']:.2f} refunded, Method: {w['method']}")
        user = await get_user(w['user_id'])
        if user and user['notification_on']:
            try:
                await context.bot.send_message(
                    chat_id=w['user_id'],
                    text=f"❌ Your withdraw request for {w['amount']:.2f} via {w['method']} was rejected. The amount has been refunded to your balance."
                )
            except Exception as e:
                logger.error(f"Error sending message: {e}")
        await query.edit_message_text(f"❌ Withdraw of {w['amount']:.2f} for User {w['user_id']} rejected and refunded.")

# -------------------- Payment Methods Admin --------------------
async def payment_methods_admin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in ADMIN_IDS:
        return
    methods = await get_payment_methods()
    msg = "💳 <b>Payment Methods</b>\n\n"
    if methods:
        for m in methods:
            msg += f"• {m['name']}\n"
    else:
        msg += "No methods defined.\n"
    msg += "\nUse buttons below to manage."
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("➕ Add Method", callback_data="pm_add")],
        [InlineKeyboardButton("➖ Remove Method", callback_data="pm_remove")]
    ])
    await update.message.reply_text(msg, parse_mode='HTML', reply_markup=keyboard)

async def payment_methods_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if query.from_user.id not in ADMIN_IDS:
        return
    data = query.data
    if data == "pm_add":
        await query.edit_message_text("Send the name of the new payment method (e.g., Bkash):")
        return ADMIN_ADD_PAYMENT_METHOD
    elif data == "pm_remove":
        methods = await get_payment_methods()
        if not methods:
            await query.edit_message_text("No methods to remove.")
            return
        keyboard = []
        for m in methods:
            keyboard.append([InlineKeyboardButton(m['name'], callback_data=f"pm_remove_{m['name']}")])
        keyboard.append([InlineKeyboardButton("🔙 Back", callback_data="pm_back")])
        await query.edit_message_text("Select method to remove:", reply_markup=InlineKeyboardMarkup(keyboard))
    elif data.startswith("pm_remove_"):
        name = data.replace("pm_remove_", "")
        await remove_payment_method(name)
        await query.edit_message_text(f"✅ Method '{name}' removed.")
    elif data == "pm_back":
        await query.edit_message_text("Returning to Payment Methods menu.")
        # We can just let it end

async def add_payment_method_received(update: Update, context: ContextTypes.DEFAULT_TYPE):
    name = update.message.text.strip()
    success = await add_payment_method(name)
    if success:
        await update.message.reply_text(f"✅ Payment method '{name}' added.", reply_markup=get_admin_keyboard())
    else:
        await update.message.reply_text(f"❌ Method '{name}' already exists.", reply_markup=get_admin_keyboard())
    return ConversationHandler.END

# -------------------- Income Settings --------------------
async def income_settings(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in ADMIN_IDS:
        return
    keys = [SETTING_GMAIL_PRICE, SETTING_REF_BONUS, SETTING_MIN_WITHDRAW, SETTING_MAX_WITHDRAW, SETTING_DAILY_LIMIT, SETTING_CURRENCY_SYMBOL]
    msg = "💰 <b>Income Settings</b>\n\n"
    keyboard = []
    for key in keys:
        val = await get_setting(key, DEFAULT_SETTINGS[key])
        msg += f"<code>{key}</code> = <code>{val}</code>\n"
        keyboard.append([InlineKeyboardButton(f"Edit {key}", callback_data=f"edit_setting_{key}")])
    await update.message.reply_text(msg, parse_mode='HTML', reply_markup=InlineKeyboardMarkup(keyboard))

# -------------------- Bot Settings --------------------
async def bot_settings(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in ADMIN_IDS:
        return
    keys = [SETTING_BOT_NAME, SETTING_START_MSG, SETTING_HELP_MSG, SETTING_CURRENCY_SYMBOL]
    msg = "⚙ <b>Bot Settings</b>\n\n"
    keyboard = []
    for key in keys:
        val = await get_setting(key, DEFAULT_SETTINGS[key])
        msg += f"<code>{key}</code> = <code>{val}</code>\n"
        keyboard.append([InlineKeyboardButton(f"Edit {key}", callback_data=f"edit_setting_{key}")])
    await update.message.reply_text(msg, parse_mode='HTML', reply_markup=InlineKeyboardMarkup(keyboard))

# -------------------- Bonus Manager --------------------
async def bonus_manager(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in ADMIN_IDS:
        return
    keys = [SETTING_BONUS_AMOUNT, SETTING_BONUS_COOLDOWN]
    msg = "🎁 <b>Bonus System</b>\n\n"
    keyboard = []
    for key in keys:
        val = await get_setting(key, DEFAULT_SETTINGS[key])
        msg += f"<code>{key}</code> = <code>{val}</code>\n"
        keyboard.append([InlineKeyboardButton(f"Edit {key}", callback_data=f"edit_setting_{key}")])
    await update.message.reply_text(msg, parse_mode='HTML', reply_markup=InlineKeyboardMarkup(keyboard))

# -------------------- Edit Setting Conversation --------------------
async def edit_setting_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if query.from_user.id not in ADMIN_IDS:
        return
    key = query.data.replace("edit_setting_", "")
    context.user_data['editing_setting'] = key
    current = await get_setting(key, DEFAULT_SETTINGS.get(key, ""))
    await query.edit_message_text(f"Current value for <code>{key}</code> is: <code>{current}</code>\n\nSend the new value (or /cancel to abort):", parse_mode='HTML')
    return ADMIN_EDIT_SETTING

async def edit_setting_value(update: Update, context: ContextTypes.DEFAULT_TYPE):
    key = context.user_data.get('editing_setting')
    if not key:
        await update.message.reply_text("Error: No setting being edited. Please start over.")
        return ConversationHandler.END
    new_value = update.message.text.strip()
    await set_setting(key, new_value)
    await add_history(update.effective_user.id, "Setting Changed", f"{key} = {new_value}")
    await update.message.reply_text(f"✅ Setting <code>{key}</code> updated to <code>{new_value}</code>.", parse_mode='HTML', reply_markup=get_admin_keyboard())
    context.user_data.pop('editing_setting', None)
    return ConversationHandler.END

# -------------------- Rank System --------------------
async def rank_system(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in ADMIN_IDS:
        return
    enabled = await get_setting(SETTING_RANK_ENABLED, "1")
    status = "✅ Enabled" if enabled == "1" else "❌ Disabled"
    keyboard = InlineKeyboardMarkup([[InlineKeyboardButton("Toggle Enable/Disable", callback_data="rank_toggle")], [InlineKeyboardButton("Reset All Ranks", callback_data="rank_reset")]])
    await update.message.reply_text(f"🏆 <b>Rank System</b>\n\nStatus: {status}", parse_mode='HTML', reply_markup=keyboard)

async def rank_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if query.from_user.id not in ADMIN_IDS:
        return
    if query.data == "rank_toggle":
        current = await get_setting(SETTING_RANK_ENABLED, "1")
        new = "0" if current == "1" else "1"
        await set_setting(SETTING_RANK_ENABLED, new)
        await query.edit_message_text(f"Rank system {'enabled' if new=='1' else 'disabled'}.")
    elif query.data == "rank_reset":
        async with aiosqlite.connect(DB_NAME) as db:
            await db.execute('PRAGMA journal_mode=WAL;')
            await db.execute("UPDATE users SET total_earn = 0, balance = 0")
            await db.commit()
        await query.edit_message_text("All ranks have been reset.")

# -------------------- Daily Task Manager --------------------
async def daily_tasks(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in ADMIN_IDS:
        return
    tasks = await get_all_tasks()
    msg = "🎯 <b>Daily Task Manager</b>\n\n"
    for t in tasks:
        status = "✅ Active" if t['is_active'] else "❌ Inactive"
        msg += f"ID {t['id']}: {t['description']} - {t['required_gmails']} Gmail, Reward {t['reward']} ({status})\n"
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("➕ Add Task", callback_data="task_add")],
        [InlineKeyboardButton("✏️ Edit Task", callback_data="task_edit_select")],
        [InlineKeyboardButton("🗑 Delete Task", callback_data="task_delete_select")]
    ])
    await update.message.reply_text(msg, parse_mode='HTML', reply_markup=keyboard)

async def task_add_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    await query.edit_message_text("Send the task description:")
    return ADMIN_TASK_DESC

async def task_add_desc(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data['task_desc'] = update.message.text
    await update.message.reply_text("Send required Gmail count (number):")
    return ADMIN_TASK_REQ

async def task_add_req(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        req = int(update.message.text)
    except:
        await update.message.reply_text("Invalid number. Send required Gmail count:")
        return ADMIN_TASK_REQ
    context.user_data['task_req'] = req
    await update.message.reply_text("Send reward amount:")
    return ADMIN_TASK_REWARD

async def task_add_reward(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        reward = float(update.message.text)
    except:
        await update.message.reply_text("Invalid amount. Send reward:")
        return ADMIN_TASK_REWARD
    desc = context.user_data['task_desc']
    req = context.user_data['task_req']
    await add_task(desc, req, reward)
    await update.message.reply_text("✅ Task added.", reply_markup=get_admin_keyboard())
    return ConversationHandler.END

async def task_edit_select(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    tasks = await get_all_tasks()
    if not tasks:
        await query.edit_message_text("No tasks to edit.")
        return
    keyboard = []
    for t in tasks:
        keyboard.append([InlineKeyboardButton(f"{t['description']}", callback_data=f"task_edit_{t['id']}")])
    await query.edit_message_text("Select task to edit:", reply_markup=InlineKeyboardMarkup(keyboard))
    return ADMIN_EDIT_TASK_SELECT

async def task_edit_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    task_id = int(query.data.split('_')[2])
    task = await get_task_by_id(task_id)
    if not task:
        await query.edit_message_text("Task not found.")
        return ConversationHandler.END
    context.user_data['edit_task_id'] = task_id
    await query.edit_message_text(f"Editing task: {task['description']}\n\nSend new description (or /skip to keep current):")
    return ADMIN_EDIT_TASK_DESC

async def task_edit_desc(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text
    if text != "/skip":
        context.user_data['edit_desc'] = text
    await update.message.reply_text("Send new required Gmail count (or /skip):")
    return ADMIN_EDIT_TASK_REQ

async def task_edit_req(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text
    if text != "/skip":
        try:
            context.user_data['edit_req'] = int(text)
        except:
            await update.message.reply_text("Invalid number. Send required Gmail count (or /skip):")
            return ADMIN_EDIT_TASK_REQ
    await update.message.reply_text("Send new reward amount (or /skip):")
    return ADMIN_EDIT_TASK_REWARD

async def task_edit_reward(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text
    if text != "/skip":
        try:
            context.user_data['edit_reward'] = float(text)
        except:
            await update.message.reply_text("Invalid amount. Send reward (or /skip):")
            return ADMIN_EDIT_TASK_REWARD
    await update.message.reply_text("Send new active status (1 for active, 0 for inactive, or /skip):")
    return ADMIN_EDIT_TASK_ACTIVE

async def task_edit_active(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text
    task_id = context.user_data['edit_task_id']
    task = await get_task_by_id(task_id)
    if not task:
        await update.message.reply_text("Task not found.")
        return ConversationHandler.END
    new_desc = context.user_data.get('edit_desc', task['description'])
    new_req = context.user_data.get('edit_req', task['required_gmails'])
    new_reward = context.user_data.get('edit_reward', task['reward'])
    if text != "/skip":
        try:
            new_active = int(text)
        except:
            await update.message.reply_text("Invalid. Enter 1 or 0 (or /skip):")
            return ADMIN_EDIT_TASK_ACTIVE
    else:
        new_active = task['is_active']
    await update_task(task_id, new_desc, new_req, new_reward, new_active)
    await update.message.reply_text("✅ Task updated.", reply_markup=get_admin_keyboard())
    return ConversationHandler.END

async def task_delete_select(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    tasks = await get_all_tasks()
    if not tasks:
        await query.edit_message_text("No tasks to delete.")
        return
    keyboard = []
    for t in tasks:
        keyboard.append([InlineKeyboardButton(f"{t['description']}", callback_data=f"task_delete_{t['id']}")])
    await query.edit_message_text("Select task to delete:", reply_markup=InlineKeyboardMarkup(keyboard))

async def task_delete_confirm(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    task_id = int(query.data.split('_')[2])
    await delete_task(task_id)
    await query.edit_message_text("✅ Task deleted.")

# -------------------- Broadcast --------------------
async def broadcast_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in ADMIN_IDS:
        return
    await update.message.reply_text("📢 Send the message you want to broadcast to all users:", reply_markup=get_cancel_keyboard())
    return ADMIN_BROADCAST_MSG

async def broadcast_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text
    users = await get_all_users()
    success = 0
    fail = 0
    for u in users:
        try:
            await context.bot.send_message(chat_id=u['user_id'], text=text)
            success += 1
        except Exception as e:
            logger.error(f"Error sending message to {u['user_id']}: {e}")
            fail += 1
    await update.message.reply_text(f"Broadcast sent to {success} users, failed: {fail}.", reply_markup=get_admin_keyboard())
    return ConversationHandler.END

# -------------------- Security Settings --------------------
async def security_settings(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in ADMIN_IDS:
        return
    force_join = await get_setting(SETTING_FORCE_JOIN, "0")
    anti_spam = await get_setting(SETTING_ANTI_SPAM, "1")
    maintenance = await get_setting(SETTING_MAINTENANCE_MODE, "0")
    channels = await get_channels()
    msg = f"🛡 <b>Security Settings</b>\n\nForce Join: {'ON' if force_join=='1' else 'OFF'}\nAnti-Spam: {'ON' if anti_spam=='1' else 'OFF'}\nMaintenance Mode: {'ON' if maintenance=='1' else 'OFF'}\n\nChannels ({len(channels)}):\n"
    for ch in channels:
        msg += f"• {ch['channel_url']} (<code>{ch['channel_id']}</code>)\n"
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("Toggle Force Join", callback_data="sec_force")],
        [InlineKeyboardButton("Toggle Anti-Spam", callback_data="sec_antispam")],
        [InlineKeyboardButton("Toggle Maintenance", callback_data="sec_maintenance")],
        [InlineKeyboardButton("➕ Add Channel", callback_data="sec_addchannel")],
        [InlineKeyboardButton("➖ Remove Channel", callback_data="sec_removechannel")]
    ])
    await update.message.reply_text(msg, parse_mode='HTML', reply_markup=keyboard)

async def security_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if query.from_user.id not in ADMIN_IDS:
        return
    data = query.data
    if data == "sec_force":
        current = await get_setting(SETTING_FORCE_JOIN, "0")
        new = "0" if current == "1" else "1"
        await set_setting(SETTING_FORCE_JOIN, new)
        await query.edit_message_text(f"Force Join {'enabled' if new=='1' else 'disabled'}.")
    elif data == "sec_antispam":
        current = await get_setting(SETTING_ANTI_SPAM, "1")
        new = "0" if current == "1" else "1"
        await set_setting(SETTING_ANTI_SPAM, new)
        await query.edit_message_text(f"Anti-Spam {'enabled' if new=='1' else 'disabled'}.")
    elif data == "sec_maintenance":
        current = await get_setting(SETTING_MAINTENANCE_MODE, "0")
        new = "0" if current == "1" else "1"
        await set_setting(SETTING_MAINTENANCE_MODE, new)
        await query.edit_message_text(f"Maintenance Mode {'enabled' if new=='1' else 'disabled'}.")
    elif data == "sec_addchannel":
        await query.edit_message_text("Send the channel ID (e.g., @channel or -100...):")
        return ADMIN_CHANNEL_ID
    elif data == "sec_removechannel":
        channels = await get_channels()
        if not channels:
            await query.edit_message_text("No channels to remove.")
            return
        keyboard = []
        for ch in channels:
            keyboard.append([InlineKeyboardButton(ch['channel_url'], callback_data=f"remch_{ch['channel_id']}")])
        await query.edit_message_text("Select channel to remove:", reply_markup=InlineKeyboardMarkup(keyboard))

async def add_channel_id(update: Update, context: ContextTypes.DEFAULT_TYPE):
    channel_id = update.message.text.strip()
    context.user_data['channel_id'] = channel_id
    await update.message.reply_text("Send the channel invite link (URL):")
    return ADMIN_CHANNEL_URL

async def add_channel_url(update: Update, context: ContextTypes.DEFAULT_TYPE):
    url = update.message.text.strip()
    channel_id = context.user_data['channel_id']
    await add_channel(channel_id, url)
    await update.message.reply_text("✅ Channel added.", reply_markup=get_admin_keyboard())
    return ConversationHandler.END

async def remove_channel_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    channel_id = query.data.split('_')[1]
    await remove_channel(channel_id)
    await query.edit_message_text("Channel removed.")

# -------------------- Set Command (legacy) --------------------
async def set_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in ADMIN_IDS:
        return
    args = context.args
    if len(args) < 2:
        await update.message.reply_text("Usage: /set key value")
        return
    key = args[0]
    value = ' '.join(args[1:])
    await set_setting(key, value)
    await update.message.reply_text(f"Setting <code>{key}</code> updated to <code>{value}</code>.", parse_mode='HTML')

# -------------------- Post Init --------------------
async def post_init(application: Application):
    await setup_database()

# -------------------- Main --------------------
def main():
    if not BOT_TOKEN:
        logger.error("BOT_TOKEN is not set. Please provide it via environment variable.")
        return

    application = Application.builder().token(BOT_TOKEN).post_init(post_init).build()
    application.add_handler(TypeHandler(Update, anti_spam_middleware), group=-1)

    # Conversation Handlers
    gmail_conv = ConversationHandler(
        entry_points=[MessageHandler(filters.Regex('^➕ Submit Gmail$'), submit_gmail_start)],
        states={
            GMAIL_INPUT: [MessageHandler(filters.TEXT & ~filters.Regex('^❌ Cancel$'), submit_gmail_email)],
            GMAIL_PASSWORD: [MessageHandler(filters.TEXT & ~filters.Regex('^❌ Cancel$'), submit_gmail_password)]
        },
        fallbacks=[MessageHandler(filters.Regex('^❌ Cancel$'), cancel), MessageHandler(filters.Regex('^🔙 Back to Main Menu$'), back_to_main)]
    )

    # Updated withdraw conversation
    withdraw_conv = ConversationHandler(
        entry_points=[MessageHandler(filters.Regex('^📤 Request Withdraw$'), withdraw_start)],
        states={
            WITHDRAW_AMOUNT: [MessageHandler(filters.TEXT & ~filters.Regex('^❌ Cancel$'), withdraw_amount)],
            WITHDRAW_METHOD_SELECT: [CallbackQueryHandler(withdraw_method_select_callback, pattern='^wmethod_')],
            WITHDRAW_ACCOUNT_DETAILS: [MessageHandler(filters.TEXT & ~filters.Regex('^❌ Cancel$'), withdraw_account_details)]
        },
        fallbacks=[MessageHandler(filters.Regex('^❌ Cancel$'), cancel), MessageHandler(filters.Regex('^🔙 Back to Main Menu$'), back_to_main)]
    )

    payment_conv = ConversationHandler(
        entry_points=[MessageHandler(filters.Regex('^💳 Payment Method$'), payment_method_start)],
        states={PAYMENT_METHOD_INPUT: [MessageHandler(filters.TEXT & ~filters.Regex('^❌ Cancel$'), payment_method_save)]},
        fallbacks=[MessageHandler(filters.Regex('^❌ Cancel$'), cancel)]
    )

    task_add_conv = ConversationHandler(
        entry_points=[CallbackQueryHandler(task_add_start, pattern='^task_add$')],
        states={
            ADMIN_TASK_DESC: [MessageHandler(filters.TEXT & ~filters.Regex('^❌ Cancel$'), task_add_desc)],
            ADMIN_TASK_REQ: [MessageHandler(filters.TEXT & ~filters.Regex('^❌ Cancel$'), task_add_req)],
            ADMIN_TASK_REWARD: [MessageHandler(filters.TEXT & ~filters.Regex('^❌ Cancel$'), task_add_reward)]
        },
        fallbacks=[MessageHandler(filters.Regex('^❌ Cancel$'), cancel)]
    )

    task_edit_conv = ConversationHandler(
        entry_points=[CallbackQueryHandler(task_edit_start, pattern='^task_edit_\\d+$')],
        states={
            ADMIN_EDIT_TASK_DESC: [MessageHandler(filters.TEXT & ~filters.Regex('^/skip$'), task_edit_desc)],
            ADMIN_EDIT_TASK_REQ: [MessageHandler(filters.TEXT, task_edit_req)],
            ADMIN_EDIT_TASK_REWARD: [MessageHandler(filters.TEXT, task_edit_reward)],
            ADMIN_EDIT_TASK_ACTIVE: [MessageHandler(filters.TEXT, task_edit_active)]
        },
        fallbacks=[CommandHandler("skip", lambda u,c: ConversationHandler.END)]
    )

    broadcast_conv = ConversationHandler(
        entry_points=[MessageHandler(filters.Regex('^📢 Broadcast$'), broadcast_start)],
        states={ADMIN_BROADCAST_MSG: [MessageHandler(filters.TEXT & ~filters.Regex('^❌ Cancel$'), broadcast_message)]},
        fallbacks=[MessageHandler(filters.Regex('^❌ Cancel$'), cancel)]
    )

    add_channel_conv = ConversationHandler(
        entry_points=[CallbackQueryHandler(security_callback, pattern='^sec_addchannel$')],
        states={
            ADMIN_CHANNEL_ID: [MessageHandler(filters.TEXT & ~filters.Regex('^❌ Cancel$'), add_channel_id)],
            ADMIN_CHANNEL_URL: [MessageHandler(filters.TEXT & ~filters.Regex('^❌ Cancel$'), add_channel_url)]
        },
        fallbacks=[MessageHandler(filters.Regex('^❌ Cancel$'), cancel)]
    )

    edit_setting_conv = ConversationHandler(
        entry_points=[CallbackQueryHandler(edit_setting_callback, pattern='^edit_setting_')],
        states={
            ADMIN_EDIT_SETTING: [MessageHandler(filters.TEXT & ~filters.Regex('^/cancel$'), edit_setting_value)]
        },
        fallbacks=[CommandHandler("cancel", cancel)]
    )

    add_payment_method_conv = ConversationHandler(
        entry_points=[CallbackQueryHandler(payment_methods_callback, pattern='^pm_add$')],
        states={
            ADMIN_ADD_PAYMENT_METHOD: [MessageHandler(filters.TEXT & ~filters.Regex('^❌ Cancel$'), add_payment_method_received)]
        },
        fallbacks=[MessageHandler(filters.Regex('^❌ Cancel$'), cancel)]
    )

    application.add_handler(gmail_conv)
    application.add_handler(withdraw_conv)
    application.add_handler(payment_conv)
    application.add_handler(task_add_conv)
    application.add_handler(task_edit_conv)
    application.add_handler(broadcast_conv)
    application.add_handler(add_channel_conv)
    application.add_handler(edit_setting_conv)
    application.add_handler(add_payment_method_conv)

    # Command Handlers
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("help", help_command))
    application.add_handler(CommandHandler("admin", admin_panel))
    application.add_handler(CommandHandler("set", set_command))
    application.add_handler(CommandHandler("user", admin_user))

    # Message Handlers (User)
    application.add_handler(MessageHandler(filters.Regex('^📂 My Gmail$'), my_gmail))
    application.add_handler(MessageHandler(filters.Regex('^👤 Profile$'), profile))
    application.add_handler(MessageHandler(filters.Regex('^📊 My Rank$'), my_rank))
    application.add_handler(MessageHandler(filters.Regex('^💳 Withdraw$'), withdraw_menu))
    application.add_handler(MessageHandler(filters.Regex('^⚙ Settings$'), settings_menu))
    application.add_handler(MessageHandler(filters.Regex('^❓ Help$'), help_command))
    application.add_handler(MessageHandler(filters.Regex('^🎁 Bonus$'), claim_bonus))
    application.add_handler(MessageHandler(filters.Regex('^🎯 Daily Task$'), daily_task_user))
    application.add_handler(MessageHandler(filters.Regex('^🏦 My Bank Rank$'), my_bank_rank))
    application.add_handler(MessageHandler(filters.Regex('^📜 Activity History$'), activity_history))
    application.add_handler(MessageHandler(filters.Regex('^👤 My Position$'), my_position))
    application.add_handler(MessageHandler(filters.Regex('^📅 Daily Rank$'), daily_rank))
    application.add_handler(MessageHandler(filters.Regex('^📆 Weekly Rank$'), weekly_rank))
    application.add_handler(MessageHandler(filters.Regex('^📜 Withdraw History$'), withdraw_history))
    application.add_handler(MessageHandler(filters.Regex('^🔔 Notifications$'), notifications))
    application.add_handler(MessageHandler(filters.Regex('^🌐 Language$'), language))
    application.add_handler(MessageHandler(filters.Regex('^🔙 Back to Main Menu$'), back_to_main))

    # Message Handlers (Admin)
    application.add_handler(MessageHandler(filters.Regex('^📊 Statistics$'), admin_stats))
    application.add_handler(MessageHandler(filters.Regex('^👥 All Users$'), all_users))
    application.add_handler(MessageHandler(filters.Regex('^📧 Gmail Manager$'), gmail_manager))
    application.add_handler(MessageHandler(filters.Regex('^💰 Income Settings$'), income_settings))
    application.add_handler(MessageHandler(filters.Regex('^💳 Withdraw Manager$'), withdraw_manager))
    application.add_handler(MessageHandler(filters.Regex('^🏆 Rank System$'), rank_system))
    application.add_handler(MessageHandler(filters.Regex('^🎯 Daily Task Manager$'), daily_tasks))
    application.add_handler(MessageHandler(filters.Regex('^🎁 Bonus System$'), bonus_manager))
    application.add_handler(MessageHandler(filters.Regex('^🛡 Security$'), security_settings))
    application.add_handler(MessageHandler(filters.Regex('^⚙ Bot Settings$'), bot_settings))
    application.add_handler(MessageHandler(filters.Regex('^💳 Payment Methods$'), payment_methods_admin))

    # Callback Query Handlers
    application.add_handler(CallbackQueryHandler(gmail_callback, pattern='^gmail_'))
    application.add_handler(CallbackQueryHandler(withdraw_callback, pattern='^withdraw_'))
    application.add_handler(CallbackQueryHandler(claim_daily_callback, pattern='^claim_daily$'))
    application.add_handler(CallbackQueryHandler(rank_callback, pattern='^rank_'))
    application.add_handler(CallbackQueryHandler(user_management, pattern='^user_'))
    application.add_handler(CallbackQueryHandler(security_callback, pattern='^sec_'))
    application.add_handler(CallbackQueryHandler(remove_channel_callback, pattern='^remch_'))
    application.add_handler(CallbackQueryHandler(task_edit_select, pattern='^task_edit_select$'))
    application.add_handler(CallbackQueryHandler(task_delete_select, pattern='^task_delete_select$'))
    application.add_handler(CallbackQueryHandler(task_delete_confirm, pattern='^task_delete_\\d+$'))
    application.add_handler(CallbackQueryHandler(force_join_callback, pattern='^force_join_verify$'))
    application.add_handler(CallbackQueryHandler(payment_methods_callback, pattern='^pm_'))

    application.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == '__main__':
    main()