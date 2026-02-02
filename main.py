import logging
import sqlite3
import random
import re
import asyncio
import uuid
import os
import json
from typing import Dict, Tuple, List, Optional, Set
from datetime import datetime, timedelta
from urllib.parse import quote
import matplotlib.pyplot as plt
import matplotlib
matplotlib.use('Agg')
from io import BytesIO

# Сторонние библиотеки
import aiosqlite
import aiohttp
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery, BufferedInputFile
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.exceptions import TelegramBadRequest
from asyncio import CancelledError

# ==================== НАСТРОЙКИ ====================
BOT_TOKEN = "8515526360:AAF3aVQRjt_oYuJnSxrtr6lDXIS1ZCC_c6I" # ВАШ ТОКЕН
ADMIN_IDS = [8560004588] # ID Админов
MAIN_ADMIN_ID = 8122843073 # Главный админ (нельзя менять/удалять/банить)
DB_NAME = "nft_gifts.db"
FINDS_DB_PATH = "C:\\Users\\katao\\Desktop\\ыыыы\\finds.db"
LOG_FILE_PATH = "bot.log"
GRAPHS_DIR = "user_graphs"
os.makedirs(GRAPHS_DIR, exist_ok=True)

# ==================== ЛОГИРОВАНИЕ ====================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE_PATH, encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ==================== FSM STATES ====================
class UserInput(StatesGroup):
    get_specific_amount = State()
    set_custom_text = State()

class AdminInput(StatesGroup):
    set_channels = State()
    set_blacklist = State()
    get_user_by_id = State()
    user_msg_direct = State()
    set_admin_duration = State()
    set_ban_duration = State()

# ==================== БАЗА ДАННЫХ (Основная) ===================
class Database:
    def __init__(self, db_name=DB_NAME):
        self.db_name = db_name
        self.write_lock = asyncio.Lock()  # Блокировка для записи

    async def init_db(self):
        async with aiosqlite.connect(self.db_name, timeout=30) as conn:
            await conn.execute('PRAGMA journal_mode=WAL')
            await conn.execute('''
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY, username TEXT, first_name TEXT,
                registered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, is_banned BOOLEAN DEFAULT 0,
                ban_expires_at TIMESTAMP, is_admin BOOLEAN DEFAULT 0, admin_expires_at TIMESTAMP,
                text_template TEXT DEFAULT 'buy', custom_text TEXT DEFAULT '' )''')
            await conn.execute('CREATE TABLE IF NOT EXISTS gifts (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT UNIQUE, active BOOLEAN DEFAULT 1)')
            await conn.execute('CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, value TEXT)')
            await conn.execute('CREATE TABLE IF NOT EXISTS girl_indicators (id INTEGER PRIMARY KEY AUTOINCREMENT, word TEXT UNIQUE)')
            await conn.execute('CREATE TABLE IF NOT EXISTS bot_messages (message_key TEXT PRIMARY KEY, message_text TEXT)')
            
            # Таблицы для статистики
            await conn.execute('''
            CREATE TABLE IF NOT EXISTS user_stats (
                user_id INTEGER PRIMARY KEY,
                total_searches INTEGER DEFAULT 0,
                total_found INTEGER DEFAULT 0,
                last_search_date TIMESTAMP,
                favorite_gift TEXT,
                search_history TEXT DEFAULT '{}',
                FOREIGN KEY (user_id) REFERENCES users(user_id)
            )''')
            
            await conn.execute('''
            CREATE TABLE IF NOT EXISTS user_sessions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                session_start TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                session_end TIMESTAMP,
                actions_count INTEGER DEFAULT 0,
                FOREIGN KEY (user_id) REFERENCES users(user_id)
            )''')
            
            await conn.execute('''
            CREATE TABLE IF NOT EXISTS gift_search_counts (
                user_id INTEGER,
                gift_name TEXT,
                search_count INTEGER DEFAULT 0,
                last_searched TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (user_id, gift_name),
                FOREIGN KEY (user_id) REFERENCES users(user_id)
            )''')
            
            await conn.commit()
            await self.migrate_db()
            await self.add_default_gifts()
            await self.add_default_girl_indicators()
            await self.add_default_bot_messages()

    async def migrate_db(self):
        async with aiosqlite.connect(self.db_name, timeout=30) as conn:
            # Проверяем и добавляем недостающие колонки в таблицу users
            cursor = await conn.execute("PRAGMA table_info(users)")
            columns = [col[1] for col in await cursor.fetchall()]
            
            if 'ban_expires_at' not in columns: 
                await conn.execute('ALTER TABLE users ADD COLUMN ban_expires_at TIMESTAMP')
            if 'admin_expires_at' not in columns: 
                await conn.execute('ALTER TABLE users ADD COLUMN admin_expires_at TIMESTAMP')
            
            # Проверяем и добавляем недостающие колонки в таблицу user_stats
            cursor = await conn.execute("PRAGMA table_info(user_stats)")
            columns = [col[1] for col in await cursor.fetchall()]
            
            if 'total_searches' not in columns:
                await conn.execute('ALTER TABLE user_stats ADD COLUMN total_searches INTEGER DEFAULT 0')
            if 'total_found' not in columns:
                await conn.execute('ALTER TABLE user_stats ADD COLUMN total_found INTEGER DEFAULT 0')
            if 'last_search_date' not in columns:
                await conn.execute('ALTER TABLE user_stats ADD COLUMN last_search_date TIMESTAMP')
            if 'favorite_gift' not in columns:
                await conn.execute('ALTER TABLE user_stats ADD COLUMN favorite_gift TEXT')
            if 'search_history' not in columns:
                await conn.execute('ALTER TABLE user_stats ADD COLUMN search_history TEXT DEFAULT "{}"')
            
            await conn.commit()

    async def get_setting(self, key: str, default: str = "") -> str:
        async with aiosqlite.connect(self.db_name, timeout=30) as conn:
            async with conn.execute('SELECT value FROM settings WHERE key = ?', (key,)) as cursor:
                res = await cursor.fetchone()
                return res[0] if res else default

    async def set_setting(self, key: str, value: str):
        async with self.write_lock:
            async with aiosqlite.connect(self.db_name, timeout=30) as conn:
                await conn.execute('INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)', (key, value))
                await conn.commit()

    async def get_shop_blacklist(self) -> Set[str]:
        blacklist_str = await self.get_setting('shop_blacklist', '')
        return {name.strip().lower().replace('@', '') for name in blacklist_str.split()}

    async def get_required_channels(self) -> List[str]:
        channels_str = await self.get_setting('required_channels', '')
        return [ch.strip() for ch in channels_str.split() if ch.strip().startswith('@')]

    async def get_bot_message(self, key: str, default: str = "") -> str:
        async with aiosqlite.connect(self.db_name, timeout=30) as conn:
            async with conn.execute('SELECT message_text FROM bot_messages WHERE message_key = ?', (key,)) as cursor:
                res = await cursor.fetchone()
                return res[0] if res else default

    async def add_default_bot_messages(self):
        defaults = { 
            "start_subscribed": "*Главное меню* ⌵", 
            "subscription_needed": "⚠️ *Подпишитесь на каналы для доступа!*",
        }
        async with aiosqlite.connect(self.db_name, timeout=30) as conn:
            for key, text in defaults.items():
                await conn.execute('INSERT OR IGNORE INTO bot_messages (message_key, message_text) VALUES (?, ?)', (key, text))
            await conn.commit()
            
    async def add_default_girl_indicators(self):
        inds = ['анна','аня','маша','мария','елена','лена','ольга','оля','ирина','ира','наталья','наташа','татьяна','таня','ксения','юлия','юля','виктория','вика','анастасия','настя','дарья','даша','диана','софия','софья','александра','саша','екатерина','катя','светлана','света','марина','алина','алиса','ангелина','анжела','анжелика','валентина','валя','валерия','лера','вера','вероника','ника','галина','галя','евгения','женя','елизавета','лиза','жанна','зоя','инга','инесса','карина','кира','кристина','лада','лариса','лидия','лилия','любовь','люда','маргарита','рита','мирослава','надежда','надя','оксана','полина','поля','регина','снежана','тамара','ульяна','элеонора','эмилия','яна','ярослава','emily','sophia','olivia','ava','isabella','mia','charlotte','amelia','harper','lily','grace','chloe','ella','scarlett','victoria','madison','luna','aurora','claire','alice','hannah','sarah','zoe','maya','leah','sophie','marie','anna','clara','elena','lisa','natalia','julia','isabel','beatrice','girl','lady','queen','miss','woman','beauty','love','cute','angel','star','princess','девушка','девочка','леди','красавица','милашка','зайка','киска','солнышко','богиня','фея','русалка','нимфа','муза','2000','2001','2002','2003','2004','2005','2006','2007','2008','2009','2010','1999','1998','1997','❤','💖','💕','💞','💗','💓','💘','✨','🌟','⭐','🌙','🌸','🌺','🌷','🌹','💐','🎀','👑','🦋','🐰','🐱','💄','💍']
        async with aiosqlite.connect(self.db_name, timeout=30) as conn:
            await conn.executemany('INSERT OR IGNORE INTO girl_indicators (word) VALUES (?)', [(i,) for i in inds])
            await conn.commit()

    async def get_girl_indicators(self) -> Set[str]:
        async with aiosqlite.connect(self.db_name, timeout=30) as conn:
            async with conn.execute('SELECT word FROM girl_indicators') as cursor:
                return {row[0].lower() for row in await cursor.fetchall()}

    async def get_all_gifts(self) -> List[str]:
        async with aiosqlite.connect(self.db_name, timeout=30) as conn:
            async with conn.execute('SELECT name FROM gifts WHERE active = 1 ORDER BY name ASC') as cursor:
                return [row[0] for row in await cursor.fetchall()]

    async def add_default_gifts(self):
        defaults = sorted(list(set([
            'Jack-in-the-Box', 'Mighty Arm', 'B-Day Candle', 'Flying Broom', 'Star Notepad', 'Moon Pendant', 'Ginger Cookie', 'Witch Hat', 'Joyful Bundle', 'Restless Jar', 'Hanging Star', 'Love Candle', 'Happy Brownie', 'Eternal Candle', 'Holiday Drink', 'Lush Bouquet', 'Skull Flower', 'Ion Gem', 'Tama Gadget', 'Artisan Brick', 'Perfume Bottle', 'Swag Bag', 'Mini Oscar', 'Genie Lamp', 'Scared Cat', 'Hex Pot', 'Voodoo Doll', 'Neko Helmet', 'Snoop Dogg', 'Sleigh Bell', 'Snoop Cigar', 'Spiced Wine', 'Desk Calendar', 'Lol Pop', 'Light Sword', 'Cookie Heart', 'Lunar Snake', 'Party Sparkler', 'Crystal Ball', 'Low Rider', 'Clover Pin', 'Bunny Muffin', 'Faith Amulet', 'Big Year', 'Durov\'s Cap', 'Plush Pepe', 'Cupid Charm', 'Gem Signet', 'Heroic Helmet', 'Fresh Socks', 'Swiss Watch', 'Valentine Box', 'Spy Agaric', 'Jolly Chimp', 'Spring Basket', 'Electric Skull', 'Santa Hat', 'Record Player', 'Input Key', 'Hypno Lollipop', 'Snow Globe', 'Stellar Rocket', 'Evil Eye', 'Sharp Tongue', 'Winter Wreath', 'Ionic Dryer', 'Xmas Stocking', 'Sakura Flower', 'Love Potion', 'Kissed Frog', 'Snake Box', 'Jingle Bells', 'Astral Shard', 'Top Hat', 'Diamond Ring', 'Magic Potion', 'Whip Cupcake', 'Mad Pumpkin', 'Easter Egg', 'Pet Snake', 'Westside Sign', 'Loot Bag', 'Trapped Heart', 'Ice Cream', 'Berry Box', 'Candy Cane', 'Bonded Ring', 'Vintage Cigar', 'Heart Locket', 'Jelly Bunny', 'Jester Hat', 'Precious Peach', 'Instant Ramen', 'Toy Bear', 'Nail Bracelet', 'Snow Mittens', 'Eternal Rose', 'Bow Tie', 'Mousse Cake', 'Sky Stilettos', 'Signet Ring', 'Homemade Cake', 'Ancient Scroll', 'Big Cubus', 'Bling Binky', 'Enchanted Book', 'Golden Key', 'Jewelry Box', 'Magic Wand', 'Mimic Chest', 'Molten Core', 'Mystery Box', 'Oranges', 'Poker Chips', 'Red Caviar', 'Retro Player', 'Silver Coin', 'Toxic Ooze', 'Treasure Chest'
        ])))
        async with aiosqlite.connect(self.db_name, timeout=30) as conn:
            await conn.executemany('INSERT OR IGNORE INTO gifts (name) VALUES (?)', [(g,) for g in defaults])
            await conn.commit()

    async def register_user(self, user_id: int, username: str, first_name: str):
        async with self.write_lock:
            async with aiosqlite.connect(self.db_name, timeout=30) as conn:
                await conn.execute('INSERT OR IGNORE INTO users (user_id, username, first_name) VALUES (?, ?, ?)', (user_id, username or "", first_name or ""))
                await conn.execute('INSERT OR IGNORE INTO user_stats (user_id) VALUES (?)', (user_id,))
                await conn.commit()

    async def get_user_info(self, user_id: int) -> Optional[Dict]:
        async with aiosqlite.connect(self.db_name, timeout=30) as conn:
            conn.row_factory = aiosqlite.Row
            async with conn.execute('SELECT * FROM users WHERE user_id = ?', (user_id,)) as cursor:
                row = await cursor.fetchone()
                return dict(row) if row else None

    async def get_user_stats(self, user_id: int) -> Optional[Dict]:
        async with aiosqlite.connect(self.db_name, timeout=30) as conn:
            conn.row_factory = aiosqlite.Row
            async with conn.execute('SELECT * FROM user_stats WHERE user_id = ?', (user_id,)) as cursor:
                row = await cursor.fetchone()
                if row:
                    stats = dict(row)
                    # Parse search history JSON
                    if stats.get('search_history'):
                        try:
                            stats['search_history'] = json.loads(stats['search_history'])
                        except:
                            stats['search_history'] = {}
                    else:
                        stats['search_history'] = {}
                    return stats
                return None

    async def is_admin(self, user_id: int) -> bool:
        if user_id in ADMIN_IDS or user_id == MAIN_ADMIN_ID: 
            return True
        user_info = await self.get_user_info(user_id)
        if not user_info: 
            return False
        is_adm = user_info.get('is_admin')
        expires_str = user_info.get('admin_expires_at')
        if is_adm and expires_str and datetime.now() > datetime.fromisoformat(expires_str):
            await self.set_admin(user_id, False)
            return False
        return bool(is_adm)

    async def is_banned(self, user_id: int) -> bool:
        user_info = await self.get_user_info(user_id)
        if not user_info: 
            return False
        is_ban = user_info.get('is_banned')
        expires_str = user_info.get('ban_expires_at')
        if is_ban and expires_str and datetime.now() > datetime.fromisoformat(expires_str):
            await self.set_ban(user_id, False)
            return False
        return bool(is_ban)

    async def set_admin(self, user_id: int, status: bool, duration_seconds: int = 0):
        if user_id == MAIN_ADMIN_ID:
            return  # Нельзя менять главного админа
        
        expires = None
        if status and duration_seconds > 0:
            expires = (datetime.now() + timedelta(seconds=duration_seconds)).isoformat()
        
        async with self.write_lock:
            async with aiosqlite.connect(self.db_name, timeout=30) as conn:
                await conn.execute('UPDATE users SET is_admin = ?, admin_expires_at = ? WHERE user_id = ?', 
                                 (1 if status else 0, expires, user_id))
                await conn.commit()

    async def set_ban(self, user_id: int, status: bool, duration_seconds: int = 0):
        if user_id == MAIN_ADMIN_ID:
            return  # Нельзя банить главного админа
        
        expires = None
        if status and duration_seconds > 0:
            expires = (datetime.now() + timedelta(seconds=duration_seconds)).isoformat()
        
        async with self.write_lock:
            async with aiosqlite.connect(self.db_name, timeout=30) as conn:
                await conn.execute('UPDATE users SET is_banned = ?, ban_expires_at = ? WHERE user_id = ?', 
                                 (1 if status else 0, expires, user_id))
                await conn.commit()

    async def get_user_text_settings(self, user_id: int) -> Tuple[str, str]:
        async with aiosqlite.connect(self.db_name, timeout=30) as conn:
            async with conn.execute('SELECT text_template, custom_text FROM users WHERE user_id = ?', (user_id,)) as cursor:
                res = await cursor.fetchone()
                return (res[0], res[1]) if res else ('buy', '')

    async def set_user_text_settings(self, user_id: int, template: str, custom: str = ""):
        async with self.write_lock:
            async with aiosqlite.connect(self.db_name, timeout=30) as conn:
                await conn.execute('UPDATE users SET text_template = ?, custom_text = ? WHERE user_id = ?', (template, custom, user_id))
                await conn.commit()
            
    async def get_all_users_paginated(self, page: int, per_page: int) -> List[Dict]:
        async with aiosqlite.connect(self.db_name, timeout=30) as conn:
            conn.row_factory = aiosqlite.Row
            offset = page * per_page
            async with conn.execute('SELECT * FROM users ORDER BY registered_at DESC LIMIT ? OFFSET ?', (per_page, offset)) as cursor:
                return [dict(row) for row in await cursor.fetchall()]

    # ==================== СТАТИСТИКА ====================
    
    async def start_user_session(self, user_id: int):
        """Начать сессию пользователя"""
        async with self.write_lock:
            async with aiosqlite.connect(self.db_name, timeout=30) as conn:
                # Сначала завершаем старые незавершенные сессии
                await conn.execute('''
                    UPDATE user_sessions 
                    SET session_end = CURRENT_TIMESTAMP 
                    WHERE user_id = ? AND session_end IS NULL
                ''', (user_id,))
                
                # Создаем новую сессию
                await conn.execute('INSERT INTO user_sessions (user_id) VALUES (?)', (user_id,))
                await conn.commit()

    async def end_user_session(self, user_id: int):
        """Завершить сессию пользователя"""
        async with self.write_lock:
            async with aiosqlite.connect(self.db_name, timeout=30) as conn:
                await conn.execute('''
                    UPDATE user_sessions 
                    SET session_end = CURRENT_TIMESTAMP 
                    WHERE user_id = ? AND session_end IS NULL
                ''', (user_id,))
                await conn.commit()

    async def increment_session_actions(self, user_id: int):
        """Увеличить счетчик действий в текущей сессии"""
        async with self.write_lock:
            async with aiosqlite.connect(self.db_name, timeout=30) as conn:
                await conn.execute('''
                    UPDATE user_sessions 
                    SET actions_count = actions_count + 1 
                    WHERE user_id = ? AND session_end IS NULL
                ''', (user_id,))
                await conn.commit()

    async def update_search_stats(self, user_id: int, gift_name: str, found_count: int):
        """Обновить статистику поиска"""
        async with self.write_lock:
            try:
                async with aiosqlite.connect(self.db_name, timeout=30) as conn:
                    # Обновить общую статистику
                    await conn.execute('''
                        UPDATE user_stats 
                        SET total_searches = total_searches + 1,
                            total_found = total_found + ?,
                            last_search_date = CURRENT_TIMESTAMP
                        WHERE user_id = ?
                    ''', (found_count, user_id))
                    
                    # Обновить счетчик для конкретного подарка
                    await conn.execute('''
                        INSERT OR REPLACE INTO gift_search_counts (user_id, gift_name, search_count, last_searched)
                        VALUES (?, ?, COALESCE((SELECT search_count FROM gift_search_counts WHERE user_id = ? AND gift_name = ?), 0) + 1, CURRENT_TIMESTAMP)
                    ''', (user_id, gift_name, user_id, gift_name))
                    
                    # Обновить любимый подарк
                    await conn.execute('''
                        UPDATE user_stats 
                        SET favorite_gift = (
                            SELECT gift_name 
                            FROM gift_search_counts 
                            WHERE user_id = ? 
                            ORDER BY search_count DESC, last_searched DESC 
                            LIMIT 1
                        )
                        WHERE user_id = ?
                    ''', (user_id, user_id))
                    
                    # Обновить историю поисков
                    await self.update_search_history(user_id, gift_name, found_count, conn)
                    
                    await conn.commit()
            except Exception as e:
                logger.error(f"Error updating search stats: {e}")

    async def update_search_history(self, user_id: int, gift_name: str, found_count: int, conn=None):
        """Обновить историю поисков в JSON формате"""
        async def _update(connection):
            # Получить текущую историю
            async with connection.execute('SELECT search_history FROM user_stats WHERE user_id = ?', (user_id,)) as cursor:
                row = await cursor.fetchone()
                history = {}
                if row and row[0]:
                    try:
                        history = json.loads(row[0])
                    except:
                        history = {}
                
                # Добавить текущий поиск
                date_str = datetime.now().strftime("%Y-%m-%d")
                if date_str not in history:
                    history[date_str] = []
                
                history[date_str].append({
                    'gift': gift_name,
                    'found': found_count,
                    'time': datetime.now().strftime("%H:%M:%S")
                })
                
                # Сохранить только последние 30 дней
                if len(history) > 30:
                    oldest_date = sorted(history.keys())[0]
                    del history[oldest_date]
                
                # Сохранить обновленную историю
                await connection.execute('UPDATE user_stats SET search_history = ? WHERE user_id = ?', 
                                       (json.dumps(history), user_id))
        
        if conn:
            await _update(conn)
        else:
            async with self.write_lock:
                async with aiosqlite.connect(self.db_name, timeout=30) as connection:
                    await _update(connection)
                    await connection.commit()

    async def get_user_search_stats(self, user_id: int) -> Dict:
        """Получить полную статистику поисков пользователя"""
        stats = await self.get_user_stats(user_id)
        if not stats:
            return {}
        
        async with aiosqlite.connect(self.db_name, timeout=30) as conn:
            conn.row_factory = aiosqlite.Row
            
            # Получить топ подарков
            async with conn.execute('''
                SELECT gift_name, search_count, last_searched 
                FROM gift_search_counts 
                WHERE user_id = ? 
                ORDER BY search_count DESC 
                LIMIT 10
            ''', (user_id,)) as cursor:
                top_gifts = [dict(row) for row in await cursor.fetchall()]
            
            # Получить статистику сессий
            async with conn.execute('''
                SELECT COUNT(*) as session_count,
                       SUM(actions_count) as total_actions,
                       AVG(actions_count) as avg_actions,
                       SUM(julianday(session_end) - julianday(session_start)) * 24 * 60 as total_minutes
                FROM user_sessions 
                WHERE user_id = ? AND session_end IS NOT NULL
            ''', (user_id,)) as cursor:
                session_stats = await cursor.fetchone()
                session_data = dict(session_stats) if session_stats else {}
            
            return {
                'basic_stats': stats,
                'top_gifts': top_gifts,
                'session_stats': session_data
            }

    async def get_user_graph_data(self, user_id: int) -> Optional[bytes]:
        """Сгенерировать график статистики пользователя"""
        stats = await self.get_user_search_stats(user_id)
        if not stats or not stats['top_gifts']:
            return None
        
        # Создать график
        plt.figure(figsize=(10, 6))
        
        # Данные для графика
        gifts = [item['gift_name'][:15] + ('...' if len(item['gift_name']) > 15 else '') 
                for item in stats['top_gifts']]
        counts = [item['search_count'] for item in stats['top_gifts']]
        
        # Цветовая схема
        colors = plt.cm.Set3(range(len(gifts)))
        
        # Создать столбчатую диаграмму
        bars = plt.bar(gifts, counts, color=colors, edgecolor='black')
        
        # Добавить значения на столбцы
        for bar, count in zip(bars, counts):
            plt.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.1,
                    str(count), ha='center', va='bottom', fontsize=9)
        
        plt.title(f'Топ подарков пользователя ID: {user_id}', fontsize=14, fontweight='bold')
        plt.xlabel('Название подарка', fontsize=12)
        plt.ylabel('Количество поисков', fontsize=12)
        plt.xticks(rotation=45, ha='right')
        plt.tight_layout()
        
        # Сохранить в буфер
        buf = BytesIO()
        plt.savefig(buf, format='png', dpi=100)
        plt.close()
        buf.seek(0)
        
        return buf.getvalue()

# ==================== ЛОГГЕР НАХОДОК ====================
class FindsLogger:
    def __init__(self, db_path: str):
        self.db_path = db_path
        try: 
            os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        except OSError as e: 
            logger.error(f"Could not create directory: {e}")

    async def init_db(self):
        async with aiosqlite.connect(self.db_path, timeout=30) as conn:
            await conn.execute('''CREATE TABLE IF NOT EXISTS found_owners (id INTEGER PRIMARY KEY AUTOINCREMENT,
                found_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, username TEXT, gift_name TEXT, gift_number INTEGER, nft_url TEXT)''')
            await conn.commit()

    async def log_find(self, username: str, gift_name: str, gift_number: int, nft_url: str):
        try:
            async with aiosqlite.connect(self.db_path, timeout=30) as conn:
                await conn.execute('INSERT INTO found_owners (username, gift_name, gift_number, nft_url) VALUES (?, ?, ?, ?)',
                    (username, gift_name, gift_number, nft_url))
                await conn.commit()
        except Exception as e: 
            logger.error(f"FindsLogger Error: {e}")

# ==================== ПАРСЕР ====================
class NFTParser:
    def __init__(self, db: Database, finds_logger: FindsLogger):
        self.db, self.finds_logger = db, finds_logger
        self.base_url = "https://t.me/nft/{}-{}"
        self.session: Optional[aiohttp.ClientSession] = None
        self.active_searches: Dict[str, asyncio.Event] = {}
        self.user_results: Dict[int, List[Dict]] = {}
        self.sys_names = {'nft', 'gift', 'gifts', 'telegram', 'fragment', 'ton', 'store'}
        self.regex = re.compile(r't\.me/([a-zA-Z0-9_]{5,32})')
        self.semaphore = asyncio.Semaphore(150)

    async def init_session(self):
        if not self.session or self.session.closed:
            conn = aiohttp.TCPConnector(limit=300, limit_per_host=150)
            self.session = aiohttp.ClientSession(connector=conn)

    async def close_session(self):
        if self.session and not self.session.closed: 
            await self.session.close()

    async def check_url(self, gift: str, num: int) -> Optional[Dict]:
        clean_name = gift.replace(" ", "").replace("-", "")
        url = self.base_url.format(clean_name, num)
        
        async with self.semaphore:
            try:
                headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"}
                async with self.session.get(url, allow_redirects=False, headers=headers, timeout=15) as resp:
                    if resp.status == 200:
                        txt = await resp.text()
                        if 'Gift not found' in txt or 't.me/' not in txt: 
                            return None
                        found_match = self.regex.findall(txt)
                        for user in found_match:
                            if user.lower() not in self.sys_names:
                                return {'username': user, 'number': num, 'url': url, 'gift_name': gift}
                    elif resp.status == 429:
                        logger.warning(f"Rate limit hit! Semaphore will handle the delay.")
                        await asyncio.sleep(2)
                    return None
            except (asyncio.TimeoutError, aiohttp.ClientError, CancelledError): 
                return None
            except Exception: 
                return None
    
    async def _worker(self, queue: asyncio.Queue, gift: str, found: List, users_set: Set,
                     lock: asyncio.Lock, total_checked: List[int], cb, max_results: int,
                     stop_event: asyncio.Event, mode: str, girl_inds: Set, shop_blacklist: Set):
        while not stop_event.is_set():
            try:
                num = await asyncio.wait_for(queue.get(), timeout=1.0)
            except (asyncio.TimeoutError, CancelledError):
                break
            
            item = await self.check_url(gift, num)
            
            if item:
                async with lock:
                    if stop_event.is_set(): 
                        break
                    
                    username_lower = item['username'].lower()
                    if username_lower not in users_set and username_lower not in shop_blacklist:
                        if mode == 'girls':
                            is_g = any(x in username_lower for x in girl_inds) or any(username_lower.endswith(x) for x in ['a', 'ya', 'na', 'ka'])
                            if not is_g:
                                queue.task_done()
                                continue
                        
                        logger.info(f"[OWNER FOUND] User: @{item['username']}, Gift: {item['gift_name']} #{item['number']}")
                        await self.finds_logger.log_find(item['username'], item['gift_name'], item['number'], item['url'])
                        found.append(item)
                        users_set.add(username_lower)
                        
                        if len(found) >= max_results:
                            stop_event.set()

            async with lock:
                total_checked[0] += 1
                if cb and (total_checked[0] % 20 == 0 or (item and len(found) < max_results)):
                    try: 
                        await cb(len(found), max_results, total_checked[0])
                    except Exception as e: 
                        logger.error(f"Progress update error: {e}")
            
            queue.task_done()

    async def search(self, gift: str, uid: int, mode: str, max_results: int, cb, sid: str) -> List[Dict]: 
        await self.init_session()
        found, users_set = [], set()
        lock = asyncio.Lock()
        total_checked = [0]
        stop_event = asyncio.Event()
        self.active_searches[sid] = stop_event
        girl_inds = await self.db.get_girl_indicators() if mode == 'girls' else set()
        shop_blacklist = await self.db.get_shop_blacklist()
        
        priority_range = list(range(1, 2001))
        mid_range = list(range(2001, 7001))
        other_range = list(range(7001, 25001))
        random.shuffle(priority_range)
        random.shuffle(mid_range)
        random.shuffle(other_range)
        search_range = priority_range + mid_range + other_range

        queue = asyncio.Queue()
        for num in search_range:
            queue.put_nowait(num)
        
        N_WORKERS = 100
        worker_tasks = []
        for _ in range(N_WORKERS):
            task = asyncio.create_task(
                self._worker(queue, gift, found, users_set, lock, total_checked, cb, max_results, stop_event, mode, girl_inds, shop_blacklist)
            )
            worker_tasks.append(task)
        
        try:
            await stop_event.wait()
        finally:
            for task in worker_tasks:
                task.cancel()
            await asyncio.gather(*worker_tasks, return_exceptions=True)
            if sid in self.active_searches: 
                del self.active_searches[sid]
        
        if cb: 
            await cb(len(found), max_results, total_checked[0])
        
        # Обновить статистику пользователя
        try:
            await db.update_search_stats(uid, gift, len(found))
        except Exception as e:
            logger.error(f"Error updating search stats: {e}")
        
        self.user_results[uid] = found
        return found

    def cancel(self, sid):
        if sid in self.active_searches: 
            self.active_searches[sid].set()

# ==================== INIT & UTILS ====================
db = Database()
finds_logger = FindsLogger(db_path=FINDS_DB_PATH)
parser = NFTParser(db=db, finds_logger=finds_logger)
bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode="MarkdownV2"))
dp = Dispatcher(storage=MemoryStorage())

def progress_bar(curr, total, size=15):
    if total == 0: 
        return '░' * size
    p = min(curr / total, 1.0)
    fill = int(size * p)
    return '█' * fill + '░' * (size - fill)

def search_text(found: int, total: int, attempts: int):
    progress_line = f"📊 *Прогресс: {found}/{total}*\n`[{progress_bar(found, total)}]`"
    return (f"🔍 *Поиск\\.\\.\\.*\n\n{progress_line}\n\n"
            f"┣ *Проверено: {attempts}*\n┣ *dev: @stuffiny*\n┗ *Найдено: {found}*")

def escape_markdown(text: str) -> str:
    if not isinstance(text, str): 
        text = str(text)
    escape_chars = r'_*[]()~`>#+-=|{}.!'
    return re.sub(f'([{re.escape(escape_chars)}])', r'\\\1', text)

def gen_link(user, gift, tmpl, cust):
    if tmpl == 'custom': 
        txt = cust
    elif tmpl == 'buy': 
        txt = f"привет, не хочешь продать свой подарок {gift}? Сразу скажу что не попрошу скинуть сначала подарок:)"
    else: 
        txt = "привет, не отвлекаю? можешь плиз поставить мне в канал реакции пожалуйста? А я тебе за это 150-200 звездочек дам, очень поможешь🙏"
    return f"https://t.me/{user}?text={quote(txt)}"

async def check_sub(user_id: int) -> bool:
    if await db.is_admin(user_id): 
        return True
    channels = await db.get_required_channels()
    if not channels: 
        return True
    for ch in channels:
        try:
            m = await bot.get_chat_member(ch, user_id)
            if m.status not in ['creator', 'administrator', 'member']: 
                return False
        except Exception: 
            return False
    return True

def format_duration(seconds: int) -> str:
    """Форматировать продолжительность в читаемый вид"""
    if seconds == 0:
        return "навсегда"
    
    days, seconds = divmod(seconds, 86400)
    hours, seconds = divmod(seconds, 3600)
    minutes, seconds = divmod(seconds, 60)
    
    parts = []
    if days > 0:
        parts.append(f"{days}д")
    if hours > 0:
        parts.append(f"{hours}ч")
    if minutes > 0:
        parts.append(f"{minutes}м")
    if seconds > 0 and not (days or hours or minutes):
        parts.append(f"{seconds}с")
    
    return " ".join(parts)

# ==================== ГЛАВНЫЕ ОБРАБОТЧИКИ ====================
async def start_search_process(message_to_edit: types.Message, user: types.User, mode: str, gift: str, max_results: int):
    sid = str(uuid.uuid4())[:8]
    stop_callback_data = f"stop:{sid}:{gift}:{mode}"
    
    await message_to_edit.edit_text(
        search_text(0, max_results, 0), 
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🛑 Стоп", callback_data=stop_callback_data)]
        ])
    )
    
    last_update_time, last_found_count, last_checked_count = asyncio.get_event_loop().time(), -1, -1
    
    async def cb(found_now: int, max_r: int, attempts: int):
        nonlocal last_update_time, last_found_count, last_checked_count
        current_time = asyncio.get_event_loop().time()
        
        if found_now != last_found_count or attempts != last_checked_count or (current_time - last_update_time > 2.0):
            try:
                display_found = min(found_now, max_r)
                status_text = search_text(display_found, max_r, attempts)
                await message_to_edit.edit_text(
                    status_text,
                    reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🛑 Стоп", callback_data=stop_callback_data)]])
                )
                last_update_time, last_found_count, last_checked_count = current_time, found_now, attempts
            except TelegramBadRequest as e:
                if "message is not modified" not in str(e).lower(): 
                    logger.warning(f"Progress update fail: {e}")
            except Exception as e: 
                logger.error(f"Unexpected progress update error: {e}")
    
    try:
        res = await parser.search(gift, user.id, mode, max_results, cb, sid)
    except Exception as e:
        logger.error(f"Search error: {e}")
        await message_to_edit.edit_text(f"❌ *Ошибка при поиске: {str(e)}*", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔄 Попробовать еще", callback_data=f"select_gift:{mode}:{gift}")], [InlineKeyboardButton(text="🏠 Главное меню", callback_data="main_menu")]]))
        return
    
    if sid in parser.active_searches: 
        parser.cancel(sid)
    if not res:
        await message_to_edit.edit_text(f"😔 *Ничего не найдено для {escape_markdown(gift)}*", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔄 Попробовать еще", callback_data=f"select_gift:{mode}:{gift}")], [InlineKeyboardButton(text="🏠 Главное меню", callback_data="main_menu")]]))
        return
    
    await asyncio.sleep(0.5)
    await show_res(user.id, message_to_edit, 0, gift, mode)

@dp.message(Command("start"))
async def handle_start(msg: types.Message):
    user_id = msg.from_user.id
    if await db.is_banned(user_id): 
        return
    
    await db.register_user(user_id, msg.from_user.username, msg.from_user.first_name)
    
    # Начать сессию пользователя
    await db.start_user_session(user_id)
    await db.increment_session_actions(user_id)
    
    if not await check_sub(user_id):
        channels = await db.get_required_channels()
        kb = [[InlineKeyboardButton(text=f"📣 {c.replace('@', '')}", url=f"https://t.me/{c.replace('@', '')}")] for c in channels]
        kb.append([InlineKeyboardButton(text="✅ Я подписался", callback_data="check_sub")])
        text = await db.get_bot_message("subscription_needed", "⚠️ *Подпишитесь на каналы для доступа\\!*")
        return await msg.answer(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))
    await menu(msg)

@dp.callback_query(F.data == "check_sub")
async def handle_sub_check(call: CallbackQuery):
    await db.increment_session_actions(call.from_user.id)
    
    if await check_sub(call.from_user.id): 
        await call.message.delete()
        await menu(call.message)
    else: 
        await call.answer("❌ Вы не подписались на все каналы!", show_alert=True)

@dp.callback_query(F.data == "main_menu")
async def handle_main_menu_cb(call: CallbackQuery): 
    await db.increment_session_actions(call.from_user.id)
    await menu(call)

async def menu(msg_or_call):
    await db.increment_session_actions(msg_or_call.from_user.id)
    
    is_adm = await db.is_admin(msg_or_call.from_user.id)
    kb = [
        [InlineKeyboardButton(text="🎁 Парсинг", callback_data="mode:search"), 
         InlineKeyboardButton(text="👧 Девочки", callback_data="mode:girls")],
        [InlineKeyboardButton(text="📦 Список NFT", callback_data="mode:list"), 
         InlineKeyboardButton(text="⚙️ Текст", callback_data="settings")]
    ]
    if is_adm: 
        kb.append([InlineKeyboardButton(text="👑 Админ-панель", callback_data="admin_home")])
    
    text = await db.get_bot_message("start_subscribed", "*Главное меню* ⌵")
    try:
        if isinstance(msg_or_call, types.Message): 
            await msg_or_call.answer(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))
        else: 
            await msg_or_call.message.edit_text(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))
    except Exception as e: 
        logger.error(f"Menu display error: {e}")

@dp.callback_query(F.data.startswith("mode:"))
async def sel_mode(call: CallbackQuery):
    await db.increment_session_actions(call.from_user.id)
    
    mode = call.data.split(":")[1]
    if mode == 'list':
        g = await db.get_all_gifts()
        text = f"📦 *Список доступных NFT \\({len(g)}\\):*\n" + ", ".join(g)
        if len(g) > 50:
            text = f"📦 *Список доступных NFT \\({len(g)}\\):*\n" + ", ".join(g[:50]) + f"\n\n\\.\\.\\.и еще {escape_markdown(len(g)-50)} подарков"
        return await call.message.edit_text(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🏠 Назад", callback_data="main_menu")]]))
    await call.message.edit_text(f"🎁 *Выберите подарок:*", reply_markup=await gifts_kb(await db.get_all_gifts(), 0, mode))

async def gifts_kb(gifts, page, mode):
    start, end = page*8, (page+1)*8
    chunk, kb = gifts[start:end], []
    for i in range(0, len(chunk), 2):
        row = [InlineKeyboardButton(text=chunk[j][:12] + ("..." if len(chunk[j]) > 12 else ""), callback_data=f"select_gift:{mode}:{chunk[j]}") for j in range(i, min(i+2, len(chunk)))]
        kb.append(row)
    nav = []
    if page > 0: 
        nav.append(InlineKeyboardButton(text="⬅️", callback_data=f"pg:{page-1}:{mode}"))
    nav.append(InlineKeyboardButton(text=f"{page+1}", callback_data="noop"))
    if end < len(gifts): 
        nav.append(InlineKeyboardButton(text="➡️", callback_data=f"pg:{page+1}:{mode}"))
    if nav: 
        kb.append(nav)
    kb.append([InlineKeyboardButton(text="🏠 Главное меню", callback_data="main_menu")])
    return InlineKeyboardMarkup(inline_keyboard=kb)

@dp.callback_query(F.data.startswith("pg:"))
async def handle_pg_cb(call: CallbackQuery):
    await db.increment_session_actions(call.from_user.id)
    
    _, p, m = call.data.split(":")
    await call.message.edit_reply_markup(reply_markup=await gifts_kb(await db.get_all_gifts(), int(p), m))

@dp.callback_query(F.data.startswith("select_gift:"))
async def select_gift(call: CallbackQuery, state: FSMContext):
    await db.increment_session_actions(call.from_user.id)
    
    _, mode, gift = call.data.split(":")
    await state.set_state(UserInput.get_specific_amount)
    await state.update_data(mode=mode, gift=gift, gift_menu_msg_id=call.message.message_id)
    is_admin = await db.is_admin(call.from_user.id)
    limit = 1000 if is_admin else 50
    await call.message.edit_text(f"🔢 *Введите желаемое количество для поиска* \\(1\\-{limit}\\):", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="✖️ Отмена", callback_data="main_menu")]]))

@dp.message(UserInput.get_specific_amount)
async def process_custom_specific_amount(msg: types.Message, state: FSMContext):
    await db.increment_session_actions(msg.from_user.id)
    
    data = await state.get_data()
    mode, gift, gift_menu_msg_id = data['mode'], data['gift'], data.get('gift_menu_msg_id')
    await state.clear()
    
    is_admin = await db.is_admin(msg.from_user.id)
    limit = 1000 if is_admin else 50
    
    try:
        if not msg.text.isdigit() or not (1 <= int(msg.text) <= limit):
            if gift_menu_msg_id: 
                await bot.delete_message(msg.chat.id, gift_menu_msg_id)
            await msg.answer(f"❗️ *Ошибка: Введите число от 1 до {limit}\\.*")
            return await menu(msg)
        
        amount = int(msg.text)
        await msg.delete()
        status_msg = await bot.edit_message_text(text="🔄 *Начинаю поиск\\.\\.\\.*", chat_id=msg.chat.id, message_id=gift_menu_msg_id)
        await start_search_process(status_msg, msg.from_user, mode, gift, amount)

    except Exception as e:
        logger.error(f"Error in process_custom_specific_amount: {e}")
        status_msg = await msg.answer("🔄 *Начинаю поиск\\.\\.\\.*")
        await start_search_process(status_msg, msg.from_user, mode, gift, int(msg.text))

async def show_res(uid, msg, page, gift, mode):
    res = parser.user_results.get(uid, [])
    if not res:
        return await msg.edit_text("😕 *Не найдено результатов для отображения\\.*", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🏠 Главное меню", callback_data="main_menu")]]))

    header = f"🦣 *Насобирал тебе лохматых: {len(res)} шт\\.*\n\n"
    start, end = page * 5, (page + 1) * 5
    chunk = res[start:end]
    tmpl, cust = await db.get_user_text_settings(uid)
    body = ""
    for r in chunk:
        safe_username = escape_markdown(r['username'])
        write_link = gen_link(r['username'], r['gift_name'], tmpl, cust)
        nft_link = r.get('url', '') 
        body += f"🔎 [LINK NFT]({nft_link}) \\| @{safe_username} \\| [Написать]({write_link})\n"
    
    total_pages = ((len(res) - 1) // 5) + 1
    footer = f"\n*Страница {page + 1} из {total_pages}*"
    full_text = header + body + footer
    
    kb, nav = [], []
    if page > 0: 
        nav.append(InlineKeyboardButton(text="⬅️", callback_data=f"res:{page-1}:{gift}:{mode}"))
    if end < len(res): 
        nav.append(InlineKeyboardButton(text="➡️", callback_data=f"res:{page+1}:{gift}:{mode}"))
    if nav: 
        kb.append(nav)
    kb.append([InlineKeyboardButton(text="🔄 Найти еще", callback_data=f"select_gift:{mode}:{gift}")])
    kb.append([InlineKeyboardButton(text="🏠 Главное меню", callback_data="main_menu")])
    
    try:
        await msg.edit_text(full_text, reply_markup=InlineKeyboardMarkup(inline_keyboard=kb), disable_web_page_preview=True)
    except Exception as e:
        logger.error(f"Error showing results: {e}")

@dp.callback_query(F.data.startswith("res:"))
async def res_nav(call: CallbackQuery):
    await db.increment_session_actions(call.from_user.id)
    
    _, p, g, m = call.data.split(":")
    await show_res(call.from_user.id, call.message, int(p), g, m)

@dp.callback_query(F.data.startswith("stop:"))
async def stop_search(call: CallbackQuery):
    await db.increment_session_actions(call.from_user.id)
    
    try: 
        _, sid, gift, mode = call.data.split(":", 3)
    except ValueError: 
        sid, gift, mode = "unknown", "unknown", "search"
    
    parser.cancel(sid)
    await call.answer("Поиск остановлен", show_alert=False)
    await asyncio.sleep(0.5) 
    res = parser.user_results.get(call.from_user.id, [])
    if res and gift != "unknown": 
        await show_res(call.from_user.id, call.message, 0, gift, mode)
    else:
        try: 
            await call.message.edit_text("🛑 *Поиск был остановлен пользователем\\.*", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🏠 Главное меню", callback_data="main_menu")]]))
        except TelegramBadRequest as e:
            if "message to edit not found" not in str(e).lower(): 
                logger.error(f"Stop search message edit error: {e}")

# ==================== НАСТРОЙКИ ПОЛЬЗОВАТЕЛЯ ====================
@dp.callback_query(F.data == "settings")
async def settings_menu(call: CallbackQuery):
    await db.increment_session_actions(call.from_user.id)
    
    t, _ = await db.get_user_text_settings(call.from_user.id)
    kb = [
        [InlineKeyboardButton(text=f"{'✅ ' if t=='buy' else ''}🛒 Купить", callback_data="set:buy")],
        [InlineKeyboardButton(text=f"{'✅ ' if t=='reaction' else ''}❤️ Реакции", callback_data="set:reaction")],
        [InlineKeyboardButton(text=f"{'✅ ' if t=='custom' else ''}✏️ Свой текст", callback_data="set:custom")],
        [InlineKeyboardButton(text="🏠 Назад", callback_data="main_menu")]
    ]
    await call.message.edit_text("⚙️ *Выберите шаблон сообщения:*", reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))

@dp.callback_query(F.data.startswith("set:"))
async def set_text_mode(call: CallbackQuery, state: FSMContext):
    await db.increment_session_actions(call.from_user.id)
    
    m = call.data.split(":")[1]
    if m == 'custom':
        await state.set_state(UserInput.set_custom_text)
        await call.message.edit_text("✏️ *Введите свой текст для сообщения:*", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="✖️ Отмена", callback_data="settings")]]))
    else:
        await db.set_user_text_settings(call.from_user.id, m)
        await settings_menu(call)

@dp.message(UserInput.set_custom_text)
async def save_custom_text(msg: types.Message, state: FSMContext):
    await db.increment_session_actions(msg.from_user.id)
    
    await db.set_user_text_settings(msg.from_user.id, 'custom', msg.text)
    await state.clear()
    await msg.answer("✅ *Текст сохранен\\!*")
    await menu(msg)

# ==================== АДМИН-ПАНЕЛЬ ========================
@dp.callback_query(F.data == "admin_home")
async def admin_home(call: types.CallbackQuery):
    if not await db.is_admin(call.from_user.id): 
        return
    
    await db.increment_session_actions(call.from_user.id)
    
    kb = [
        [InlineKeyboardButton(text="👥 Пользователи", callback_data="adm_users_list:0")],
        [InlineKeyboardButton(text="📢 Каналы для подписки", callback_data="adm_channels")],
        [InlineKeyboardButton(text="🚫 Черный список", callback_data="adm_blacklist")],
        [InlineKeyboardButton(text="📊 Статистика бота", callback_data="adm_bot_stats")],
        [InlineKeyboardButton(text="🏠 Выход в главное меню", callback_data="main_menu")]
    ]
    await call.message.edit_text("👑 *Панель Администратора*", reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))

# --- Управление каналами ---
@dp.callback_query(F.data == "adm_channels")
async def adm_channels_menu(call: CallbackQuery, state: FSMContext):
    if not await db.is_admin(call.from_user.id): 
        return
    
    await db.increment_session_actions(call.from_user.id)
    
    channels = await db.get_required_channels()
    text = "📢 *Управление каналами для подписки*\n\nТекущий список:\n"
    text += f"`{' '.join(channels)}`" if channels else "_Список пуст_"
    text += "\n\nОтправьте юзернеймы каналов через пробел \\(например, `@channel1 @channel2`\\) для обновления списка\\. Чтобы очистить список, отправьте `clear`\\."
    kb = [[InlineKeyboardButton(text="🔙 Назад", callback_data="admin_home")]]
    await call.message.edit_text(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))
    await call.message.answer("✏️ Введите новый список каналов:")
    await state.set_state(AdminInput.set_channels)

@dp.message(AdminInput.set_channels)
async def process_set_channels(msg: types.Message, state: FSMContext):
    if not await db.is_admin(msg.from_user.id): 
        return
    
    await db.increment_session_actions(msg.from_user.id)
    await state.clear()
    
    new_channels = msg.text.lower()
    if new_channels == 'clear':
        await db.set_setting('required_channels', '')
        await msg.answer("✅ *Список каналов очищен\\!*")
    else:
        await db.set_setting('required_channels', new_channels)
        await msg.answer("✅ *Список каналов обновлен\\!*")
    await menu(msg)

# --- Управление черным списком ---
@dp.callback_query(F.data == "adm_blacklist")
async def adm_blacklist_menu(call: CallbackQuery, state: FSMContext):
    if not await db.is_admin(call.from_user.id): 
        return
    
    await db.increment_session_actions(call.from_user.id)
    
    blacklist = await db.get_shop_blacklist()
    text = "🚫 *Управление черным списком магазинов*\n\nТекущий список:\n"
    text += f"`{' '.join(blacklist)}`" if blacklist else "_Список пуст_"
    text += "\n\nОтправьте юзернеймы для черного списка через пробел \\(например, `shop1 market2`\\), чтобы обновить\\. Для очистки отправьте `clear`\\."
    kb = [[InlineKeyboardButton(text="🔙 Назад", callback_data="admin_home")]]
    await call.message.edit_text(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))
    await call.message.answer("✏️ Введите новый черный список:")
    await state.set_state(AdminInput.set_blacklist)

@dp.message(AdminInput.set_blacklist)
async def process_set_blacklist(msg: types.Message, state: FSMContext):
    if not await db.is_admin(msg.from_user.id): 
        return
    
    await db.increment_session_actions(msg.from_user.id)
    await state.clear()
    
    new_blacklist = msg.text.lower()
    if new_blacklist == 'clear':
        await db.set_setting('shop_blacklist', '')
        await msg.answer("✅ *Черный список очищен\\!*")
    else:
        await db.set_setting('shop_blacklist', new_blacklist)
        await msg.answer("✅ *Черный список обновлен\\!*")
    await menu(msg)

# --- Управление пользователями ---
@dp.callback_query(F.data.startswith("adm_users_list:"))
async def adm_users_list(call: CallbackQuery):
    if not await db.is_admin(call.from_user.id): 
        return
    
    await db.increment_session_actions(call.from_user.id)
    
    page = int(call.data.split(":")[1])
    users = await db.get_all_users_paginated(page, 10)
    kb = []
    for u in users:
        status = "🚫" if u['is_banned'] else "👤"
        admin_status = " 👑" if u['is_admin'] else ""
        if u['user_id'] == MAIN_ADMIN_ID:
            admin_status = " 👑👑"  # Двойная корона для главного админа
        
        uname = u['username'] or f"ID: {u['user_id']}"
        safe_uname = escape_markdown(uname)
        btn_text = f"{status}@{safe_uname}{admin_status}" if u['username'] else f"{status} {safe_uname}{admin_status}"
        kb.append([InlineKeyboardButton(text=btn_text[:64], callback_data=f"adm_user_manage:{u['user_id']}")])
    
    nav = []
    if page > 0: 
        nav.append(InlineKeyboardButton(text="⬅️", callback_data=f"adm_users_list:{page-1}"))
    nav.append(InlineKeyboardButton(text=f"· {page+1} ·", callback_data="noop"))
    if len(users) == 10: 
        nav.append(InlineKeyboardButton(text="➡️", callback_data=f"adm_users_list:{page+1}"))
    if nav: 
        kb.append(nav)
    
    kb.append([InlineKeyboardButton(text="🔍 Найти по ID", callback_data="adm_find_user")])
    kb.append([InlineKeyboardButton(text="🔙 Назад", callback_data="admin_home")])
    await call.message.edit_text("📋 *Список пользователей*", reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))

@dp.callback_query(F.data == "adm_find_user")
async def adm_find_user_start(call: CallbackQuery, state: FSMContext):
    if not await db.is_admin(call.from_user.id): 
        return
    
    await db.increment_session_actions(call.from_user.id)
    
    await state.set_state(AdminInput.get_user_by_id)
    await call.message.edit_text("🆔 *Введите ID пользователя для поиска:*", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="✖️ Отмена", callback_data="adm_users_list:0")]]))

@dp.message(AdminInput.get_user_by_id)
async def adm_find_user_process(msg: types.Message, state: FSMContext):
    if not await db.is_admin(msg.from_user.id): 
        return
    
    await db.increment_session_actions(msg.from_user.id)
    await state.clear()
    
    if msg.text.isdigit(): 
        await user_management_panel(msg, int(msg.text))
    else: 
        await msg.answer("❗️ *Ошибка: ID должен быть числом\\.*")

@dp.callback_query(F.data.startswith("adm_user_manage:"))
async def adm_user_manage_cb(call: CallbackQuery):
    if not await db.is_admin(call.from_user.id): 
        return
    
    await db.increment_session_actions(call.from_user.id)
    
    await user_management_panel(call, int(call.data.split(":")[1]), is_callback=True)

async def user_management_panel(msg_or_call, user_id, is_callback=False):
    message = msg_or_call.message if is_callback else msg_or_call
    user_info = await db.get_user_info(user_id)
    if not user_info:
        text = "❗️ *Пользователь не найден\\.*"
        kb = [[InlineKeyboardButton(text="🔙 К списку", callback_data="adm_users_list:0")]]
        if is_callback: 
            await message.edit_text(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))
        else: 
            await message.answer(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))
        return

    username_display = f"@{escape_markdown(user_info['username'])}" if user_info['username'] else "Нет"
    reg_date_str = escape_markdown(user_info['registered_at'].split(" ")[0])
    
    # Получить статистику пользователя
    stats = await db.get_user_stats(user_id)
    
    text_lines = [
        f"👤 *Управление: {escape_markdown(user_info['first_name'])}*",
        f"**ID**: `{user_id}` \\| **Username**: {username_display}",
        f"**Зарегистрирован**: {reg_date_str}"
    ]
    
    # Статистика если есть
    if stats:
        text_lines.append(f"**Всего поисков**: {stats.get('total_searches', 0)}")
        text_lines.append(f"**Всего найдено**: {stats.get('total_found', 0)}")
        if stats.get('favorite_gift'):
            text_lines.append(f"**Любимый подарок**: {escape_markdown(stats['favorite_gift'])}")
        if stats.get('last_search_date'):
            last_search = escape_markdown(stats['last_search_date'].split('.')[0])
            text_lines.append(f"**Последний поиск**: {last_search}")
    
    ban_status = '✅' if user_info['is_banned'] else '❌'
    ban_line = f"**Бан**: {ban_status}"
    if user_info['is_banned'] and user_info.get('ban_expires_at'):
        ban_exp = escape_markdown(user_info['ban_expires_at'].split('.')[0])
        ban_line += f" \\(до {ban_exp}\\)"
    text_lines.append(ban_line)
    
    admin_status = '✅' if user_info['is_admin'] else '❌'
    admin_line = f"**Админ**: {admin_status}"
    if user_info['is_admin'] and user_info.get('admin_expires_at'):
        admin_exp = escape_markdown(user_info['admin_expires_at'].split('.')[0])
        admin_line += f" \\(до {admin_exp}\\)"
    text_lines.append(admin_line)

    template_map = {'buy': '🛒 Купить', 'reaction': '❤️ Реакции', 'custom': '✏️ Свой текст'}
    template_text = template_map.get(user_info['text_template'], 'Неизвестно')
    text_lines.append(f"**Шаблон текста**: {escape_markdown(template_text)}")
    if user_info['text_template'] == 'custom' and user_info['custom_text']:
        custom_text_preview = user_info['custom_text'][:50] + ('...' if len(user_info['custom_text']) > 50 else '')
        text_lines.append(f"**Свой текст**: \"_{escape_markdown(custom_text_preview)}..._\"")
    
    text = "\n".join(text_lines)
    
    kb = []
    
    # Кнопки управления статусом (не для главного админа)
    if user_id != MAIN_ADMIN_ID:
        if user_info['is_banned']:
            kb.append([InlineKeyboardButton(text="✅ Разбанить", callback_data=f"adm_unban:{user_id}")])
        else:
            kb.append([
                InlineKeyboardButton(text="🚫 Бан навсегда", callback_data=f"adm_ban:{user_id}:0"),
                InlineKeyboardButton(text="⏱ Бан на время", callback_data=f"adm_ban_duration:{user_id}")
            ])
        
        if user_info['is_admin']:
            kb.append([InlineKeyboardButton(text="🔻 Снять админа", callback_data=f"adm_unadmin:{user_id}")])
        else:
            kb.append([InlineKeyboardButton(text="👑 Дать админа", callback_data=f"adm_admin_duration:{user_id}")])
    else:
        kb.append([InlineKeyboardButton(text="👑👑 Главный админ (недоступно)", callback_data="noop")])
    
    # Кнопки статистики
    kb.append([
        InlineKeyboardButton(text="📊 Статистика", callback_data=f"adm_user_stats:{user_id}"),
        InlineKeyboardButton(text="📈 График", callback_data=f"adm_user_graph:{user_id}")
    ])
    
    kb.append([
        InlineKeyboardButton(text="✉️ Написать", callback_data=f"adm_msg_user:{user_id}"),
        InlineKeyboardButton(text="🗑 Удалить", callback_data=f"adm_delete_user:{user_id}")
    ])
    
    kb.append([InlineKeyboardButton(text="🔙 К списку", callback_data="adm_users_list:0")])
    
    if is_callback: 
        await message.edit_text(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))
    else: 
        await message.answer(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))

@dp.callback_query(F.data.startswith("adm_ban_duration:"))
async def adm_set_ban_duration(call: CallbackQuery, state: FSMContext):
    if not await db.is_admin(call.from_user.id): 
        return
    
    await db.increment_session_actions(call.from_user.id)
    
    user_id = int(call.data.split(":")[1])
    if user_id == MAIN_ADMIN_ID:
        return await call.answer("❗️ Нельзя банить главного админа\\.", show_alert=True)
    
    await state.set_state(AdminInput.set_ban_duration)
    await state.update_data(target_user_id=user_id)
    
    # Используем HTML вместо Markdown для избежания проблем с экранированием
    await call.message.edit_text(
        f"⏱ <b>Установите время бана для <code>{user_id}</code></b>\n\n"
        "Введите время в секундах (например):\n"
        "• 3600 = 1 час\n"
        "• 7200 = 2 часа\n"
        "• 86400 = 1 день\n"
        "• 0 = навсегда\n\n"
        "Или используйте быстрые кнопки:",
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="1 час", callback_data=f"adm_ban:{user_id}:3600"),
             InlineKeyboardButton(text="3 часа", callback_data=f"adm_ban:{user_id}:10800")],
            [InlineKeyboardButton(text="1 день", callback_data=f"adm_ban:{user_id}:86400"),
             InlineKeyboardButton(text="7 дней", callback_data=f"adm_ban:{user_id}:604800")],
            [InlineKeyboardButton(text="Навсегда", callback_data=f"adm_ban:{user_id}:0")],
            [InlineKeyboardButton(text="✖️ Отмена", callback_data=f"adm_user_manage:{user_id}")]
        ])
    )

@dp.message(AdminInput.set_ban_duration)
async def adm_process_ban_duration(msg: types.Message, state: FSMContext):
    if not await db.is_admin(msg.from_user.id): 
        return
    
    await db.increment_session_actions(msg.from_user.id)
    
    data = await state.get_data()
    user_id = data['target_user_id']
    await state.clear()
    
    if not msg.text.isdigit():
        await msg.answer("❗️ *Ошибка: Введите число секунд\\.*")
        return
    
    duration = int(msg.text)
    await db.set_ban(user_id, True, duration)
    
    duration_text = format_duration(duration)
    await msg.answer(f"✅ *Пользователь `{user_id}` забанен на {duration_text}\\!*")
    await user_management_panel(msg, user_id)

@dp.callback_query(F.data.startswith("adm_admin_duration:"))
async def adm_set_admin_duration(call: CallbackQuery, state: FSMContext):
    if not await db.is_admin(call.from_user.id): 
        return
    
    await db.increment_session_actions(call.from_user.id)
    
    user_id = int(call.data.split(":")[1])
    if user_id == MAIN_ADMIN_ID:
        return await call.answer("❗️ Нельзя менять статус главного админа\\.", show_alert=True)
    
    await state.set_state(AdminInput.set_admin_duration)
    await state.update_data(target_user_id=user_id)
    
    # Используем HTML вместо Markdown для избежания проблем с экранированием
    await call.message.edit_text(
        f"👑 <b>Установите время админки для <code>{user_id}</code></b>\n\n"
        "Введите время в секундах (например):\n"
        "• 3600 = 1 час\n"
        "• 7200 = 2 часа\n"
        "• 86400 = 1 день\n"
        "• 604800 = 1 неделя\n\n"
        "Или используйте быстрые кнопки:",
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="1 час", callback_data=f"adm_admin:{user_id}:3600"),
             InlineKeyboardButton(text="3 часа", callback_data=f"adm_admin:{user_id}:10800")],
            [InlineKeyboardButton(text="20 минут", callback_data=f"adm_admin:{user_id}:1200"),
             InlineKeyboardButton(text="1 час 20 мин 3 сек", callback_data=f"adm_admin:{user_id}:4803")],
            [InlineKeyboardButton(text="1 день", callback_data=f"adm_admin:{user_id}:86400"),
             InlineKeyboardButton(text="7 дней", callback_data=f"adm_admin:{user_id}:604800")],
            [InlineKeyboardButton(text="✖️ Отмена", callback_data=f"adm_user_manage:{user_id}")]
        ])
    )

@dp.message(AdminInput.set_admin_duration)
async def adm_process_admin_duration(msg: types.Message, state: FSMContext):
    if not await db.is_admin(msg.from_user.id): 
        return
    
    await db.increment_session_actions(msg.from_user.id)
    
    data = await state.get_data()
    user_id = data['target_user_id']
    await state.clear()
    
    if not msg.text.isdigit():
        await msg.answer("❗️ *Ошибка: Введите число секунд\\.*")
        return
    
    duration = int(msg.text)
    await db.set_admin(user_id, True, duration)
    
    duration_text = format_duration(duration)
    await msg.answer(f"✅ *Пользователь `{user_id}` получил админку на {duration_text}\\!*")
    await user_management_panel(msg, user_id)

@dp.callback_query(F.data.startswith(("adm_ban:", "adm_unban:", "adm_admin:", "adm_unadmin:")))
async def adm_user_status_change(call: CallbackQuery):
    if not await db.is_admin(call.from_user.id): 
        return
    
    await db.increment_session_actions(call.from_user.id)
    
    action, user_id_str, *params = call.data.split(":")
    user_id = int(user_id_str)
    
    if user_id == MAIN_ADMIN_ID:
        return await call.answer("❗️ Нельзя менять статус главного админа\\.", show_alert=True)
    
    if action == "adm_ban":
        duration = int(params[0])
        await db.set_ban(user_id, True, duration)
        duration_text = format_duration(duration)
        await call.answer(f"🚫 Забанен на {duration_text}", show_alert=True)
    elif action == "adm_unban":
        await db.set_ban(user_id, False)
        await call.answer("✅ Разбанен", show_alert=True)
    elif action == "adm_admin":
        duration = int(params[0])
        await db.set_admin(user_id, True, duration)
        duration_text = format_duration(duration)
        await call.answer(f"👑 Админ выдан на {duration_text}", show_alert=True)
    elif action == "adm_unadmin":
        await db.set_admin(user_id, False)
        await call.answer("🔻 Админ снят", show_alert=True)
    
    await user_management_panel(call, user_id, is_callback=True)

@dp.callback_query(F.data.startswith("adm_user_stats:"))
async def adm_user_stats(call: CallbackQuery):
    if not await db.is_admin(call.from_user.id): 
        return
    
    await db.increment_session_actions(call.from_user.id)
    
    user_id = int(call.data.split(":")[1])
    stats = await db.get_user_search_stats(user_id)
    user_info = await db.get_user_info(user_id)
    
    if not stats or not user_info:
        await call.answer("❌ Статистика не найдена", show_alert=True)
        return
    
    username = f"@{user_info['username']}" if user_info['username'] else f"ID: {user_id}"
    
    text_lines = [
        f"📊 *Статистика пользователя {escape_markdown(username)}*",
        f"**ID**: `{user_id}`",
        f"**Имя**: {escape_markdown(user_info['first_name'])}",
        "",
        "📈 *Основная статистика:*",
        f"• Всего поисков: {stats['basic_stats'].get('total_searches', 0)}",
        f"• Всего найдено: {stats['basic_stats'].get('total_found', 0)}",
        f"• Любимый подарок: {escape_markdown(stats['basic_stats'].get('favorite_gift', 'Нет данных'))}",
    ]
    
    if stats['basic_stats'].get('last_search_date'):
        last_search = escape_markdown(stats['basic_stats']['last_search_date'].split('.')[0])
        text_lines.append(f"• Последний поиск: {last_search}")
    
    if stats.get('session_stats') and stats['session_stats'].get('session_count'):
        text_lines.extend([
            "",
            "💻 *Статистика сессий:*",
            f"• Всего сессий: {stats['session_stats'].get('session_count', 0)}",
            f"• Всего действий: {stats['session_stats'].get('total_actions', 0)}",
            f"• Среднее действий/сессия: {stats['session_stats'].get('avg_actions', 0):.1f}",
        ])
        if stats['session_stats'].get('total_minutes'):
            text_lines.append(f"• Всего минут в боте: {stats['session_stats'].get('total_minutes', 0):.1f}")
    
    if stats.get('top_gifts'):
        text_lines.extend([
            "",
            "🎁 *Топ подарков:*"
        ])
        for i, gift in enumerate(stats['top_gifts'][:5], 1):
            text_lines.append(f"{i}\\. {escape_markdown(gift['gift_name'][:20])} — {gift['search_count']} поисков")
    
    text = "\n".join(text_lines)
    
    kb = [
        [InlineKeyboardButton(text="📈 Показать график", callback_data=f"adm_user_graph:{user_id}")],
        [InlineKeyboardButton(text="🔙 Назад", callback_data=f"adm_user_manage:{user_id}")]
    ]
    
    await call.message.edit_text(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))

@dp.callback_query(F.data.startswith("adm_user_graph:"))
async def adm_user_graph(call: CallbackQuery):
    if not await db.is_admin(call.from_user.id): 
        return
    
    await db.increment_session_actions(call.from_user.id)
    
    user_id = int(call.data.split(":")[1])
    user_info = await db.get_user_info(user_id)
    
    if not user_info:
        await call.answer("❌ Пользователь не найден", show_alert=True)
        return
    
    # Генерация графика
    graph_data = await db.get_user_graph_data(user_id)
    
    if not graph_data:
        await call.answer("❌ Недостаточно данных для графика", show_alert=True)
        return
    
    # Отправка графика
    username = f"@{user_info['username']}" if user_info['username'] else f"ID: {user_id}"
    caption = f"📈 *График статистики пользователя {escape_markdown(username)}*\n`ID: {user_id}`"
    
    # Сохраняем график во временный файл
    temp_file = f"{GRAPHS_DIR}/graph_{user_id}_{datetime.now().timestamp()}.png"
    with open(temp_file, 'wb') as f:
        f.write(graph_data)
    
    try:
        with open(temp_file, 'rb') as photo:
            await bot.send_photo(
                chat_id=call.message.chat.id,
                photo=BufferedInputFile(photo.read(), filename=f"graph_{user_id}.png"),
                caption=caption,
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="🔙 Назад", callback_data=f"adm_user_manage:{user_id}")]
                ])
            )
        # Удалить временный файл
        os.remove(temp_file)
        
        # Удалить исходное сообщение
        await call.message.delete()
        
    except Exception as e:
        logger.error(f"Error sending graph: {e}")
        await call.answer("❌ Ошибка при создании графика", show_alert=True)
        await call.message.edit_text(
            f"❌ *Ошибка при создании графика:*\n`{e}`",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🔙 Назад", callback_data=f"adm_user_manage:{user_id}")]
            ])
        )

@dp.callback_query(F.data.startswith("adm_delete_user:"))
async def adm_delete_user(call: CallbackQuery):
    if not await db.is_admin(call.from_user.id): 
        return
    
    await db.increment_session_actions(call.from_user.id)
    
    user_id = int(call.data.split(":")[1])
    
    if user_id == MAIN_ADMIN_ID:
        return await call.answer("❗️ Нельзя удалить главного админа\\.", show_alert=True)
    
    await call.message.edit_text(
        f"🗑 *Вы уверены, что хотите удалить пользователя `{user_id}`?*\n\n"
        "⚠️ *Внимание:* Это действие необратимо\\. "
        "Удалятся все данные пользователя, включая статистику\\.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✅ Да, удалить", callback_data=f"adm_confirm_delete:{user_id}"),
             InlineKeyboardButton(text="❌ Нет, отмена", callback_data=f"adm_user_manage:{user_id}")]
        ])
    )

@dp.callback_query(F.data.startswith("adm_confirm_delete:"))
async def adm_confirm_delete_user(call: CallbackQuery):
    if not await db.is_admin(call.from_user.id): 
        return
    
    await db.increment_session_actions(call.from_user.id)
    
    user_id = int(call.data.split(":")[1])
    
    if user_id == MAIN_ADMIN_ID:
        return await call.answer("❗️ Нельзя удалить главного админа\\.", show_alert=True)
    
    async with aiosqlite.connect(DB_NAME, timeout=30) as conn:
        # Удалить пользователя и связанные данные
        await conn.execute('DELETE FROM users WHERE user_id = ?', (user_id,))
        await conn.execute('DELETE FROM user_stats WHERE user_id = ?', (user_id,))
        await conn.execute('DELETE FROM gift_search_counts WHERE user_id = ?', (user_id,))
        await conn.execute('DELETE FROM user_sessions WHERE user_id = ?', (user_id,))
        await conn.commit()
    
    await call.answer(f"✅ Пользователь {user_id} удален", show_alert=True)
    await adm_users_list(call)

@dp.callback_query(F.data.startswith("adm_msg_user:"))
async def adm_direct_message_start(call: CallbackQuery, state: FSMContext):
    if not await db.is_admin(call.from_user.id): 
        return
    
    await db.increment_session_actions(call.from_user.id)
    
    user_id = int(call.data.split(":")[1])
    await state.set_state(AdminInput.user_msg_direct)
    await state.update_data(target_user_id=user_id)
    await call.message.edit_text(f"✍️ *Введите сообщение для `{user_id}`:*", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="✖️ Отмена", callback_data=f"adm_user_manage:{user_id}")]]))

@dp.message(AdminInput.user_msg_direct)
async def adm_direct_message_process(msg: types.Message, state: FSMContext):
    if not await db.is_admin(msg.from_user.id): 
        return
    
    await db.increment_session_actions(msg.from_user.id)
    
    data = await state.get_data()
    user_id = data['target_user_id']
    await state.clear()
    
    try:
        await bot.send_message(user_id, f"📮 *Сообщение от админа:*\n\n{msg.text}")
        await msg.answer("✅ *Сообщение отправлено\\!*")
    except Exception as e: 
        await msg.answer(f"❌ *Ошибка отправки:*\n`{e}`")
    
    await user_management_panel(msg, user_id)

# --- Общая статистика бота ---
@dp.callback_query(F.data == "adm_bot_stats")
async def adm_bot_stats(call: CallbackQuery):
    if not await db.is_admin(call.from_user.id): 
        return
    
    await db.increment_session_actions(call.from_user.id)
    
    async with aiosqlite.connect(DB_NAME, timeout=30) as conn:
        conn.row_factory = aiosqlite.Row
        
        # Общая статистика пользователей
        async with conn.execute('SELECT COUNT(*) as total_users FROM users') as cursor:
            total_users = (await cursor.fetchone())['total_users']
        
        # Используем таблицу user_stats для активных пользователей
        async with conn.execute('SELECT COUNT(*) as active_users FROM user_stats WHERE last_search_date IS NOT NULL') as cursor:
            active_users = (await cursor.fetchone())['active_users']
        
        async with conn.execute('SELECT COUNT(*) as banned_users FROM users WHERE is_banned = 1') as cursor:
            banned_users = (await cursor.fetchone())['banned_users']
        
        async with conn.execute('SELECT COUNT(*) as admin_users FROM users WHERE is_admin = 1') as cursor:
            admin_users = (await cursor.fetchone())['admin_users']
        
        # Статистика поисков
        async with conn.execute('SELECT SUM(total_searches) as total_searches, SUM(total_found) as total_found FROM user_stats') as cursor:
            search_stats = await cursor.fetchone()
            total_searches = search_stats['total_searches'] or 0
            total_found = search_stats['total_found'] or 0
        
        # Топ активных пользователей
        async with conn.execute('''
            SELECT u.user_id, u.username, u.first_name, us.total_searches, us.total_found
            FROM user_stats us
            JOIN users u ON us.user_id = u.user_id
            ORDER BY us.total_searches DESC
            LIMIT 5
        ''') as cursor:
            top_users = [dict(row) for row in await cursor.fetchall()]
        
        # Топ популярных подарков
        async with conn.execute('''
            SELECT gift_name, SUM(search_count) as total_searches
            FROM gift_search_counts
            GROUP BY gift_name
            ORDER BY total_searches DESC
            LIMIT 5
        ''') as cursor:
            top_gifts = [dict(row) for row in await cursor.fetchall()]
    
    # Собираем текст БЕЗ Markdown форматирования
    text_lines = [
        "📊 Общая статистика бота",
        "",
        "👥 Пользователи:",
        f"• Всего пользователей: {total_users}",
        f"• Активных пользователей: {active_users}",
        f"• Забанено: {banned_users}",
        f"• Админов: {admin_users}",
        "",
        "🔍 Поиски:",
        f"• Всего поисков: {total_searches}",
        f"• Всего найдено: {total_found}",
        f"• Эффективность: {(total_found/total_searches*100 if total_searches > 0 else 0):.1f}%",
    ]
    
    if top_users:
        text_lines.extend([
            "",
            "🏆 Топ активных пользователей:"
        ])
        for i, user in enumerate(top_users, 1):
            username = f"@{user['username']}" if user['username'] else f"ID: {user['user_id']}"
            name = user['first_name'] or ''
            # Экранируем только точки, которые могут быть в именах
            name_escaped = name.replace('.', '․')  # Используем другую точку или просто удаляем
            text_lines.append(f"{i}. {username} ({name_escaped}) — {user['total_searches']} поисков, {user['total_found']} найдено")
    
    if top_gifts:
        text_lines.extend([
            "",
            "🎁 Топ популярных подарков:"
        ])
        for i, gift in enumerate(top_gifts, 1):
            text_lines.append(f"{i}. {gift['gift_name']} — {gift['total_searches']} поисков")
    
    text = "\n".join(text_lines)
    
    kb = [
        [InlineKeyboardButton(text="🔄 Обновить", callback_data="adm_bot_stats")],
        [InlineKeyboardButton(text="🔙 Назад", callback_data="admin_home")]
    ]
    
    # Отправляем с отключенным Markdown
    await call.message.edit_text(
        text, 
        reply_markup=InlineKeyboardMarkup(inline_keyboard=kb),
        parse_mode=None  # Отключаем Markdown полностью
    )

# ==================== MAIN ====================
async def main():
    await db.init_db()
    await finds_logger.init_db()
    dp.shutdown.register(parser.close_session)
    
    # Завершение сессий при завершении работы
    @dp.shutdown()
    async def on_shutdown():
        logger.info("Завершаю все пользовательские сессии...")
        async with aiosqlite.connect(DB_NAME, timeout=30) as conn:
            await conn.execute('UPDATE user_sessions SET session_end = CURRENT_TIMESTAMP WHERE session_end IS NULL')
            await conn.commit()
        logger.info("Все сессии завершены")
    
    try:
        logger.info("Bot starting polling...")
        await dp.start_polling(bot)
    finally:
        await bot.session.close()
        logger.info("Bot stopped.")

if __name__ == '__main__':
    try: 
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit): 

        logger.info("Shutdown signal received.")
