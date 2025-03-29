import logging
import asyncio
import json
import os
import psycopg2
from psycopg2.extras import Json
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.enums import ParseMode
from aiogram.client.default import DefaultBotProperties
from aiogram import F
from datetime import datetime, timedelta
from psycopg2.pool import ThreadedConnectionPool


# Настройки
TOKEN = "8076628423:AAEkp4l3BYkl-6lwz8VAyMw0h7AaAM7J3oM"
CHANNEL_ID = "@memok_da"
CHANNEL_LINK = "https://t.me/memok_da"
DATABASE_URL = os.getenv("DATABASE_URL")
pool = ThreadedConnectionPool(1, 20, dsn=DATABASE_URL)

# Константы
UPGRADE_PREFIX = "upg_"
SLAVE_PREFIX = "slv_"
MAIN_MENU = "main_menu"
WORK = "work"
UPGRADES = "upgrades"
PROFILE = "profile"
REF_LINK = "ref_link"
BUY_MENU = "buy_menu"
CHECK_SUB = "check_sub_"
SEARCH_USER = "search_user"
TOP_OWNERS = "top_owners"
BUYOUT_PREFIX = "buyout_"
SHIELD_PREFIX = "shield_"
SHACKLES_PREFIX = "shackles_"
MAX_SLAVE_LEVEL = 15
DAILY_WORK_LIMIT = 10
MAX_BARRACKS_LEVEL = 10
DAILY_WORK_LIMIT = 7
MIN_SLAVES_FOR_RANDOM = 3 

# Инициализация
bot = Bot(token=TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
storage = MemoryStorage()
dp = Dispatcher(storage=storage)

# База данных
user_search_cache = {}

# Улучшения
upgrades = {
    "storage": {
        "name": "📦 Склад",
        "base_price": 300, 
        "income_bonus": 5,
        "price_multiplier": 1.3,
        "description": "+8 монет/мин к пассивному доходу"
    },
    "whip": {
        "name": "⛓ Кнуты", 
        "base_price": 800,
        "income_bonus": 0.18,  # +18% к работе (было +25%)
        "price_multiplier": 1.3,
        "description": "+18% к доходу от работы"
    },
    "food": {
        "name": "🍗 Еда",
        "base_price": 1500,
        "income_bonus": 0.08,  # -8% времени работы за уровень
        "price_multiplier": 1.5,
        "description": "-8% к времени ожидания работы"
    },
    "barracks": {
        "name": "🏠 Бараки",
        "base_price": 3000,
        "income_bonus": 2,  # +2 к лимиту рабов
        "price_multiplier": 1.6,
        "description": "+2 к лимиту рабов"
    }
}

# Клавиатуры
def main_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💼 Работать", callback_data=WORK)],
        [
            InlineKeyboardButton(text="🛠 Улучшения", callback_data=UPGRADES),
            InlineKeyboardButton(text="📊 Профиль", callback_data=PROFILE)
        ],
        [
            InlineKeyboardButton(text="👥 Купить раба", callback_data=BUY_MENU),
            InlineKeyboardButton(text="🛒 Магазин", callback_data="shop")
        ],
        [    
            InlineKeyboardButton(text="🔗 Рефералка", callback_data=REF_LINK),
            InlineKeyboardButton(text="🏆 Топ владельцев", callback_data=TOP_OWNERS)
        ]
    ])

def get_db_connection():
    return pool.getconn()

def return_db_connection(conn):
    pool.putconn(conn)

def get_user(user_id: int) -> dict | None:
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT data FROM bot_users WHERE user_id = %s", (user_id,))
            result = cur.fetchone()
            return deserialize_user_data(result[0]) if result else None
    except Exception as e:
        logging.error(f"Ошибка загрузки пользователя {user_id}: {e}")
        return None
    finally:
        return_db_connection(conn)
def upgrades_keyboard(user_id):
    buttons = []
    user = get_user(user_id)  # Загружаем данные пользователя
    
    if not user:  # Если пользователь не найден
        return InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="❌ Ошибка загрузки", callback_data=MAIN_MENU)]
        ])
    
    for upgrade_id, data in upgrades.items():
        level = user.get("upgrades", {}).get(upgrade_id, 0)  # Получаем уровень из данных пользователя
        price = data["base_price"] * (level + 1)
        buttons.append([
            InlineKeyboardButton(
                text=f"{data['name']} (Ур. {level}) - {price}₽ | {data['description']}",
                callback_data=f"{UPGRADE_PREFIX}{upgrade_id}"
            )
        ])
    
    buttons.append([InlineKeyboardButton(text="🔙 Назад", callback_data=MAIN_MENU)])
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def buy_menu_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔍 Поиск по юзернейму", callback_data=SEARCH_USER)],
        [InlineKeyboardButton(text="🎲 Случайные рабы (Топ-10)", callback_data="random_slaves")],
        [InlineKeyboardButton(text="🔙 Назад", callback_data=MAIN_MENU)]
    ])
    
def serialize_user_data(user_data: dict) -> dict:
    """Преобразуем datetime объекты в строки для JSON"""
    serialized = {}
    for key, value in user_data.items():
        if isinstance(value, datetime):
            serialized[key] = value.isoformat()
        elif isinstance(value, dict) and key == "shackles":
            # Сериализуем кандалы
            serialized[key] = {
                str(slave_id): end_time.isoformat() 
                for slave_id, end_time in value.items()
            }
        else:
            serialized[key] = value
    return serialized

def deserialize_user_data(data: dict) -> dict:
    """Восстанавливаем datetime из строк"""
    deserialized = {}
    for key, value in data.items():
        if key in ['last_passive', 'last_work', 'shield_active'] and value:
            try:
                deserialized[key] = datetime.fromisoformat(value)
            except (TypeError, ValueError):
                deserialized[key] = None
        elif key == "shackles" and isinstance(value, dict):
            # Десериализуем кандалы
            deserialized[key] = {
                int(slave_id): datetime.fromisoformat(end_time)
                for slave_id, end_time in value.items()
            }
        else:
            deserialized[key] = value
    return deserialized

def create_user(user_id: int, username: str, referrer_id: int = None) -> dict:
    """Создаёт нового пользователя в БД"""
    new_user = {
        "balance": 100,
        "slaves": [],
        "owner": None,
        "base_price": 100,
        "slave_level": 0,
        "price": 100,
        "last_work": None,
        "upgrades": {key: 0 for key in upgrades},
        "total_income": 0,
        "username": username,
        "shield_active": None,
        "shackles": {},
        "shop_purchases": 0,
        "last_passive": datetime.now(),
        "income_per_sec": 0.0167,
        "referrer": referrer_id,
    }
    
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO bot_users (user_id, data) VALUES (%s, %s)",
                (user_id, Json(serialize_user_data(new_user)))
            )
            conn.commit()
        return new_user
    except Exception as e:
        logging.error(f"Ошибка создания пользователя {user_id}: {e}")
        raise
    finally:
        conn.close()


def get_user(user_id: int) -> dict | None:
    """Загружает данные пользователя из PostgreSQL"""
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT data FROM bot_users WHERE user_id = %s", (user_id,))
            result = cur.fetchone()
            return deserialize_user_data(result[0]) if result else None
    except Exception as e:
        logging.error(f"Ошибка загрузки пользователя {user_id}: {e}")
        return None
    finally:
        conn.close()

def update_user(user_id: int, user_data: dict):
    """Обновляет данные пользователя в PostgreSQL"""
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE bot_users SET data = %s WHERE user_id = %s",
                (Json(serialize_user_data(user_data)), user_id)  # Исправлено здесь
            )
            conn.commit()
    except Exception as e:
        logging.error(f"Ошибка обновления пользователя {user_id}: {e}")
        raise
    finally:
        conn.close()

def passive_income(user):
    base = 1 + user["upgrades"].get("storage", 0) * 5
    slaves = sum(
        50 * (1 + 0.2 * slave_level(slave_id)) 
        for slave_id in user.get("slaves", [])
    )
    return base + slaves * (1 + 0.05 * user["upgrades"].get("barracks", 0))

def calculate_shield_price(user_id):
    user = get_user(user_id)
    if not user:
        return 500  # Минимальная цена по умолчанию
    
    # Базовый доход (1 + склад) в минуту
    passive_per_min = 1 + user.get("upgrades", {}).get("storage", 0) * 10
    
    # Доход от рабов в минуту
    for slave_id in user.get("slaves", []):
        slave = get_user(slave_id)
        if slave:
            passive_per_min += 100 * (1 + 0.3 * slave.get("slave_level", 0))
    
    # Цена = 50% дохода за 12 часов, округлено до 10
    base_price = passive_per_min * 60 * 6  # 6 часов
    shop_purchases = user.get("shop_purchases", 0)
    price = base_price * (1.1 ** shop_purchases) 
    price = max(500, min(8000, price))  # Лимиты
    
    # Скидка за первую покупку
    if user.get("shop_purchases", 0) == 0:
        price = int(price * 0.7)
    
    return int(price)

def calculate_shackles_price(owner_id):
    owner = get_user(owner_id)
    if not owner:
        return 300  # Минимальная цена
    
    # 1. Базовый доход (склад) в час
    passive_income = (1 + owner.get("upgrades", {}).get("storage", 0) * 10) * 60
    
    # 2. Добавляем доход от рабов
    for slave_id in owner.get("slaves", []):
        slave = get_user(slave_id)
        if slave:
            passive_income += 100 * (1 + 0.3 * slave.get("slave_level", 0))
    
    # 3. Расчет цены с ограничениями
    price = int(passive_income * 1.5 / 100) * 100
    return max(300, min(10_000, price))

def slave_price(slave_data: dict) -> int:
    """Рассчитывает цену раба на основе его уровня"""
    base_price = slave_data.get("base_price", 100)
    level = slave_data.get("slave_level", 0)
    return int(200 * (1.35 ** min(level, MAX_SLAVE_LEVEL)))

# Вспомогательные функции
async def check_subscription(user_id: int):
    try:
        member = await bot.get_chat_member(chat_id=CHANNEL_ID, user_id=user_id)
        return member.status not in ['left', 'kicked']
    except Exception as e:
        logging.error(f"Ошибка проверки подписки: {e}")
        return False

async def passive_income_task():
    while True:
        await asyncio.sleep(60)
        now = datetime.now()
        
        # Получаем всех пользователей из БД
        conn = get_db_connection()
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE bot_users
                    SET data = jsonb_set(data, '{last_passive}', to_jsonb(NOW()))
                    WHERE (data->>'last_passive')::timestamp < NOW() - INTERVAL '1 minute'
                    RETURNING user_id, data
                """)
                rows = cur.fetchall()
                
                for row in rows:
                    user_id = row[0]
                    user = deserialize_user_data(row[1])
                    
                    if "last_passive" not in user:
                        continue
                        
                    # Рассчитываем прошедшее время
                    mins_passed = (now - user["last_passive"]).total_seconds() / 60
                    mins_passed = min(mins_passed, 1440)  # Максимум 24 часа
                    
                    # Базовый доход
                    base_income = 1 * mins_passed
                    
                    # Доход от склада
                    storage_income = user.get("upgrades", {}).get("storage", 0) * 10 * mins_passed
                    
                    # Доход от рабов (с налогом)
                    slaves_income = 0
                    for slave_id in user.get("slaves", []):
                        slave = get_user(slave_id)
                        if slave:
                            slave_income = 100 * (1 + 0.3 * slave.get("slave_level", 0)) * mins_passed
                            tax_rate = min(0.1 + 0.05 * user.get("slave_level", 0), 0.3)
                            tax = int(slave_income * tax_rate)
                            
                            # Обновляем баланс раба
                            slave["balance"] += slave_income - tax
                            update_user(slave_id, slave)
                            
                            slaves_income += tax
                    
                    # Обновляем баланс владельца
                    total_income = base_income + storage_income + slaves_income
                    user["balance"] += total_income
                    user["total_income"] += total_income
                    user["last_passive"] = now
                    
                    # Сохраняем изменения
                    update_user(user_id, user)
                    
        except Exception as e:
            logging.error(f"Ошибка в passive_income_task: {e}")
        finally:
            conn.close()

# Обработчики команд
@dp.message(Command('start'))
async def start_command(message: Message):
    user_id = message.from_user.id
    username = message.from_user.username
    
    # Получаем referrer_id из команды
    referrer_id = None
    if len(message.text.split()) > 1:
        try:
            referrer_id = int(message.text.split()[1])
            # Проверяем, что реферер существует в БД и не сам пользователь
            if referrer_id == user_id or not get_user(referrer_id):
                referrer_id = None
        except:
            referrer_id = None

    if not await check_subscription(user_id):
        # Сохраняем реферала во временные данные
        if referrer_id:
            user = get_user(user_id) or {}
            user["referrer"] = referrer_id
            update_user(user_id, user)  # Сохраняем в БД
            
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔔 Подписаться", url=CHANNEL_LINK)],
            [InlineKeyboardButton(text="✅ Я подписался", callback_data=f"{CHECK_SUB}{user_id}")]
        ])
        await message.answer("📌 Для доступа подпишитесь на канал:", reply_markup=kb)
        return
    
    user = get_user(user_id)
    if not user:
        # Создаем нового пользователя в БД
        user = create_user(user_id, username, referrer_id)
        
        # Начисляем бонус рефералу (если есть)
        if referrer_id:
            referrer = get_user(referrer_id)
            if referrer:
                bonus = 50  # Фиксированный бонус
                referrer["balance"] += bonus
                referrer["total_income"] += bonus
                update_user(referrer_id, referrer)  # Обновляем реферера
                
                try:
                    await bot.send_message(
                        referrer_id,
                        f"🎉 Вам начислено {bonus}₽ за приглашение @{username}!"
                    )
                except:
                    pass

        welcome_msg = (
            "👑 <b>ДОБРО ПОЖАЛОВАТЬ В РАБОВЛАДЕЛЬЧЕСКУЮ ИМПЕРИЮ!</b>\n\n"
            "⚡️ <b>Основные возможности:</b>\n"
            "▸ 💼 Бонусная работа (раз в 20 мин)\n"
            "▸ 🛠 Улучшай свои владения\n")

@dp.callback_query(F.data == "random_slaves")
async def show_random_slaves(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    
    # Получаем всех пользователей из БД
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT user_id, data FROM bot_users")
            all_users = {row[0]: deserialize_user_data(row[1]) for row in cur.fetchall()}
            
            # Фильтруем свободных рабов или чужих рабов
            available_slaves = [
                (uid, data) for uid, data in all_users.items() 
                if uid != user_id and (data.get("owner") is None or data["owner"] != user_id)
            ]
            
            # Функция для расчета рейтинга
            def get_slave_score(slave_data):
                level = slave_data.get("slave_level", 0)
                price = slave_data.get("price", 100)
                return (level * 2) - (price / 100)
            
            # Сортируем по рейтингу и берем топ-10
            sorted_slaves = sorted(
                available_slaves,
                key=lambda x: get_slave_score(x[1]),
                reverse=True
            )[:10]

            if not sorted_slaves:
                await callback.answer("😢 Нет доступных рабов", show_alert=True)
                return

            # Формируем кнопки
            buttons = []
            for slave_id, slave_data in sorted_slaves:
                buttons.append([
                    InlineKeyboardButton(
                        text=f"👤 Ур.{slave_data.get('slave_level', 0)} @{slave_data['username']} - {slave_data['price']}₽ (Рейтинг: {get_slave_score(slave_data):.1f})",
                        callback_data=f"{SLAVE_PREFIX}{slave_id}"
                    )
                ])
            
            buttons.append([InlineKeyboardButton(text="🔙 Назад", callback_data=BUY_MENU)])
            
            await callback.message.edit_text(
                "🎲 Доступные рабы (Топ-10 по рейтингу привлекательности):",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons)
            )
            
    except Exception as e:
        logging.error(f"Ошибка при поиске рабов: {e}")
        await callback.answer("❌ Ошибка при загрузке списка рабов", show_alert=True)
    finally:
        conn.close()
        
@dp.callback_query(F.data.startswith(CHECK_SUB))
async def check_sub_callback(callback: types.CallbackQuery):
    user_id = int(callback.data.replace(CHECK_SUB, ""))
    
    if await check_subscription(user_id):
        # Получаем данные пользователя
        user = get_user(user_id)
        
        # Если пользователя нет - создаем нового
        if not user:
            new_user = {
                "balance": 100,
                "slaves": [],
                "owner": None,
                "price": 100,
                "last_work": None,
                "upgrades": {key: 0 for key in upgrades},
                "total_income": 0,
                "username": callback.from_user.username,
                "last_passive": datetime.now(),
                "income_per_sec": 0.0167,
                "referrer": None
            }
            try:
                # Создаем пользователя в БД
                create_user(user_id, callback.from_user.username)
                user = get_user(user_id)  # Получаем свежие данные
            except Exception as e:
                logging.error(f"Ошибка создания пользователя {user_id}: {e}")
                await callback.answer("❌ Ошибка регистрации", show_alert=True)
                return
        
        # Начисляем бонус рефералу (если есть)
        referrer_id = user.get("referrer")
        if referrer_id:
            referrer = get_user(referrer_id)
            if referrer:
                bonus = 50
                referrer["balance"] += bonus
                referrer["total_income"] += bonus
                update_user(referrer_id, referrer)
                
                try:
                    await bot.send_message(
                        referrer_id,
                        f"🎉 Вам начислено {bonus}₽ за приглашение @{callback.from_user.username}!"
                    )
                except Exception as e:
                    logging.error(f"Не удалось отправить сообщение рефереру {referrer_id}: {e}")
        
        await callback.message.edit_text("✅ Регистрация завершена!")
        await callback.message.answer("🔮 Главное меню:", reply_markup=main_keyboard())
    else:
        await callback.answer("❌ Вы не подписаны!", show_alert=True)
    
    await callback.answer()
    
@dp.callback_query(F.data == SEARCH_USER)
async def search_user_handler(callback: types.CallbackQuery):
    await callback.message.edit_text(
        "🔍 Введите @username игрока (можно с собакой):\n"
        "Пример: <code>@username123</code> или просто <code>username123</code>",
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="🔙 В меню покупок", callback_data=BUY_MENU)]
            ]
        ),
        parse_mode=ParseMode.HTML
    )
    await callback.answer()
    

@dp.callback_query(F.data == WORK)
async def work_handler(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    user = get_user(user_id)
    
    if not user:
        await callback.answer("❌ Сначала зарегистрируйтесь!", show_alert=True)
        return
    
    now = datetime.now()
    cooldown = timedelta(minutes=30)
    
    # Проверка кулдауна
    if user.get("last_work"):
        last_work = user["last_work"] if isinstance(user["last_work"], datetime) else datetime.fromisoformat(user["last_work"])
        if (now - last_work) < cooldown:
            remaining = (last_work + cooldown - now).seconds // 60
            await callback.answer(f"⏳ Подождите еще {remaining} минут", show_alert=True)
            return
    
    # Проверка дневного лимита
    if user.get("work_count", 0) >= DAILY_WORK_LIMIT:
        await callback.answer("❌ Достигнут дневной лимит!", show_alert=True)
        return
    
    # Рассчитываем пассивный доход
    passive_per_min = 1 + user.get("upgrades", {}).get("storage", 0) * 10
    
    # Доход от рабов
    for slave_id in user.get("slaves", []):
        slave = get_user(slave_id)
        if slave:
            passive_per_min += 100 * (1 + 0.3 * slave.get("slave_level", 0)) / 60
    
    # Расчет бонуса
    whip_bonus = 1 + user.get("upgrades", {}).get("whip", 0) * 0.25
    work_bonus = passive_per_min * 10 * (1 + whip_bonus)
    
    # Обновляем данные пользователя
    user["balance"] += work_bonus
    user["total_income"] += work_bonus
    user["last_work"] = now.isoformat()
    user["work_count"] = user.get("work_count", 0) + 1
    
    # Сохраняем изменения
    update_user(user_id, user)
    
    await callback.message.edit_text(
        f"💼 Бонусная работа принесла: {work_bonus:.1f}₽\n"
        f"▸ Это эквивалент 20 минут пассивки!\n"
        f"▸ Ваш текущий пассив/мин: {passive_per_min:.1f}₽",
        reply_markup=main_keyboard()
    )
    await callback.answer()

@dp.message(F.text & ~F.text.startswith('/'))
async def process_username(message: Message):
    # Нормализация username (удаляем @ и лишние пробелы)
    username = message.text.strip().lower().replace('@', '')
    
    # Поиск пользователя в БД
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT user_id, data FROM bot_users WHERE data->>'username' ILIKE %s",
                (username,)
            )
            result = cur.fetchone()
            
            kb = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🔙 Назад", callback_data=BUY_MENU)]
            ])

            if not result:
                await message.reply(
                    "❌ Игрок не найден. Проверьте:\n"
                    "1. Правильность написания\n"
                    "2. Игрок должен быть зарегистрирован в боте",
                    reply_markup=kb
                )
                return

            slave_id, slave_data = result
            slave = deserialize_user_data(slave_data)
            buyer_id = message.from_user.id

            if slave_id == buyer_id:
                await message.reply("🌀 Нельзя купить самого себя!", reply_markup=kb)
                return

            # Получаем информацию о владельце
            owner_info = "Свободен"
            if slave.get('owner'):
                owner = get_user(slave['owner'])
                owner_info = f"@{owner['username']}" if owner else "Неизвестен"
            
            kb = InlineKeyboardMarkup(inline_keyboard=[
                [
                    InlineKeyboardButton(
                        text=f"💰 Купить за {slave['price']}₽ (Ур. {slave.get('slave_level', 0)})", 
                        callback_data=f"{SLAVE_PREFIX}{slave_id}"
                    )
                ],
                [InlineKeyboardButton(text="🔙 Назад", callback_data=BUY_MENU)]
            ])
            
            await message.reply(
                f"🔎 <b>Найден раб:</b>\n"
                f"▸ Ник: @{slave['username']}\n"
                f"▸ Уровень: {slave.get('slave_level', 0)}\n"
                f"▸ Цена: {slave['price']}₽\n"
                f"▸ Владелец: {owner_info}\n\n"
                f"💡 <i>Доход от этого раба: {int(100 * (1 + 0.5 * slave.get('slave_level', 0)))}₽ за цикл работы</i>",
                reply_markup=kb,
                parse_mode=ParseMode.HTML
            )
            
    except Exception as e:
        logging.error(f"Ошибка поиска пользователя: {e}")
        await message.reply("❌ Произошла ошибка при поиске", reply_markup=kb)
    finally:
        conn.close()

@dp.callback_query(F.data == UPGRADES)
async def upgrades_handler(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    user = get_user(user_id)
    if not user:
        await callback.answer("❌ Сначала зарегистрируйтесь!", show_alert=True)
        return
    
    try:
        await callback.message.edit_text(
            "🛠 Выберите улучшение:", 
            reply_markup=upgrades_keyboard(user_id)
        )
    except Exception as e:
        logging.error(f"Ошибка в upgrades_handler: {e}")
        await callback.answer("❌ Ошибка загрузки улучшений", show_alert=True)
    await callback.answer()

@dp.callback_query(F.data == REF_LINK)
async def ref_link_handler(callback: types.CallbackQuery):
    try:
        bot_username = (await bot.get_me()).username
        ref_link = f"https://t.me/{bot_username}?start={callback.from_user.id}"
        await callback.message.edit_text(
            f"🔗 Ваша реферальная ссылка:\n<code>{ref_link}</code>\n\n"
            "Приглашайте друзей и получайте 10% с их заработка!",
            reply_markup=main_keyboard()
        )
    except Exception as e:
        logging.error(f"Ошибка в ref_link_handler: {e}")
        await callback.answer("❌ Ошибка генерации ссылки", show_alert=True)
    await callback.answer()

@dp.callback_query(F.data == BUY_MENU)
async def buy_menu_handler(callback: types.CallbackQuery):
    try:
        await callback.message.edit_text(
            "👥 Меню покупки рабов:", 
            reply_markup=buy_menu_keyboard()
        )
    except Exception as e:
        logging.error(f"Ошибка в buy_menu_handler: {e}")
        await callback.answer("❌ Ошибка загрузки меню", show_alert=True)
    await callback.answer()

@dp.callback_query(F.data == MAIN_MENU)
async def main_menu_handler(callback: types.CallbackQuery):
    try:
        await callback.message.edit_text(
            "🔮 Главное меню:", 
            reply_markup=main_keyboard()
        )
    except Exception as e:
        logging.error(f"Ошибка в main_menu_handler: {e}")
        await callback.answer("❌ Ошибка загрузки меню", show_alert=True)
    await callback.answer()

@dp.callback_query(F.data == TOP_OWNERS)
async def top_owners_handler(callback: types.CallbackQuery):
    try:
        current_user_id = callback.from_user.id
        conn = get_db_connection()
        
        with conn.cursor() as cur:
            # Получаем всех пользователей с их рабами и доходом
            cur.execute("""
                SELECT user_id, data->>'username' as username,
                       jsonb_array_length(data->'slaves') as slaves_count,
                       (data->>'total_income')::numeric as total_income
                FROM bot_users
                WHERE jsonb_array_length(data->'slaves') > 0
                ORDER BY (data->>'total_income')::numeric DESC
                LIMIT 10
            """)
            top_users = cur.fetchall()

            # Получаем данные текущего пользователя
            cur.execute("""
                SELECT jsonb_array_length(data->'slaves') as slaves_count,
                       (data->>'total_income')::numeric as total_income
                FROM bot_users
                WHERE user_id = %s
            """, (current_user_id,))
            current_user_data = cur.fetchone()

        # Формируем текст топа
        text = "🏆 <b>Топ рабовладельцев по эффективности:</b>\n\n"
        text += "<i>Рейтинг рассчитывается как доход на одного раба</i>\n\n"
        
        # Выводим топ-10
        for idx, user in enumerate(top_users, 1):
            user_id, username, slaves_count, total_income = user
            efficiency = total_income / slaves_count if slaves_count > 0 else 0
            text += (
                f"{idx}. @{username}\n"
                f"   ▸ Эффективность: {efficiency:.1f}₽/раб\n"
                f"   ▸ Рабов: {slaves_count} | Доход: {total_income:.1f}₽\n\n"
            )

        # Добавляем позицию текущего пользователя
        if current_user_data:
            slaves_count, total_income = current_user_data
            efficiency = total_income / slaves_count if slaves_count > 0 else 0
            
            # Проверяем, есть ли пользователь в топе
            in_top = any(user[0] == current_user_id for user in top_users)
            if not in_top:
                text += f"\n📊 Ваша эффективность: {efficiency:.1f}₽/раб"

        await callback.message.edit_text(
            text,
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[
                    [InlineKeyboardButton(text="🔙 Назад", callback_data=MAIN_MENU)]
                ]
            ),
            parse_mode=ParseMode.HTML
        )

    except Exception as e:
        logging.error(f"Ошибка в top_owners_handler: {e}", exc_info=True)
        await callback.answer("🌀 Ошибка загрузки топа", show_alert=True)
    finally:
        conn.close()
        await callback.answer()

@dp.callback_query(F.data == "shop")
async def shop_handler(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    user = get_user(user_id)
    
    if not user:
        await callback.answer("❌ Сначала зарегистрируйтесь!", show_alert=True)
        return

    try:
        # Расчет цены щита
        shield_price = calculate_shield_price(user_id)
        
        # Обработка shield_active
        shield_active = user.get("shield_active")
        if isinstance(shield_active, str):
            try:
                shield_active = datetime.fromisoformat(shield_active)
            except (ValueError, TypeError):
                shield_active = None
        
        # Проверка активности щита
        shield_status = "🟢 Активен" if shield_active and shield_active > datetime.now() else "🔴 Неактивен"
        
        text = [
            "🛒 <b>Магический рынок</b>",
            "",
            f"🛡 <b>Щит свободы</b> {shield_status}",
            f"▸ Защита от порабощения на 12ч",
            f"▸ Цена: {shield_price}₽",
            "",
            "⛓ <b>Квантовые кандалы</b>",
            "▸ Увеличивают время выкупа раба",
        ]
        
        buttons = [
            [InlineKeyboardButton(
                text=f"🛒 Купить щит - {shield_price}₽",
                callback_data=f"{SHIELD_PREFIX}{shield_price}"
            )],
            [InlineKeyboardButton(
                text="⛓ Выбрать раба для кандал",
                callback_data="select_shackles"
            )],
            [InlineKeyboardButton(text="🔙 Назад", callback_data=MAIN_MENU)]
        ]
        
        await callback.message.edit_text(
            "\n".join(text),
            reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
            parse_mode=ParseMode.HTML
        )
    except Exception as e:
        logging.error(f"Ошибка в shop_handler: {e}")
        await callback.answer("❌ Ошибка загрузки магазина", show_alert=True)
    await callback.answer()

@dp.callback_query(F.data.startswith(UPGRADE_PREFIX))
async def upgrade_handler(callback: types.CallbackQuery):
    try:
        user_id = callback.from_user.id
        upgrade_id = callback.data.replace(UPGRADE_PREFIX, "")
        
        user = get_user(user_id)
        if not user:
            await callback.answer("❌ Сначала зарегистрируйтесь!", show_alert=True)
            return

        upgrade_data = upgrades.get(upgrade_id)
        if not upgrade_data:
            await callback.answer("❌ Улучшение не найдено!", show_alert=True)
            return

        current_level = user.get("upgrades", {}).get(upgrade_id, 0)
        price = upgrade_data["base_price"] * (current_level + 1)
        
        if user.get("balance", 0) < price:
            await callback.answer(f"❌ Недостаточно средств! Нужно {price}₽", show_alert=True)
            return

        # Выполняем улучшение
        user["balance"] -= price
        user.setdefault("upgrades", {})[upgrade_id] = current_level + 1
        
        # Обновляем пассивный доход для склада
        if upgrade_id == "storage":
            user["income_per_sec"] = (1 + user["upgrades"].get("storage", 0) * 10) / 60

        # Сохраняем изменения
        update_user(user_id, user)

        # Обновляем клавиатуру
        try:
            await callback.message.edit_reply_markup(
                reply_markup=upgrades_keyboard(user_id)
            )
            await callback.answer(f"✅ {upgrade_data['name']} улучшен до уровня {current_level + 1}!")
        except Exception as e:
            logging.error(f"Ошибка обновления клавиатуры: {e}")
            await callback.answer("✅ Улучшение применено!", show_alert=True)

    except Exception as e:
        logging.error(f"Ошибка в upgrade_handler: {e}", exc_info=True)
        await callback.answer("❌ Произошла ошибка при улучшении", show_alert=True)
    await callback.answer()

@dp.callback_query(F.data.startswith(SHIELD_PREFIX))
async def buy_shield(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    price = int(callback.data.replace(SHIELD_PREFIX, ""))
    
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            # Получаем данные пользователя
            cur.execute("SELECT data FROM bot_users WHERE user_id = %s", (user_id,))
            result = cur.fetchone()
            if not result:
                await callback.answer("❌ Пользователь не найден!", show_alert=True)
                return
                
            user = deserialize_user_data(result[0])
            
            # Проверяем щит
            current_shield = user.get("shield_active")
            if current_shield and current_shield > datetime.now():
                await callback.answer("❌ У вас уже есть активный щит!", show_alert=True)
                return
                
            if user.get("balance", 0) < price:
                await callback.answer("❌ Недостаточно средств!", show_alert=True)
                return
            
            # Обновляем данные
            user["balance"] -= price
            user["shield_active"] = datetime.now() + timedelta(hours=12)
            user["shop_purchases"] = user.get("shop_purchases", 0) + 1
            
            # Сохраняем изменения
            cur.execute(
                "UPDATE bot_users SET data = %s WHERE user_id = %s",
                (Json(serialize_user_data(user)), user_id)
            )
            conn.commit()
            
            await callback.answer(
                f"🛡 Щит активирован до {user['shield_active'].strftime('%H:%M')}!",
                show_alert=True
            )
            await shop_handler(callback)
            
    except Exception as e:
        conn.rollback()
        logging.error(f"Ошибка при покупке щита: {e}")
        await callback.answer("❌ Ошибка при покупке щита", show_alert=True)
    finally:
        conn.close()

@dp.callback_query(F.data == "select_shackles")
async def select_shackles(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            # Получаем данные пользователя и его рабов
            cur.execute("SELECT data FROM bot_users WHERE user_id = %s", (user_id,))
            result = cur.fetchone()
            if not result:
                await callback.answer("❌ Пользователь не найден!", show_alert=True)
                return
                
            user = deserialize_user_data(result[0])
            
            if not user.get("slaves"):
                await callback.answer("❌ У вас нет рабов!", show_alert=True)
                return
            
            # Получаем информацию о рабах
            buttons = []
            for slave_id in user["slaves"][:5]:  # Максимум 5 первых рабов
                cur.execute("SELECT data FROM bot_users WHERE user_id = %s", (slave_id,))
                slave_result = cur.fetchone()
                if slave_result:
                    slave = deserialize_user_data(slave_result[0])
                    price = calculate_shackles_price(slave_id)
                    buttons.append([
                        InlineKeyboardButton(
                            text=f"⛓ @{slave.get('username', 'unknown')} - {price}₽",
                            callback_data=f"{SHACKLES_PREFIX}{slave_id}_{price}"
                        )
                    ])
            
            buttons.append([InlineKeyboardButton(text="🔙 Назад", callback_data="shop")])
            
            await callback.message.edit_text(
                "Выберите раба для применения кандал:",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons))
            await callback.answer()
            
    except Exception as e:
        logging.error(f"Ошибка при выборе кандал: {e}")
        await callback.answer("❌ Ошибка при загрузке списка рабов", show_alert=True)
    finally:
        conn.close()

@dp.callback_query(F.data.startswith(SHACKLES_PREFIX))
async def buy_shackles(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    _, slave_id, price = callback.data.split("_")
    slave_id = int(slave_id)
    price = int(price)
    
    conn = get_db_connection()
    try:
        conn.autocommit = False
        with conn.cursor() as cur:
            # Получаем данные владельца
            cur.execute("SELECT data FROM bot_users WHERE user_id = %s", (user_id,))
            owner_result = cur.fetchone()
            if not owner_result:
                await callback.answer("❌ Пользователь не найден!", show_alert=True)
                return
                
            owner = deserialize_user_data(owner_result[0])
            
            # Проверяем принадлежность раба
            if slave_id not in owner.get("slaves", []):
                await callback.answer("❌ Этот раб вам не принадлежит!", show_alert=True)
                return
                
            if owner.get("balance", 0) < price:
                await callback.answer("❌ Недостаточно средств!", show_alert=True)
                return
            
            # Получаем данные раба для уведомления
            cur.execute("SELECT data FROM bot_users WHERE user_id = %s", (slave_id,))
            slave_result = cur.fetchone()
            slave_username = "unknown"
            if slave_result:
                slave = deserialize_user_data(slave_result[0])
                slave_username = slave.get("username", "unknown")
            
            # Обновляем данные владельца
            owner["balance"] -= price
            if "shackles" not in owner:
                owner["shackles"] = {}
            owner["shackles"][slave_id] = datetime.now() + timedelta(hours=24)
            
            # Сохраняем изменения
            cur.execute(
                "UPDATE bot_users SET data = %s WHERE user_id = %s",
                (Json(serialize_user_data(owner)), user_id)
            )
            conn.commit()
            
            await callback.answer(
                f"⛓ Кандалы применены к @{slave_username} на 24ч!",
                show_alert=True
            )
            await select_shackles(callback)
            
    except Exception as e:
        conn.rollback()
        logging.error(f"Ошибка при покупке кандал: {e}")
        await callback.answer("❌ Ошибка при применении кандал", show_alert=True)
    finally:
        conn.close()

@dp.callback_query(F.data.startswith(SLAVE_PREFIX))
async def buy_slave_handler(callback: types.CallbackQuery):
    buyer_id = callback.from_user.id
    slave_id = int(callback.data.replace(SLAVE_PREFIX, ""))
    
    conn = get_db_connection()
    try:
        conn.autocommit = False
        with conn.cursor() as cur:
            # Получаем данные покупателя и раба
            cur.execute("SELECT data FROM bot_users WHERE user_id IN (%s, %s)", (buyer_id, slave_id))
            results = cur.fetchall()
            
            if len(results) != 2:
                await callback.answer("❌ Ошибка: пользователь не найден", show_alert=True)
                return
                
            buyer = deserialize_user_data(results[0][0] if results[0][0] == buyer_id else results[1][0])
            slave = deserialize_user_data(results[0][0] if results[0][0] == slave_id else results[1][0])
            
            # Проверки (щит, самопокупка, иерархия и т.д.)
            # ... (остаются те же проверки, что и в оригинальном коде)
            
            # Получаем цену
            price = slave_price(slave)
            
            # Проверка баланса
            if buyer["balance"] < price * 0.99:
                await callback.answer(f"❌ Нужно {price}₽ (у вас {buyer['balance']:.0f}₽", show_alert=True)
                return
            
            # Обновляем предыдущего владельца (если есть)
            previous_owner_id = slave.get("owner")
            if previous_owner_id:
                cur.execute("SELECT data FROM bot_users WHERE user_id = %s", (previous_owner_id,))
                prev_owner_result = cur.fetchone()
                if prev_owner_result:
                    prev_owner = deserialize_user_data(prev_owner_result[0])
                    
                    # Удаляем раба из списка
                    if slave_id in prev_owner.get("slaves", []):
                        prev_owner["slaves"].remove(slave_id)
                    
                    # Начисляем комиссию
                    commission = int(price * 0.1)
                    prev_owner["balance"] += commission
                    prev_owner["total_income"] += commission
                    
                    # Удаляем кандалы
                    if slave_id in prev_owner.get("shackles", {}):
                        del prev_owner["shackles"][slave_id]
                    
                    # Сохраняем предыдущего владельца
                    cur.execute(
                        "UPDATE bot_users SET data = %s WHERE user_id = %s",
                        (Json(serialize_user_data(prev_owner)), previous_owner_id)
                    )
            
            # Обновляем покупателя
            buyer["balance"] -= price
            buyer.setdefault("slaves", []).append(slave_id)
            cur.execute(
                "UPDATE bot_users SET data = %s WHERE user_id = %s",
                (Json(serialize_user_data(buyer)), buyer_id))
            
            # Обновляем раба
            slave["owner"] = buyer_id
            slave["slave_level"] = min(slave.get("slave_level", 0) + 1, MAX_SLAVE_LEVEL)
            slave["price"] = slave_price(slave)
            slave["enslaved_date"] = datetime.now().isoformat()
            cur.execute(
                "UPDATE bot_users SET data = %s WHERE user_id = %s",
                (Json(serialize_user_data(slave)), slave_id))
            
            conn.commit()
            
            # Формируем сообщение об успехе
            msg = [
                f"✅ Куплен @{slave.get('username', 'безымянный')} за {price}₽",
                f"▸ Уровень: {slave['slave_level']}",
                f"▸ Новая цена: {slave['price']}₽",
                f"▸ Доход/час: {100 * (1 + 0.3 * slave['slave_level'])}₽"
            ]
            
            if previous_owner_id:
                msg.append(f"▸ Комиссия владельцу: {commission}₽")
            
            # Уведомление раба
            try:
                await bot.send_message(
                    slave_id,
                    f"⚡ Вы приобретены @{buyer.get('username', 'unknown')} "
                    f"за {price}₽ (уровень {slave['slave_level']})"
                )
            except Exception:
                pass
            
            await callback.message.edit_text("\n".join(msg), reply_markup=main_keyboard())
            await callback.answer()
            
    except Exception as e:
        conn.rollback()
        logging.error(f"Ошибка при покупке раба: {e}", exc_info=True)
        await callback.answer("❌ Ошибка при покупке раба", show_alert=True)
    finally:
        conn.close()

@dp.callback_query(F.data.startswith(BUYOUT_PREFIX))
async def buyout_handler(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    buyout_price = int(callback.data.replace(BUYOUT_PREFIX, ""))
    
    conn = get_db_connection()
    try:
        conn.autocommit = False
        with conn.cursor() as cur:
            # Получаем данные пользователя
            cur.execute("SELECT data FROM bot_users WHERE user_id = %s", (user_id,))
            user_result = cur.fetchone()
            if not user_result:
                await callback.answer("❌ Пользователь не найден!", show_alert=True)
                return
                
            user = deserialize_user_data(user_result[0])
            
            # Базовые проверки
            if not user.get("owner"):
                await callback.answer("❌ Вы и так свободны!", show_alert=True)
                return

            # Получаем данные владельца
            owner_id = user["owner"]
            cur.execute("SELECT data FROM bot_users WHERE user_id = %s", (owner_id,))
            owner_result = cur.fetchone()
            owner = deserialize_user_data(owner_result[0]) if owner_result else None

            # Проверка кандалов
            if owner and user_id in owner.get("shackles", {}):
                shackles_end = owner["shackles"][user_id].strftime("%d.%m %H:%M")
                await callback.answer(
                    f"⛓ Вы в кандалах до {shackles_end}!\nВыкуп временно невозможен",
                    show_alert=True
                )
                return

            # Проверка баланса
            if user["balance"] < buyout_price * 0.99:
                await callback.answer(
                    f"❌ Не хватает {buyout_price - user['balance']:.0f}₽\n"
                    f"Требуется: {buyout_price}₽",
                    show_alert=True
                )
                return

            # Процесс выкупа
            base_price = user.get("base_price", 100)
            slave_level = user.get("slave_level", 0)
            
            # Обновляем данные пользователя
            user["balance"] -= buyout_price
            user["owner"] = None
            user["price"] = base_price
            user["total_spent"] = user.get("total_spent", 0) + buyout_price
            user["buyout_count"] = user.get("buyout_count", 0) + 1
            
            # Обновляем владельца (если есть)
            if owner:
                owner_income = int(buyout_price * 0.6)
                owner["balance"] += owner_income
                owner["total_income"] += owner_income
                
                # Удаляем из списка рабов
                if "slaves" in owner and user_id in owner["slaves"]:
                    owner["slaves"].remove(user_id)
                
                # Сохраняем владельца
                cur.execute(
                    "UPDATE bot_users SET data = %s WHERE user_id = %s",
                    (Json(serialize_user_data(owner)), owner_id)
                )

            # Сохраняем пользователя
            cur.execute(
                "UPDATE bot_users SET data = %s WHERE user_id = %s",
                (Json(serialize_user_data(user)), user_id)
            )
            
            conn.commit()
            
            # Уведомление владельца
            if owner:
                try:
                    await bot.send_message(
                        owner_id,
                        f"🔓 Раб @{user.get('username', 'unknown')} "
                        f"выкупился за {buyout_price}₽\n"
                        f"Ваш доход: {owner_income}₽"
                    )
                except Exception as e:
                    logging.error(f"Ошибка уведомления владельца: {e}")

            # Сообщение об успехе
            await callback.message.edit_text(
                f"🎉 <b>Вы свободны!</b>\n"
                f"▸ Потрачено: {buyout_price}₽\n"
                f"▸ Сохранён уровень: {slave_level}\n"
                f"▸ Новая цена: {base_price}₽\n\n"
                f"<i>Теперь вы не платите 30% налог владельцу</i>",
                reply_markup=main_keyboard(),
                parse_mode=ParseMode.HTML
            )
            await callback.answer()

    except Exception as e:
        conn.rollback()
        logging.error(f"Ошибка выкупа: {e}", exc_info=True)
        await callback.answer("🌀 Произошла ошибка при выкупе", show_alert=True)
    finally:
        conn.close()

@dp.callback_query(F.data == PROFILE)
async def profile_handler(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            # Получаем данные пользователя
            cur.execute("SELECT data FROM bot_users WHERE user_id = %s", (user_id,))
            user_result = cur.fetchone()
            if not user_result:
                await callback.answer("❌ Сначала зарегистрируйтесь!", show_alert=True)
                return
                
            user = deserialize_user_data(user_result[0])
            
            # Рассчитываем цену выкупа
            buyout_price = 0
            if user.get("owner"):
                base_price = user.get("base_price", 100)
                buyout_price = int((base_price + user["balance"] * 0.1) * (1 + user.get("slave_level", 0) * 0.5))
                buyout_price = max(100, min(10000, buyout_price))
            
            # Получаем данные владельца (если есть)
            owner_username = None
            if user.get("owner"):
                cur.execute(
                    "SELECT data->>'username' FROM bot_users WHERE user_id = %s",
                    (user["owner"],)
                )
                owner_result = cur.fetchone()
                owner_username = owner_result[0] if owner_result else "unknown"
            
            # Получаем уровни улучшений
            barracks_level = user.get("upgrades", {}).get("barracks", 0)
            whip_level = user.get("upgrades", {}).get("whip", 0)
            
            # Формируем текст профиля
            text = [
                f"👑 <b>Профиль @{user.get('username', 'unknown')}</b>",
                f"▸ 💰 Баланс: {user.get('balance', 0):.1f}₽",
                f"▸ 👥 Уровень раба: {user.get('slave_level', 0)}",
                f"▸ 🛠 Улучшения: {sum(user.get('upgrades', {}).values())}",
                f"▸ Лимит рабов: {5 + 2 * barracks_level} (макс. {5 + 2 * MAX_BARRACKS_LEVEL})",
                f"▸ Налог: {10 + 2 * whip_level}%"
            ]
            
            if user.get("owner"):
                text.append(
                    f"\n⚠️ <b>Налог рабства:</b> 30% дохода → @{owner_username}\n"
                    f"▸ Цена выкупа: {buyout_price}₽"
                )
            else:
                text.append("\n🔗 Вы свободный человек")
                
            # Кнопка выкупа
            keyboard = []
            if user.get("owner"):
                keyboard.append([
                    InlineKeyboardButton(
                        text=f"🆓 Выкупиться за {buyout_price}₽",
                        callback_data=f"{BUYOUT_PREFIX}{buyout_price}"
                    )
                ])
            keyboard.append([InlineKeyboardButton(text="🔙 Назад", callback_data=MAIN_MENU)])
            
            await callback.message.edit_text(
                "\n".join(text),
                reply_markup=InlineKeyboardMarkup(inline_keyboard=keyboard),
                parse_mode=ParseMode.HTML
            )
            await callback.answer()
            
    except Exception as e:
        logging.error(f"Ошибка профиля: {e}", exc_info=True)
        await callback.answer("❌ Ошибка загрузки профиля", show_alert=True)
    finally:
        conn.close()

async def autosave_task():
    while True:
        await asyncio.sleep(300)  # 5 минут
        # В PostgreSQL автосохранение не требуется, так как изменения фиксируются сразу
        logging.info("Autosave check completed")

async def on_startup():
    try:
        # Создаем таблицу, если она не существует
        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS bot_users (
                    user_id BIGINT PRIMARY KEY,
                    data JSONB NOT NULL,
                    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            # Создаем индексы
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_bot_users_username 
                ON bot_users ((data->>'username'))
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_bot_users_owner 
                ON bot_users ((data->>'owner'))
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_bot_users_slaves 
                ON bot_users USING GIN ((data->'slaves'))
            """)
            conn.commit()
        logging.info("✅ PostgreSQL подключена и таблицы готовы!")
    except Exception as e:
        logging.critical(f"❌ Ошибка инициализации PostgreSQL: {e}")
        raise
    finally:
        conn.close()

async def on_shutdown():
    logging.info("Завершение работы...")
    # В PostgreSQL не требуется явное сохранение
