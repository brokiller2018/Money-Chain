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


# Настройки
TOKEN = "8076628423:AAEkp4l3BYkl-6lwz8VAyMw0h7AaAM7J3oM"
CHANNEL_ID = "@memok_da"
CHANNEL_LINK = "https://t.me/memok_da"

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

# Инициализация
bot = Bot(token=TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
storage = MemoryStorage()
dp = Dispatcher(storage=storage)

# База данных
users = {}
user_search_cache = {}

# Улучшения
upgrades = {
    "storage": {
        "name": "📦 Склад",
        "base_price": 500,
        "income_bonus": 10,
        "description": "+10 монет/мин к пассивному доходу"
    },
    "whip": {
        "name": "⛓ Кнуты", 
        "base_price": 1000,
        "income_bonus": 25,
        "description": "+25% к доходу от работы"
    },
    "food": {
        "name": "🍗 Еда",
        "base_price": 2000,
        "income_bonus": 50,
        "description": "-10% к времени ожидания работы"
    },
    "barracks": {
        "name": "🏠 Бараки",
        "base_price": 5000,
        "income_bonus": 100,
        "description": "+5 к лимиту рабов"
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
    return psycopg2.connect(os.getenv("DATABASE_URL"))
    
def upgrades_keyboard(user_id):
    buttons = []
    for upgrade_id, data in upgrades.items():
        level = users[user_id].get("upgrades", {}).get(upgrade_id, 0)
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
        [InlineKeyboardButton(text="🔙 Назад", callback_data=MAIN_MENU)]
    ])

def serialize_user_data(user_data: dict) -> dict:
    """Преобразуем datetime объекты в строки для JSON"""
    serialized = {}
    for key, value in user_data.items():
        if isinstance(value, datetime):
            serialized[key] = value.isoformat()
        else:
            serialized[key] = value
    return serialized

def deserialize_user_data(data: dict) -> dict:
    """Восстанавливаем datetime из строк"""
    deserialized = {}
    for key, value in data.items():
        if key in ['last_passive', 'last_work'] and value:
            try:
                deserialized[key] = datetime.fromisoformat(value)
            except (TypeError, ValueError):
                deserialized[key] = datetime.now()
        else:
            deserialized[key] = value
    return deserialized

def save_db():
    conn = None
    try:
        conn = get_db_connection()
        with conn:
            with conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS bot_users (
                        user_id BIGINT PRIMARY KEY,
                        data JSONB NOT NULL,
                        last_updated TIMESTAMP WITH TIME ZONE DEFAULT NOW()
                    )
                """)
                
                # Сериализуем данные перед сохранением
                for user_id, user_data in users.items():
                    serialized_data = serialize_user_data(user_data)
                    cur.execute("""
                        INSERT INTO bot_users (user_id, data)
                        VALUES (%s, %s)
                        ON CONFLICT (user_id) 
                        DO UPDATE SET 
                            data = EXCLUDED.data,
                            last_updated = NOW()
                    """, (user_id, Json(serialized_data)))
        
    except psycopg2.Error as e:
        logging.error(f"Database error in save_db: {e}")
        raise
    finally:
        if conn:
            conn.close()


def load_db():
    conn = None
    try:
        conn = get_db_connection()
        with conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_name = 'bot_users'
                    )
                """)
                table_exists = cur.fetchone()[0]
                
                if not table_exists:
                    return {}
                
                cur.execute("SELECT user_id, data FROM bot_users")
                rows = cur.fetchall()
                
                loaded_users = {}
                for user_id, data in rows:
                    # Десериализуем данные при загрузке
                    loaded_users[user_id] = deserialize_user_data(data)
                
                return loaded_users
                
    except psycopg2.Error as e:
        logging.error(f"Database error in load_db: {e}")
        return {}
    finally:
        if conn:
            conn.close()

def calculate_shield_price(user_id):
    user = users[user_id]
    # Базовый доход (1 + склад) в минуту
    passive_per_min = 1 + user.get("upgrades", {}).get("storage", 0) * 10
    # Доход от рабов в минуту
    passive_per_min += sum(
        100 * (1 + 0.3 * users[slave_id].get("slave_level", 0))
        for slave_id in user.get("slaves", [])
        if slave_id in users
    )
    # Цена = 50% дохода за 12 часов, округлено до 10
    price = round((passive_per_min * 60 * 12 * 0.5) / 10) * 10
    # Ограничения
    price = max(300, min(5000, price))
    # Скидка за первую покупку
    if user.get("shop_purchases", 0) == 0:
        price = int(price * 0.7)
    return price

def calculate_shackles_price(owner_id):
    owner = users[owner_id]
    
    # 1. Считаем пассивный доход владельца в час
    passive_income = (
        1 + owner.get("upgrades", {}).get("storage", 0) * 10  # Склад
    ) * 60  # В час
    
    # 2. Добавляем доход от всех рабов
    for slave_id in owner.get("slaves", []):
        if slave_id in users:
            slave = users[slave_id]
            passive_income += 100 * (1 + 0.3 * slave.get("slave_level", 0))
    
    # 3. Цена = 150% от часового дохода, округлено до 100
    price = int(passive_income * 1.5 / 100) * 100
    
    # 4. Ограничиваем диапазон (300–10 000₽)
    return max(300, min(10_000, price))

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
        for user_id, user in users.items():
            if "last_passive" in user:
                mins_passed = (now - user["last_passive"]).total_seconds() / 60
                
                # Базовый доход
                base_income = 1 * mins_passed
                
                # Доход от склада
                storage_income = user.get("upgrades", {}).get("storage", 0) * 10 * mins_passed
                
                # Доход от рабов (с налогом 20%)
                slaves_income = 0
                for slave_id in user.get("slaves", []):
                    if slave_id in users:
                        slave = users[slave_id]
                        slave_income = 100 * (1 + 0.3 * slave.get("slave_level", 0)) * mins_passed
                        tax = int(slave_income * 0.2)  # Налог 20%
                        slave["balance"] += slave_income - tax
                        slaves_income += tax  # Налог идёт владельцу
                
                total_income = base_income + storage_income + slaves_income
                user["balance"] += total_income
                user["total_income"] += total_income
                user["last_passive"] = now
        save_db()
            

# Обработчики команд
@dp.message(Command('start'))
async def start_command(message: Message):
    user_id = message.from_user.id
    username = message.from_user.username
    
    # Получаем referrer_id из параметров команды (если есть)
    referrer_id = None
    if len(message.text.split()) > 1:
        try:
            referrer_id = int(message.text.split()[1])
            # Проверяем, что это не сам пользователь и реферер существует
            if referrer_id == user_id or referrer_id not in users:
                referrer_id = None
        except (ValueError, IndexError):
            referrer_id = None

    if not await check_subscription(user_id):
        # Сохраняем реферала сразу, даже если пользователь еще не подписался
        if referrer_id:
            users.setdefault(user_id, {})["referrer"] = referrer_id
            save_db()
            
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔔 Подписаться", url=CHANNEL_LINK)],
            [InlineKeyboardButton(text="✅ Я подписался", callback_data=f"{CHECK_SUB}{user_id}")]
        ])
        await message.answer("📌 Для доступа подпишитесь на канал:", reply_markup=kb)
        return
    
    if user_id not in users:
        # Создаем нового пользователя с учетом реферала
        users[user_id] = {
            "balance": 100,
            "slaves": [],
            "owner": None,
            "base_price": 100,
            "enslaved_date": None,
            "slave_level": 0,
            "price": 100,
            "last_work": None,
            "upgrades": {key: 0 for key in upgrades},
            "total_income": 0,
            "username": username,
            "shield_active": None,  # Время окончания щита
            "shackles": {},  # {slave_id: end_time}
            "shop_purchases": 0,  # Счетчик покупок за день
            "last_passive": datetime.now(),
            "income_per_sec": 0.0167,
            "referrer": None  # Сохраняем реферала
        }
        
        # Начисляем бонус рефералу
        if referrer_id and referrer_id in users:
            bonus = 50  # 10% от стартового баланса
            users[referrer_id]["balance"] += bonus
            users[referrer_id]["total_income"] += bonus
            try:
                await bot.send_message(
                    referrer_id,
                    f"🎉 Вам начислено {bonus}₽ за приглашение @{username}!"
                )
            except Exception:
                pass  # Если не удалось отправить сообщение
        
        welcome_msg = (
            "👑 <b>ДОБРО ПОЖАЛОВАТЬ В РАБОВЛАДЕЛЬЧЕСКУЮ ИМПЕРИЮ!</b>\n\n"
            "⚡️ <b>Основные возможности:</b>\n"
            "▸ 💼 Бонусная работа (раз в 20 мин)\n"
            "▸ 🛠 Улучшай свои владения\n"
            "▸ 👥 Покупай рабов для пассивного дохода\n"
            "▸ 📈 Получай доход каждую минуту\n\n"
        )
        
        if referrer_id:
            referrer_name = users.get(referrer_id, {}).get("username", "друг")
            welcome_msg += f"🤝 Вас пригласил: @{referrer_name}\n\n"
        
        welcome_msg += "💰 <b>Базовая пассивка:</b> 1₽/мин"
        
        save_db()
        await message.answer(welcome_msg, reply_markup=main_keyboard(), parse_mode=ParseMode.HTML)
    else:
        await message.answer("🔮 Главное меню:", reply_markup=main_keyboard())

@dp.callback_query(F.data.startswith(CHECK_SUB))
async def check_sub_callback(callback: types.CallbackQuery):
    user_id = int(callback.data.replace(CHECK_SUB, ""))
    
    if await check_subscription(user_id):
        # Добавляем пользователя если его еще нет
        if user_id not in users:
            users[user_id] = {
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
                "referrer": None  # Сохраняем реферала
            }
        
        # Начисляем бонус рефералу
        referrer_id = users[user_id].get("referrer")
        if referrer_id and referrer_id in users:
            bonus = 50  # 10% от стартового баланса
            users[referrer_id]["balance"] += bonus
            users[referrer_id]["total_income"] += bonus
            save_db()
            
        save_db()
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
    user = users.get(user_id)
    
    if not user:
        await callback.answer("❌ Сначала зарегистрируйтесь!", show_alert=True)
        return
    
    now = datetime.now()
    cooldown = timedelta(minutes=20)
    
    if user["last_work"] and (now - user["last_work"]) < cooldown:
        remaining = (user["last_work"] + cooldown - now).seconds // 60
        await callback.answer(f"⏳ Подождите еще {remaining} минут", show_alert=True)
        return
    
    # Рассчитываем текущий пассивный доход в минуту
    passive_per_min = 1 + user.get("upgrades", {}).get("storage", 0) * 10
    passive_per_min += sum(
        100 * (1 + 0.3 * users[slave_id].get("slave_level", 0))
        for slave_id in user.get("slaves", [])
        if slave_id in users
    ) / 60
    
    # Бонус = 20 минут пассивного дохода * множитель кнутов
    whip_bonus = 1 + user.get("upgrades", {}).get("whip", 0) * 0.25
    work_bonus = passive_per_min * 20 * whip_bonus
    
    user["balance"] += work_bonus
    user["total_income"] += work_bonus
    user["last_work"] = now
    
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
    
    # Поиск пользователя
    found_user = None
    for uid, data in users.items():
        if data.get("username", "").lower() == username:
            found_user = uid
            break

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔙 Назад", callback_data=BUY_MENU)]
    ])

    if not found_user:
        await message.reply(
            "❌ Игрок не найден. Проверьте:\n"
            "1. Правильность написания\n"
            "2. Игрок должен быть зарегистрирован в боте",
            reply_markup=kb
        )
        return

    buyer_id = message.from_user.id
    if found_user == buyer_id:
        await message.reply("🌀 Нельзя купить самого себя!", reply_markup=kb)
        return

    slave = users[found_user]
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(
                text=f"💰 Купить за {slave['price']}₽ (Ур. {slave.get('slave_level', 0)})", 
                callback_data=f"{SLAVE_PREFIX}{found_user}"
            )
        ],
        [InlineKeyboardButton(text="🔙 Назад", callback_data=BUY_MENU)]
    ])

    owner_info = f"@{users[slave['owner']]['username']}" if slave.get('owner') else "Свободен"
    
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
@dp.callback_query(F.data == UPGRADES)
async def upgrades_handler(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    if user_id not in users:
        await callback.answer("❌ Сначала зарегистрируйтесь!", show_alert=True)
        return
    await callback.message.edit_text("🛠 Выберите улучшение:", reply_markup=upgrades_keyboard(user_id))
    await callback.answer()

@dp.callback_query(F.data == REF_LINK)
async def ref_link_handler(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    ref_link = f"https://t.me/{(await bot.get_me()).username}?start={user_id}"
    await callback.message.edit_text(
        f"🔗 Ваша реферальная ссылка:\n<code>{ref_link}</code>\n\n"
        "Приглашайте друзей и получайте 10% с их заработка!",
        reply_markup=main_keyboard()
    )
    await callback.answer()

@dp.callback_query(F.data == BUY_MENU)
async def buy_menu_handler(callback: types.CallbackQuery):
    await callback.message.edit_text("👥 Меню покупки рабов:", reply_markup=buy_menu_keyboard())
    await callback.answer()

@dp.callback_query(F.data == MAIN_MENU)
async def main_menu_handler(callback: types.CallbackQuery):
    await callback.message.edit_text("🔮 Главное меню:", reply_markup=main_keyboard())
    await callback.answer()

@dp.callback_query(F.data == TOP_OWNERS)
async def top_owners_handler(callback: types.CallbackQuery):
    try:
        current_user_id = callback.from_user.id
        
        # Рассчитываем эффективность для всех пользователей
        users_list = []
        for user_id, user_data in users.items():
            slaves_count = len(user_data.get("slaves", []))
            total_income = user_data.get("total_income", 0)
            efficiency = total_income / slaves_count if slaves_count > 0 else 0
            users_list.append({
                "user_id": user_id,
                "username": user_data.get("username", "Unknown"),
                "slaves": slaves_count,
                "total_income": total_income,
                "efficiency": efficiency
            })

        # Сортируем по эффективности (по убыванию)
        sorted_users = sorted(
            users_list,
            key=lambda x: x["efficiency"],
            reverse=True
        )

        # Формируем топ-10
        top_10 = sorted_users[:10]
        
        # Находим позицию текущего пользователя
        user_position = None
        for idx, user in enumerate(sorted_users, 1):
            if user["user_id"] == current_user_id:
                user_position = idx
                break

        # Формируем текст
        text = "🏆 <b>Топ рабовладельцев по эффективности:</b>\n\n"
        text += "<i>Рейтинг рассчитывается как доход на одного раба</i>\n\n"
        
        # Выводим топ-10
        for idx, user in enumerate(top_10, 1):
            if user["efficiency"] > 0:
                text += (
                    f"{idx}. @{user['username']}\n"
                    f"   ▸ Эффективность: {user['efficiency']:.1f}₽/раб\n"
                    f"   ▸ Рабов: {user['slaves']} | Доход: {user['total_income']:.1f}₽\n\n"
                )

        # Добавляем позицию пользователя
        if user_position:
            if user_position <= 10:
                text += f"\n🎉 Ваша позиция в топе: {user_position}"
            else:
                user_efficiency = next((u["efficiency"] for u in sorted_users if u["user_id"] == current_user_id), 0)
                text += f"\n📊 Ваша эффективность: {user_efficiency:.1f}₽/раб (позиция #{user_position})"
        else:
            text += "\nℹ️ Вы пока не участвуете в рейтинге"

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
        logging.error(f"Top owners error: {e}", exc_info=True)
        await callback.answer("🌀 Ошибка загрузки топа", show_alert=True)
    finally:
        await callback.answer()

@dp.callback_query(F.data == "shop")
async def shop_handler(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    user = users.get(user_id)
    
    if not user:
        await callback.answer("❌ Сначала зарегистрируйтесь!", show_alert=True)
        return

    # Расчет цены щита
    shield_price = calculate_shield_price(user_id)
    shield_status = "🟢 Активен" if user.get("shield_active") and user["shield_active"] > datetime.now() else "🔴 Неактивен"
    
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
    await callback.answer()

@dp.callback_query(F.data.startswith(UPGRADE_PREFIX))
async def upgrade_handler(callback: types.CallbackQuery):
    try:
        user_id = callback.from_user.id
        upgrade_id = callback.data.replace(UPGRADE_PREFIX, "")
        
        if user_id not in users:
            await callback.answer("❌ Сначала зарегистрируйтесь!", show_alert=True)
            return

        user = users[user_id]
        upgrade_data = upgrades.get(upgrade_id)
        
        if not upgrade_data:
            await callback.answer("❌ Улучшение не найдено!", show_alert=True)
            return

        current_level = user.get("upgrades", {}).get(upgrade_id, 0)
        price = upgrade_data["base_price"] * (current_level + 1)
        
        if user.get("balance", 0) < price:
            await callback.answer("❌ Недостаточно средств!", show_alert=True)
            return

        # Выполняем улучшение
        user["balance"] -= price
        user.setdefault("upgrades", {})[upgrade_id] = current_level + 1
        
        # Обновляем пассивный доход для склада
        if upgrade_id == "storage":
            user["income_per_sec"] = (1 + user["upgrades"].get("storage", 0) * 10) / 60

        # Сохраняем изменения в БД
        save_db()

        # Обновляем клавиатуру
        try:
            await callback.message.edit_reply_markup(
                reply_markup=upgrades_keyboard(user_id)
            )
            await callback.answer(f"✅ {upgrade_data['name']} улучшен до уровня {current_level + 1}!")
        except Exception as e:
            logging.error(f"Ошибка обновления клавиатуры: {str(e)}")
            await callback.answer("✅ Улучшение применено!", show_alert=True)

    except Exception as e:
        logging.error(f"Ошибка в обработчике улучшений: {str(e)}", exc_info=True)
        await callback.answer("❌ Произошла ошибка при улучшении", show_alert=True)

@dp.callback_query(F.data.startswith(SHIELD_PREFIX))
async def buy_shield(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    user = users.get(user_id)
    price = int(callback.data.replace(SHIELD_PREFIX, ""))
    
    if user.get("shield_active") and user["shield_active"] > datetime.now():
        await callback.answer("❌ У вас уже есть активный щит!", show_alert=True)
        return
        
    if user.get("balance", 0) < price:
        await callback.answer("❌ Недостаточно средств!", show_alert=True)
        return
    
    user["balance"] -= price
    user["shield_active"] = datetime.now() + timedelta(hours=12)
    user["shop_purchases"] = user.get("shop_purchases", 0) + 1
    save_db()
    
    await callback.answer(f"🛡 Щит активирован до {user['shield_active'].strftime('%H:%M')}!", show_alert=True)
    await shop_handler(callback)  # Обновляем магазин

@dp.callback_query(F.data == "select_shackles")
async def select_shackles(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    user = users.get(user_id)
    
    if not user or not user.get("slaves"):
        await callback.answer("❌ У вас нет рабов!", show_alert=True)
        return
    
    buttons = []
    for slave_id in user["slaves"][:5]:  # Максимум 5 первых рабов
        slave = users.get(slave_id, {})
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
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons)
    )
    await callback.answer()

@dp.callback_query(F.data.startswith(SHACKLES_PREFIX))
async def buy_shackles(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    user = users.get(user_id)
    _, slave_id, price = callback.data.split("_")
    slave_id = int(slave_id)
    price = int(price)
    
    if slave_id not in user.get("slaves", []):
        await callback.answer("❌ Этот раб вам не принадлежит!", show_alert=True)
        return
        
    if user.get("balance", 0) < price:
        await callback.answer("❌ Недостаточно средств!", show_alert=True)
        return
    
    # Применяем кандалы
    user["balance"] -= price
    if "shackles" not in user:
        user["shackles"] = {}
    user["shackles"][slave_id] = datetime.now() + timedelta(hours=24)
    save_db()
    
    await callback.answer(
        f"⛓ Кандалы применены к @{users[slave_id].get('username', 'unknown')} на 24ч!",
        show_alert=True
    )
    await select_shackles(callback)  # Возврат к выбору

@dp.callback_query(F.data.startswith(SLAVE_PREFIX))
@dp.callback_query(F.data.startswith(SLAVE_PREFIX))
async def buy_slave_handler(callback: types.CallbackQuery):
    try:
        buyer_id = callback.from_user.id
        slave_id = int(callback.data.replace(SLAVE_PREFIX, ""))
        
        # Проверка существования пользователей
        if buyer_id not in users or slave_id not in users:
            await callback.answer("❌ Ошибка: пользователь не найден", show_alert=True)
            return

        buyer = users[buyer_id]
        slave = users[slave_id]

        # Проверка щита защиты
        if slave.get("shield_active") and slave["shield_active"] > datetime.now():
            shield_time = slave["shield_active"].strftime("%d.%m %H:%M")
            await callback.answer(
                f"🛡 Цель защищена щитом до {shield_time}",
                show_alert=True
            )
            return

        # Проверка на покупку самого себя
        if slave_id == buyer_id:
            await callback.answer("❌ Нельзя купить самого себя!", show_alert=True)
            return

        # Проверка иерархии рабства
        if buyer.get("owner") == slave_id:
            await callback.answer("❌ Нельзя купить своего владельца!", show_alert=True)
            return

        # Проверка двойного владения
        if slave.get("owner") == buyer_id:
            await callback.answer("❌ Этот раб уже принадлежит вам!", show_alert=True)
            return

        # Проверка текущего владельца
        previous_owner_id = slave.get("owner")
        if previous_owner_id and previous_owner_id != buyer_id:
            await callback.answer(
                f"❌ Раб принадлежит @{users[previous_owner_id].get('username', 'unknown')}",
                show_alert=True
            )
            return

        price = slave.get("price", 100)
        
        # Проверка баланса с запасом 1% для округления
        if buyer["balance"] < price * 0.99:
            await callback.answer(
                f"❌ Нужно {price}₽ (у вас {buyer['balance']:.0f}₽)",
                show_alert=True
            )
            return

        # Логика покупки
        if previous_owner_id:
            previous_owner = users[previous_owner_id]
            
            # Удаляем из списка рабов предыдущего владельца
            if slave_id in previous_owner.get("slaves", []):
                previous_owner["slaves"].remove(slave_id)
            
            # Комиссия 10% предыдущему владельцу
            commission = int(price * 0.1)
            previous_owner["balance"] += commission
            previous_owner["total_income"] += commission

            # Снимаем кандалы при смене владельца
            if "shackles" in previous_owner and slave_id in previous_owner["shackles"]:
                del previous_owner["shackles"][slave_id]

        # Основная транзакция
        buyer["balance"] -= price
        buyer.setdefault("slaves", []).append(slave_id)

        # Обновление данных раба
        slave["owner"] = buyer_id
        slave["slave_level"] = min(  # Ограничение уровня
            slave.get("slave_level", 0) + 1, 
            10  # Максимальный уровень
        )
        slave["price"] = int(slave.get("base_price", 100) * (1.5 ** slave["slave_level"]))
        slave["enslaved_date"] = datetime.now()

        # Формирование сообщения
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

        save_db()
        await callback.message.edit_text("\n".join(msg), reply_markup=main_keyboard())
        await callback.answer()

    except Exception as e:
        logging.error(f"Ошибка покупки раба: {e}", exc_info=True)
        await callback.answer("❌ Критическая ошибка транзакции", show_alert=True)
@dp.callback_query(F.data.startswith(BUYOUT_PREFIX))
@dp.callback_query(F.data.startswith(BUYOUT_PREFIX))
async def buyout_handler(callback: types.CallbackQuery):
    try:
        user_id = callback.from_user.id
        user = users.get(user_id)
        
        # Базовые проверки
        if not user:
            await callback.answer("❌ Сначала зарегистрируйтесь!", show_alert=True)
            return
            
        if not user.get("owner"):
            await callback.answer("❌ Вы и так свободны!", show_alert=True)
            return

        # Проверка кандалов
        owner = users.get(user["owner"], {})
        if owner.get("shackles", {}).get(user_id):
            shackles_end = owner["shackles"][user_id].strftime("%d.%m %H:%M")
            await callback.answer(
                f"⛓ Вы в кандалах до {shackles_end}!\n"
                f"Выкуп временно невозможен",
                show_alert=True
            )
            return

        # Расчет цены выкупа
        base_price = user.get("base_price", 100)
        slave_level = user.get("slave_level", 0)
        
        # Формула: (база + 5% капитала) * (1 + 0.3 за уровень)
        buyout_price = int(
            (base_price + user["balance"] * 0.05) * 
            (1 + slave_level * 0.3)
        )
        
        # Ограничения цены
        buyout_price = max(100, min(20000, buyout_price))  # 100-20k
        
        # Проверка баланса (с запасом 1%)
        if user["balance"] < buyout_price * 0.99:
            await callback.answer(
                f"❌ Не хватает {buyout_price - user['balance']:.0f}₽\n"
                f"Требуется: {buyout_price}₽",
                show_alert=True
            )
            return

        # Процесс выкупа
        owner_id = user["owner"]
        user["balance"] -= buyout_price
        user["owner"] = None
        user["price"] = base_price  # Сброс цены
        
        # Владелец получает 60% от выкупа (вместо 50%)
        if owner_id in users:
            owner_income = int(buyout_price * 0.6)
            users[owner_id]["balance"] += owner_income
            users[owner_id]["total_income"] += owner_income
            
            # Удаляем из списка рабов
            if user_id in users[owner_id].get("slaves", []):
                users[owner_id]["slaves"].remove(user_id)
            
            # Уведомление владельца
            try:
                await bot.send_message(
                    owner_id,
                    f"🔓 Раб @{user.get('username', 'unknown')} "
                    f"выкупился за {buyout_price}₽\n"
                    f"Ваш доход: {owner_income}₽"
                )
            except Exception:
                pass

        # Обновление статистики
        user["total_spent"] = user.get("total_spent", 0) + buyout_price
        user["buyout_count"] = user.get("buyout_count", 0) + 1
        
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
        
        save_db()
        await callback.answer()

    except Exception as e:
        logging.error(f"Buyout error: {e}", exc_info=True)
        await callback.answer("🌀 Произошла ошибка при выкупе", show_alert=True)
# Обновленный профиль
@dp.callback_query(F.data == PROFILE)
async def profile_handler(callback: types.CallbackQuery):
    try:
        user_id = callback.from_user.id
        user = users.get(user_id)
        
        if not user:
            await callback.answer("❌ Сначала зарегистрируйтесь!", show_alert=True)
            return
        
        # Рассчитываем цену выкупа
        buyout_price = 0
        if user.get("owner"):
            base_price = user.get("base_price", 100)
            buyout_price = int((base_price + user["balance"] * 0.1) * (1 + user.get("slave_level", 0) * 0.5))
            buyout_price = max(100, min(10000, buyout_price))
        
        # Формируем текст профиля
        text = [
            f"👑 <b>Профиль @{user.get('username', 'unknown')}</b>",
            f"▸ 💰 Баланс: {user.get('balance', 0):.1f}₽",
            f"▸ 👥 Уровень раба: {user.get('slave_level', 0)}",
            f"▸ 🛠 Улучшения: {sum(user.get('upgrades', {}).values())}",
        ]
        
        if user.get("owner"):
            owner = users.get(user["owner"], {})
            text.append(
                f"⚠️ <b>Налог рабства:</b> 30% дохода → @{owner.get('username', 'unknown')}\n"
                f"▸ Цена выкупа: {buyout_price}₽"
            )
        else:
            text.append("🔗 Вы свободный человек")
            
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
        
async def autosave_task():
    while True:
        await asyncio.sleep(300)  # 5 минут
        save_db()
        
async def on_startup():
    global users
    users = load_db()  # Загружаем БД при старте
    asyncio.create_task(passive_income_task())
    asyncio.create_task(autosave_task())
    # Сохраняем БД при корректном завершении
    import signal
    import functools
    def save_on_exit(*args):
        save_db()
    
    signal.signal(signal.SIGTERM, save_on_exit)
    signal.signal(signal.SIGINT, save_on_exit)
    
async def on_shutdown():
    save_db() 


async def main():
    try:
        # Инициализация логирования (должна быть первой)
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
            handlers=[
                logging.StreamHandler(),
                logging.FileHandler("bot.log", encoding='utf-8')
            ]
        )
        logger = logging.getLogger(__name__)
        
        logger.info("Запуск бота...")
        
        # Загрузка и инициализация
        await on_startup()
        
        # Основной цикл бота
        await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())
        
    except (KeyboardInterrupt, SystemExit):
        logger.info("Бот остановлен вручную")
    except Exception as e:
        logger.critical(f"Критическая ошибка: {str(e)}", exc_info=True)
    finally:
        logger.info("Завершение работы...")
        await on_shutdown()
        logger.info("Бот успешно остановлен")

if __name__ == "__main__":
    try:
        # Для Windows нужно установить специальный event loop
        if os.name == 'nt':
            asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
            
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
