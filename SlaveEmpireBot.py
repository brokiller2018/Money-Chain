import logging
import asyncio
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

# Функции работы с базой данных
def get_db_connection():
    """Создает соединение с базой данных"""
    return psycopg2.connect(os.getenv("DATABASE_URL"))

def get_user(user_id: int) -> dict:
    """Получает данные пользователя из базы данных"""
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT data FROM bot_users WHERE user_id = %s", (user_id,))
            result = cur.fetchone()
            if result:
                return deserialize_user_data(result[0])
            return None
    finally:
        conn.close()

def create_user(user_id: int, username: str, referrer_id: int = None) -> dict:
    """Создает нового пользователя в базе данных"""
    new_user = {
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
        "shield_active": None,
        "shackles": {},
        "shop_purchases": 0,
        "last_passive": datetime.now(),
        "income_per_sec": 0.0167,
        "referrer": referrer_id,
        "registered": True
    }
    
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO bot_users (user_id, data) VALUES (%s, %s)",
                (user_id, Json(serialize_user_data(new_user)))
            )
            conn.commit()
    finally:
        conn.close()
    
    return new_user

def update_user(user_id: int, user_data: dict):
    """Обновляет данные пользователя в базе данных"""
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE bot_users SET data = %s WHERE user_id = %s",
                (Json(serialize_user_data(user_data)), user_id)
            conn.commit()
    finally:
        conn.close()

def serialize_user_data(user_data: dict) -> dict:
    """Преобразует datetime объекты в строки для JSON"""
    return {k: v.isoformat() if isinstance(v, datetime) else v 
            for k, v in user_data.items()}

def deserialize_user_data(data: dict) -> dict:
    """Восстанавливает datetime из строк"""
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

def upgrades_keyboard(user_id):
    """Генерирует клавиатуру улучшений"""
    user = get_user(user_id)
    buttons = []
    for upgrade_id, data in upgrades.items():
        level = user.get("upgrades", {}).get(upgrade_id, 0)
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
    """Генерирует клавиатуру меню покупки"""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔍 Поиск по юзернейму", callback_data=SEARCH_USER)],
        [InlineKeyboardButton(text="🔙 Назад", callback_data=MAIN_MENU)]
    ])

# Вспомогательные функции
async def check_subscription(user_id: int):
    """Проверяет подписку пользователя на канал"""
    try:
        member = await bot.get_chat_member(chat_id=CHANNEL_ID, user_id=user_id)
        return member.status not in ['left', 'kicked']
    except Exception as e:
        logging.error(f"Ошибка проверки подписки: {e}")
        return False

async def passive_income_task():
    """Задача пассивного дохода"""
    while True:
        await asyncio.sleep(60)
        conn = get_db_connection()
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT user_id, data FROM bot_users")
                for user_id, data in cur.fetchall():
                    user = deserialize_user_data(data)
                    now = datetime.now()
                    
                    if "last_passive" not in user:
                        continue
                        
                    mins_passed = (now - user["last_passive"]).total_seconds() / 60
                    mins_passed = min(mins_passed, 24 * 60)  # Не больше суток
                    
                    # Базовый доход
                    base_income = 1 * mins_passed
                    
                    # Доход от склада
                    storage_income = user.get("upgrades", {}).get("storage", 0) * 10 * mins_passed
                    
                    # Доход от рабов (с налогом 20%)
                    slaves_income = 0
                    for slave_id in user.get("slaves", []):
                        slave = get_user(slave_id)
                        if slave:
                            slave_income = 100 * (1 + 0.3 * slave.get("slave_level", 0)) * mins_passed
                            tax = int(slave_income * 0.2)
                            slave["balance"] += slave_income - tax
                            update_user(slave_id, slave)
                            slaves_income += tax
                    
                    total_income = base_income + storage_income + slaves_income
                    user["balance"] += total_income
                    user["total_income"] += total_income
                    user["last_passive"] = now
                    
                    update_user(user_id, user)
        finally:
            conn.close()
# Обработчики команд
@dp.message(Command('start'))
async def start_command(message: Message):
    user_id = message.from_user.id
    username = message.from_user.username
    
    # Получаем referrer_id из параметров команды
    referrer_id = None
    if len(message.text.split()) > 1:
        try:
            referrer_id = int(message.text.split()[1])
            # Проверяем существование реферера в базе
            if referrer_id == user_id or not get_user(referrer_id):
                referrer_id = None
        except (ValueError, IndexError):
            referrer_id = None

    if not await check_subscription(user_id):
        # Сохраняем реферала временно в базе
        if referrer_id:
            temp_user = {
                "referrer": referrer_id,
                "registered": False
            }
            update_user(user_id, temp_user)
            
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔔 Подписаться", url=CHANNEL_LINK)],
            [InlineKeyboardButton(text="✅ Я подписался", callback_data=f"{CHECK_SUB}{user_id}")]
        ])
        await message.answer("📌 Для доступа подпишитесь на канал:", reply_markup=kb)
        return
    
    user = get_user(user_id)
    if not user or not user.get("registered"):
        # Создаем нового пользователя
        new_user = create_user(user_id, username, referrer_id)
        
        # Начисляем бонус рефералу
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
                        f"🎉 Вам начислено {bonus}₽ за приглашение @{username}!"
                    )
                except Exception:
                    pass

        welcome_msg = (
            "👑 <b>ДОБРО ПОЖАЛОВАТЬ В РАБОВЛАДЕЛЬЧЕСКУЮ ИМПЕРИЮ!</b>\n\n"
            "⚡️ <b>Основные возможности:</b>\n"
            "▸ 💼 Бонусная работа (раз в 20 мин)\n"
            "▸ 🛠 Улучшай свои владения\n"
            "▸ 👥 Покупай рабов для пассивного дохода\n\n"
        )
        
        if referrer_id:
            referrer = get_user(referrer_id)
            referrer_name = referrer.get("username", "друг") if referrer else "друг"
            welcome_msg += f"🤝 Вас пригласил: @{referrer_name}\n\n"
        
        welcome_msg += "💰 <b>Базовая пассивка:</b> 1₽/мин"
        await message.answer(welcome_msg, reply_markup=main_keyboard(), parse_mode=ParseMode.HTML)
    else:
        await message.answer("🔮 Главное меню:", reply_markup=main_keyboard())

@dp.callback_query(F.data.startswith(CHECK_SUB))
async def check_sub_callback(callback: types.CallbackQuery):
    user_id = int(callback.data.replace(CHECK_SUB, ""))
    
    if await check_subscription(user_id):
        user = get_user(user_id)
        if not user or not user.get("registered"):
            # Создаем полноценного пользователя
            username = callback.from_user.username
            new_user = create_user(user_id, username, user.get("referrer") if user else None)
            
            # Начисляем бонус рефералу
            referrer_id = new_user.get("referrer")
            if referrer_id:
                referrer = get_user(referrer_id)
                if referrer:
                    bonus = 50
                    referrer["balance"] += bonus
                    referrer["total_income"] += bonus
                    update_user(referrer_id, referrer)

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
            inline_keyboard=[[InlineKeyboardButton(text="🔙 В меню покупок", callback_data=BUY_MENU)]]
        ),
        parse_mode=ParseMode.HTML
    )
    await callback.answer()

@dp.callback_query(F.data == WORK)
async def work_handler(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    user = get_user(user_id)
    
    if not user or not user.get("registered"):
        await callback.answer("❌ Сначала зарегистрируйтесь!", show_alert=True)
        return
    
    now = datetime.now()
    cooldown = timedelta(minutes=20)
    
    if user["last_work"] and (now - user["last_work"]) < cooldown:
        remaining = (user["last_work"] + cooldown - now).seconds // 60
        await callback.answer(f"⏳ Подождите еще {remaining} минут", show_alert=True)
        return
    
    # Рассчитываем текущий пассивный доход
    passive_per_min = 1 + user.get("upgrades", {}).get("storage", 0) * 10
    passive_per_min += sum(
        100 * (1 + 0.3 * get_user(slave_id).get("slave_level", 0)) / 60
        for slave_id in user.get("slaves", [])
    )
    
    # Бонус работы
    whip_bonus = 1 + user.get("upgrades", {}).get("whip", 0) * 0.25
    work_bonus = passive_per_min * 20 * whip_bonus
    
    user["balance"] += work_bonus
    user["total_income"] += work_bonus
    user["last_work"] = now
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
    username = message.text.strip().lower().replace('@', '')
    
    # Поиск в базе данных
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT user_id, data FROM bot_users WHERE data->>'username' ILIKE %s",
                (username,)
            result = cur.fetchone()
    finally:
        conn.close()

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔙 Назад", callback_data=BUY_MENU)]
    ])

    if not result:
        await message.reply("❌ Игрок не найден", reply_markup=kb)
        return

    slave_id, slave_data = result
    slave = deserialize_user_data(slave_data)
    buyer_id = message.from_user.id

    if slave_id == buyer_id:
        await message.reply("🌀 Нельзя купить самого себя!", reply_markup=kb)
        return

    # Формируем информацию о рабе
    owner_info = "Свободен"
    if slave.get('owner'):
        owner = get_user(slave['owner'])
        owner_info = f"@{owner['username']}" if owner else "Неизвестен"
    
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(
            text=f"💰 Купить за {slave['price']}₽ (Ур. {slave.get('slave_level', 0)})", 
            callback_data=f"{SLAVE_PREFIX}{slave_id}"
        )],
        [InlineKeyboardButton(text="🔙 Назад", callback_data=BUY_MENU)]
    ])
    
    await message.reply(
        f"🔎 <b>Найден раб:</b>\n"
        f"▸ Ник: @{slave['username']}\n"
        f"▸ Уровень: {slave.get('slave_level', 0)}\n"
        f"▸ Цена: {slave['price']}₽\n"
        f"▸ Владелец: {owner_info}\n\n"
        f"💡 <i>Доход от этого раба: {int(100 * (1 + 0.5 * slave.get('slave_level', 0))}₽ за цикл работы</i>",
        reply_markup=kb,
        parse_mode=ParseMode.HTML
    )

@dp.callback_query(F.data == TOP_OWNERS)
async def top_owners_handler(callback: types.CallbackQuery):
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute("SELECT data FROM bot_users")
            users_list = [
                {
                    "user_id": row[0]['user_id'],
                    "username": row[0].get('username', 'Unknown'),
                    "slaves": len(row[0].get('slaves', [])),
                    "total_income": row[0].get('total_income', 0),
                    "efficiency": row[0].get('total_income', 0) / len(row[0].get('slaves', [1])) 
                        if len(row[0].get('slaves', [])) > 0 else 0
                }
                for row in cur.fetchall()
            ]
            
        sorted_users = sorted(
            users_list,
            key=lambda x: x["efficiency"],
            reverse=True
        )
        
        # Формирование текста топа...
        # [остальная часть аналогична, но использует sorted_users из БД]
        
    except Exception as e:
        logging.error(f"Top owners error: {e}", exc_info=True)
        await callback.answer("🌀 Ошибка загрузки топа", show_alert=True)

@dp.callback_query(F.data == "shop")
async def shop_handler(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    user = get_user(user_id)
    
    if not user or not user.get("registered"):
        await callback.answer("❌ Сначала зарегистрируйтесь!", show_alert=True)
        return

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
        
        user = get_user(user_id)
        if not user or not user.get("registered"):
            await callback.answer("❌ Сначала зарегистрируйтесь!", show_alert=True)
            return

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

        update_user(user_id, user)

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
    user = get_user(user_id)
    price = int(callback.data.replace(SHIELD_PREFIX, ""))
    
    if not user or not user.get("registered"):
        await callback.answer("❌ Сначала зарегистрируйтесь!", show_alert=True)
        return
    
    if user.get("shield_active") and user["shield_active"] > datetime.now():
        await callback.answer("❌ У вас уже есть активный щит!", show_alert=True)
        return
        
    if user.get("balance", 0) < price:
        await callback.answer("❌ Недостаточно средств!", show_alert=True)
        return
    
    user["balance"] -= price
    user["shield_active"] = datetime.now() + timedelta(hours=12)
    user["shop_purchases"] = user.get("shop_purchases", 0) + 1
    update_user(user_id, user)
    
    await callback.answer(f"🛡 Щит активирован до {user['shield_active'].strftime('%H:%M')}!", show_alert=True)
    await shop_handler(callback)

@dp.callback_query(F.data == "select_shackles")
async def select_shackles(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    user = get_user(user_id)
    
    if not user or not user.get("slaves"):
        await callback.answer("❌ У вас нет рабов!", show_alert=True)
        return
    
    buttons = []
    for slave_id in user["slaves"][:5]:  # Ограничиваем 5 рабами для пагинации
        slave = get_user(slave_id)
        if slave:
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
    user = get_user(user_id)
    _, slave_id, price = callback.data.split("_")
    slave_id = int(slave_id)
    price = int(price)
    
    if not user or slave_id not in user.get("slaves", []):
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
    update_user(user_id, user)
    
    slave = get_user(slave_id)
    slave_name = slave.get("username", "unknown") if slave else "unknown"
    
    await callback.answer(
        f"⛓ Кандалы применены к @{slave_name} на 24ч!",
        show_alert=True
    )
    await select_shackles(callback)

@dp.callback_query(F.data.startswith(SLAVE_PREFIX))
async def buy_slave_handler(callback: types.CallbackQuery):
    try:
        buyer_id = callback.from_user.id
        slave_id = int(callback.data.replace(SLAVE_PREFIX, ""))
        
        buyer = get_user(buyer_id)
        slave = get_user(slave_id)
        
        if not buyer or not buyer.get("registered") or not slave:
            await callback.answer("❌ Ошибка: пользователь не найден", show_alert=True)
            return

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
            previous_owner = get_user(previous_owner_id)
            owner_name = previous_owner.get("username", "unknown") if previous_owner else "unknown"
            await callback.answer(
                f"❌ Раб принадлежит @{owner_name}",
                show_alert=True
            )
            return

        price = slave.get("price", 100)
        
        if buyer.get("balance", 0) < price * 0.99:
            await callback.answer(
                f"❌ Нужно {price}₽ (у вас {buyer['balance']:.0f}₽)",
                show_alert=True
            )
            return

        # Начинаем транзакцию
        conn = get_db_connection()
        try:
            with conn:
                with conn.cursor() as cur:
                    # Обновляем предыдущего владельца
                    if previous_owner_id:
                        previous_owner = get_user(previous_owner_id)
                        if previous_owner:
                            if slave_id in previous_owner.get("slaves", []):
                                previous_owner["slaves"].remove(slave_id)
                            
                            commission = int(price * 0.1)
                            previous_owner["balance"] += commission
                            previous_owner["total_income"] += commission
                            
                            if "shackles" in previous_owner and slave_id in previous_owner["shackles"]:
                                del previous_owner["shackles"][slave_id]
                            
                            update_user(previous_owner_id, previous_owner)

                    # Обновляем покупателя
                    buyer["balance"] -= price
                    buyer.setdefault("slaves", []).append(slave_id)
                    update_user(buyer_id, buyer)

                    # Обновляем раба
                    slave["owner"] = buyer_id
                    slave["slave_level"] = min(slave.get("slave_level", 0) + 1, 10)
                    slave["price"] = int(slave.get("base_price", 100) * (1.5 ** slave["slave_level"]))
                    slave["enslaved_date"] = datetime.now()
                    update_user(slave_id, slave)

                    # Формируем сообщение
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

        finally:
            conn.close()

    except Exception as e:
        logging.error(f"Ошибка покупки раба: {e}", exc_info=True)
        await callback.answer("❌ Критическая ошибка транзакции", show_alert=True)

@dp.callback_query(F.data.startswith(BUYOUT_PREFIX))
async def buyout_handler(callback: types.CallbackQuery):
    try:
        user_id = callback.from_user.id
        user = get_user(user_id)
        
        if not user or not user.get("registered"):
            await callback.answer("❌ Сначала зарегистрируйтесь!", show_alert=True)
            return
            
        if not user.get("owner"):
            await callback.answer("❌ Вы и так свободны!", show_alert=True)
            return

        # Проверка кандалов
        owner = get_user(user["owner"])
        if owner and owner.get("shackles", {}).get(user_id):
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
        buyout_price = int((base_price + user["balance"] * 0.05) * (1 + slave_level * 0.3))
        buyout_price = max(100, min(20000, buyout_price))
        
        if user.get("balance", 0) < buyout_price * 0.99:
            await callback.answer(
                f"❌ Не хватает {buyout_price - user['balance']:.0f}₽\n"
                f"Требуется: {buyout_price}₽",
                show_alert=True
            )
            return

        # Начинаем транзакцию
        conn = get_db_connection()
        try:
            with conn:
                with conn.cursor() as cur:
                    owner_id = user["owner"]
                    user["balance"] -= buyout_price
                    user["owner"] = None
                    user["price"] = base_price
                    user["total_spent"] = user.get("total_spent", 0) + buyout_price
                    user["buyout_count"] = user.get("buyout_count", 0) + 1
                    update_user(user_id, user)

                    if owner_id:
                        owner = get_user(owner_id)
                        if owner:
                            owner_income = int(buyout_price * 0.6)
                            owner["balance"] += owner_income
                            owner["total_income"] += owner_income
                            
                            if user_id in owner.get("slaves", []):
                                owner["slaves"].remove(user_id)
                            
                            update_user(owner_id, owner)
                            
                            try:
                                await bot.send_message(
                                    owner_id,
                                    f"🔓 Раб @{user.get('username', 'unknown')} "
                                    f"выкупился за {buyout_price}₽\n"
                                    f"Ваш доход: {owner_income}₽"
                                )
                            except Exception:
                                pass

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

        finally:
            conn.close()

    except Exception as e:
        logging.error(f"Buyout error: {e}", exc_info=True)
        await callback.answer("🌀 Произошла ошибка при выкупе", show_alert=True)
# Обновленный профиль
@dp.callback_query(F.data == PROFILE)
async def profile_handler(callback: types.CallbackQuery):
    try:
        user_id = callback.from_user.id
        user = get_user(user_id)
        
        if not user or not user.get("registered"):
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
            f"▸ 🛡 Щит: {'🟢 Активен' if user.get('shield_active') and user['shield_active'] > datetime.now() else '🔴 Неактивен'}"
        ]
        
        if user.get("owner"):
            owner = get_user(user["owner"])
            owner_name = owner.get("username", "unknown") if owner else "unknown"
            text.append(
                f"⚠️ <b>Налог рабства:</b> 30% дохода → @{owner_name}\n"
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

async def on_startup():
    # Инициализация таблицы при первом запуске
    conn = get_db_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS bot_users (
                        user_id BIGINT PRIMARY KEY,
                        data JSONB NOT NULL,
                        last_updated TIMESTAMP WITH TIME ZONE DEFAULT NOW()
                    )
                """)
                conn.commit()
    finally:
        conn.close()
    
    asyncio.create_task(passive_income_task())
    
    # Обработчики завершения работы
    def signal_handler(*args):
        logging.info("Получен сигнал завершения, сохраняем данные...")
    
    import signal
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)
    
async def on_shutdown():
    logging.info("Завершение работы бота...")

async def main():
    try:
        # Инициализация логирования
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
        
        # Инициализация
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
