# commands.py (Part 1/5)
import logging
import asyncio
import os
from datetime import datetime, timedelta
import asyncpg
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.enums import ParseMode
from aiogram.client.default import DefaultBotProperties
from aiogram import F

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

# Улучшения (полный перебаланс)
upgrades = {
    "storage": {
        "name": "📦 Склад",
        "base_price": 300,
        "income_bonus": 5,
        "description": "+5 монет/мин к пассивному доходу",
        "multiplier": 1.4
    },
    "whip": {
        "name": "⛓ Кнуты", 
        "base_price": 800,
        "income_bonus": 20,
        "description": "+20% к доходу от работы",
        "multiplier": 1.5
    },
    "food": {
        "name": "🍗 Еда",
        "base_price": 1500,
        "income_bonus": 30,
        "description": "-15% к времени ожидания работы",
        "multiplier": 1.6
    },
    "barracks": {
        "name": "🏠 Бараки",
        "base_price": 4000,
        "income_bonus": 50,
        "description": "+3 к лимиту рабов",
        "multiplier": 2.0
    }
}

# Подключение к PostgreSQL
async def get_db():
    return await asyncpg.connect(os.getenv("DATABASE_URL"))

# Сериализация/десериализация
def serialize_user_data(user_data: dict) -> dict:
    return {
        k: v.isoformat() if isinstance(v, datetime) else v
        for k, v in user_data.items()
    }

def deserialize_user_data(data: dict) -> dict:
    return {
        k: datetime.fromisoformat(v) if k in ['last_passive', 'last_work'] and v else v
        for k, v in data.items()
    }

# Основные операции с БД
async def get_user(user_id: int) -> dict:
    async with await get_db() as conn:
        data = await conn.fetchval(
            "SELECT data FROM bot_users WHERE user_id = $1", 
            user_id
        )
        return deserialize_user_data(data) if data else None

async def update_user(user_id: int, data: dict):
    async with await get_db() as conn:
        await conn.execute(
            "INSERT INTO bot_users (user_id, data) VALUES ($1, $2) "
            "ON CONFLICT (user_id) DO UPDATE SET data = $2, last_updated = NOW()",
            user_id, 
            serialize_user_data(data)
        )

async def init_db():
    async with await get_db() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS bot_users (
                user_id BIGINT PRIMARY KEY,
                data JSONB NOT NULL,
                last_updated TIMESTAMP WITH TIME ZONE DEFAULT NOW()
            )
        """)
        # commands.py (Part 2/5)
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

async def upgrades_keyboard(user_id: int):
    user = await get_user(user_id)
    buttons = []
    for upgrade_id, data in upgrades.items():
        level = user.get("upgrades", {}).get(upgrade_id, 0)
        price = int(data["base_price"] * (data["multiplier"] ** level))
        buttons.append([
            InlineKeyboardButton(
                text=f"{data['name']} (Ур. {level}) - {price}₽ | {data['description']}",
                callback_data=f"{UPGRADE_PREFIX}{upgrade_id}"
            )
        ])
    buttons.append([InlineKeyboardButton(text="🔙 Назад", callback_data=MAIN_MENU)])
    return InlineKeyboardMarkup(inline_keyboard=buttons)

async def buy_menu_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔍 Поиск по юзернейму", callback_data=SEARCH_USER)],
        [InlineKeyboardButton(text="🔙 Назад", callback_data=MAIN_MENU)]
    ])

# Обработчики команд
@dp.message(Command('start'))
async def start_command(message: Message):
    user_id = message.from_user.id
    username = message.from_user.username
    
    # Проверка подписки
    if not await check_subscription(user_id):
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔔 Подписаться", url=CHANNEL_LINK)],
            [InlineKeyboardButton(text="✅ Я подписался", callback_data=f"{CHECK_SUB}{user_id}")]
        ])
        await message.answer("📌 Для доступа подпишитесь на канал:", reply_markup=kb)
        return

    user = await get_user(user_id)
    if not user:
        # Создание нового пользователя
        new_user = {
            "balance": 150,  # Увеличен стартовый баланс
            "slaves": [],
            "owner": None,
            "base_price": 150,
            "enslaved_date": None,
            "slave_level": 0,
            "price": 150,
            "last_work": None,
            "upgrades": {key: 0 for key in upgrades},
            "total_income": 0,
            "username": username,
            "shield_active": None,
            "shackles": {},
            "shop_purchases": 0,
            "last_passive": datetime.now(),
            "referrer": None
        }
        await update_user(user_id, new_user)
        
        # Реферальная система
        if len(message.text.split()) > 1:
            try:
                referrer_id = int(message.text.split()[1])
                if referrer_id != user_id and await get_user(referrer_id):
                    new_user["referrer"] = referrer_id
                    referrer = await get_user(referrer_id)
                    referrer["balance"] += 75  # Увеличен реферальный бонус
                    await update_user(referrer_id, referrer)
            except: pass

        welcome_msg = (
            "👑 <b>ДОБРО ПОЖАЛОВАТЬ В РАБОВЛАДЕЛЬЧЕСКУЮ ИМПЕРИЮ!</b>\n\n"
            "⚡️ <b>Основные возможности:</b>\n"
            "▸ 💼 Бонусная работа (раз в 15 мин)\n"  # Уменьшено время ожидания
            "▸ 🛠 Улучшай свои владения\n"
            "▸ 👥 Покупай рабов для пассивного дохода\n"
            "▸ 📈 Получай доход каждую минуту\n\n"
            "💰 <b>Стартовый баланс:</b> 150₽"
        )
        await message.answer(welcome_msg, reply_markup=main_keyboard())
    else:
        await message.answer("🔮 Главное меню:", reply_markup=main_keyboard())

@dp.callback_query(F.data.startswith(CHECK_SUB))
async def check_sub_callback(callback: types.CallbackQuery):
    user_id = int(callback.data.replace(CHECK_SUB, ""))
    if await check_subscription(user_id):
        user = await get_user(user_id) or {}
        if not user:
            new_user = {
                "balance": 150,
                "slaves": [],
                "owner": None,
                "base_price": 150,
                "enslaved_date": None,
                "slave_level": 0,
                "price": 150,
                "last_work": None,
                "upgrades": {
                    "storage": 0,
                    "whip": 0,
                    "food": 0,
                    "barracks": 0
                },
                "total_income": 0,
                "username": callback.from_user.username,
                "shield_active": None,
                "shackles": {},
                "shop_purchases": 0,
                "last_passive": datetime.now(),
                "referrer": None
            }
            await update_user(user_id, new_user)
        await callback.message.edit_text("✅ Регистрация завершена!")
        await callback.message.answer("🔮 Главное меню:", reply_markup=main_keyboard())
    else:
        await callback.answer("❌ Вы не подписаны!", show_alert=True)

@dp.callback_query(F.data == WORK)
async def work_handler(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    user = await get_user(user_id)
    
    if not user:
        await callback.answer("❌ Сначала зарегистрируйтесь!", show_alert=True)
        return
    
    # Новый баланс: уменьшено время перезарядки до 15 минут
    cooldown = timedelta(minutes=15)
    if user["last_work"] and (datetime.now() - user["last_work"]) < cooldown:
        remaining = (user["last_work"] + cooldown - datetime.now()).seconds // 60
        await callback.answer(f"⏳ Подождите еще {remaining} минут", show_alert=True)
        return
    
    # Пересчитанный доход с учетом балансировки
    passive_per_min = 1 + user.get("upgrades", {}).get("storage", 0) * 5
    for slave_id in user.get("slaves", []):
        slave = await get_user(slave_id)
        if slave:
            passive_per_min += 80 * (1 + 0.25 * slave.get("slave_level", 0))  # Уменьшен доход от рабов
    
    work_bonus = passive_per_min * 15 * (1 + user.get("upgrades", {}).get("whip", 0) * 0.2)
    user["balance"] += work_bonus
    user["total_income"] += work_bonus
    user["last_work"] = datetime.now()
    
    await update_user(user_id, user)
    await callback.message.edit_text(
        f"💼 Вы заработали: {work_bonus:.1f}₽\n"
        f"▸ Текущий пассив/мин: {passive_per_min:.1f}₽",
        reply_markup=main_keyboard()
    )
    # commands.py (Part 3/5)
@dp.callback_query(F.data == SEARCH_USER)
async def search_user_handler(callback: types.CallbackQuery):
    await callback.message.edit_text(
        "🔍 Введите @username игрока (можно без @):\n"
        "Пример: <code>username123</code>",
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[[InlineKeyboardButton(text="🔙 Назад", callback_data=BUY_MENU)]]
        ),
        parse_mode=ParseMode.HTML
    )
    await callback.answer()

@dp.message(F.text & ~F.text.startswith('/'))
async def process_username(message: Message):
    username = message.text.strip().lower().replace('@', '')
    
    # Поиск по базе данных
    found_user_id = None
    async with await get_db() as conn:
        users = await conn.fetch("SELECT user_id, data FROM bot_users")
        for record in users:
            user_data = deserialize_user_data(record['data'])
            if user_data.get('username', '').lower() == username:
                found_user_id = record['user_id']
                break

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔙 Назад", callback_data=BUY_MENU)]
    ])

    if not found_user_id:
        await message.reply("❌ Игрок не найден!", reply_markup=kb)
        return

    buyer_id = message.from_user.id
    if found_user_id == buyer_id:
        await message.reply("🌀 Нельзя купить самого себя!", reply_markup=kb)
        return

    slave = await get_user(found_user_id)
    owner_info = ""
    if slave.get('owner'):
        owner = await get_user(slave['owner'])
        owner_info = f"@{owner['username']}" if owner else "Система"

    price = slave['price']
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(
            text=f"💰 Купить за {price}₽ (Ур. {slave['slave_level']})",
            callback_data=f"{SLAVE_PREFIX}{found_user_id}"
        )],
        [InlineKeyboardButton(text="🔙 Назад", callback_data=BUY_MENU)]
    ])

    await message.reply(
        f"🔎 <b>Найден раб:</b>\n"
        f"▸ Ник: @{slave['username']}\n"
        f"▸ Уровень: {slave['slave_level']}\n"
        f"▸ Цена: {price}₽\n"
        f"▸ Владелец: {owner_info}\n\n"
        f"💡 Доход: {80 * (1 + 0.25 * slave['slave_level'])}₽/цикл",
        reply_markup=kb,
        parse_mode=ParseMode.HTML
    )

@dp.callback_query(F.data.startswith(SLAVE_PREFIX))
async def buy_slave_handler(callback: types.CallbackQuery):
    try:
        buyer_id = callback.from_user.id
        slave_id = int(callback.data.replace(SLAVE_PREFIX, ""))
        
        # Атомарная транзакция
        async with await get_db() as conn:
            # Получаем свежие данные
            buyer = await get_user(buyer_id)
            slave = await get_user(slave_id)

            # Проверка щита
            if slave.get('shield_active') and slave['shield_active'] > datetime.now():
                shield_time = slave['shield_active'].strftime("%d.%m %H:%M")
                await callback.answer(f"🛡 Защита до {shield_time}!", show_alert=True)
                return

            # Проверка баланса
            price = slave['price']
            if buyer['balance'] < price:
                await callback.answer(f"❌ Нужно {price}₽!", show_alert=True)
                return

            # Обновление данных
            buyer['balance'] -= price
            buyer['slaves'].append(slave_id)
            slave['owner'] = buyer_id
            slave['slave_level'] = min(slave['slave_level'] + 1, 10)
            slave['price'] = int(slave['base_price'] * (1.5 ** slave['slave_level']))

            # Сохранение изменений
            await update_user(buyer_id, buyer)
            await update_user(slave_id, slave)

            # Уведомление раба
            try:
                await bot.send_message(
                    slave_id,
                    f"⚡ Вас купил @{buyer['username']} за {price}₽"
                )
            except Exception as e:
                logging.error(f"Notification error: {e}")

            await callback.message.edit_text(
                f"✅ Успешная покупка @{slave['username']}!\n"
                f"▸ Новый уровень: {slave['slave_level']}\n"
                f"▸ Новая цена: {slave['price']}₽",
                reply_markup=main_keyboard()
            )

    except Exception as e:
        logging.error(f"Buy slave error: {e}", exc_info=True)
        await callback.answer("❌ Ошибка транзакции!", show_alert=True)

@dp.callback_query(F.data.startswith(BUYOUT_PREFIX))
async def buyout_handler(callback: types.CallbackQuery):
    try:
        user_id = callback.from_user.id
        user = await get_user(user_id)
        
        if not user.get('owner'):
            await callback.answer("🎉 Вы уже свободны!", show_alert=True)
            return

        # Расчет цены выкупа
        base_price = user['base_price']
        buyout_price = int(
            (base_price + user['balance'] * 0.15) *  # 15% от баланса
            (1 + user['slave_level'] * 0.35)        # 35% за уровень
        )
        buyout_price = max(200, min(25000, buyout_price))  # Новые лимиты

        async with await get_db() as conn:
            # Обновление данных
            user['balance'] -= buyout_price
            user['owner'] = None
            owner = await get_user(user['owner'])

            if owner:
                owner['slaves'].remove(user_id)
                owner['balance'] += int(buyout_price * 0.65)  # 65% владельцу
                await update_user(user['owner'], owner)

            await update_user(user_id, user)

            await callback.message.edit_text(
                f"🎉 Свобода за {buyout_price}₽!\n"
                f"▸ Сохранён уровень: {user['slave_level']}\n"
                f"▸ Новый лимит рабов: {3 + user.get('upgrades', {}).get('barracks', 0) * 3}",
                reply_markup=main_keyboard()
            )

    except Exception as e:
        logging.error(f"Buyout error: {e}", exc_info=True)
        await callback.answer("🌀 Ошибка выкупа", show_alert=True)
        # commands.py (Part 4/5)
async def calculate_shield_price(user_id: int) -> int:
    user = await get_user(user_id)
    if not user:
        return 0
    
    # Базовая формула: 6 часов пассивного дохода
    passive_income = 1 + user.get("upgrades", {}).get("storage", 0) * 5  # Склад
    for slave_id in user.get("slaves", []):
        slave = await get_user(slave_id)
        if slave:
            passive_income += 80 * (1 + 0.25 * slave.get("slave_level", 0))
    
    price = int(passive_income * 60 * 6)  # 6 часов в минутах
    price = max(500, min(15000, price))   # Лимиты цены
    
    # Скидка 25% для первых 3 покупок
    if user.get("shop_purchases", 0) < 3:
        price = int(price * 0.75)
    
    return (price // 100) * 100  # Округление до сотен

async def calculate_shackles_price(slave_id: int) -> int:
    slave = await get_user(slave_id)
    if not slave:
        return 0
    
    # Формула: 200% от стоимости раба
    price = int(slave["price"] * 2.0)
    return max(1000, min(30000, price))

@dp.callback_query(F.data == "shop")
async def shop_handler(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    user = await get_user(user_id)
    
    shield_price = await calculate_shield_price(user_id)
    shield_status = (
        "🟢 До %s" % user["shield_active"].strftime("%d.%m %H:%M") 
        if user.get("shield_active") and user["shield_active"] > datetime.now()
        else "🔴 Неактивен"
    )
    
    text = [
        "🛒 <b>Магический рынок</b>",
        "",
        f"🛡 <b>Щит свободы</b> ({shield_status})",
        f"▸ Защита от покупки на 24 часа",
        f"▸ Цена: {shield_price}₽",
        "",
        "⛓ <b>Адские кандалы</b>",
        "▸ Блокируют выкуп раба на 48 часов",
        "▸ Цена: индивидуально для каждого раба"
    ]
    
    buttons = [
        [InlineKeyboardButton(
            text=f"🛡 Купить щит — {shield_price}₽",
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

@dp.callback_query(F.data.startswith(SHIELD_PREFIX))
async def buy_shield(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    user = await get_user(user_id)
    price = int(callback.data.replace(SHIELD_PREFIX, ""))
    
    if user["balance"] < price:
        await callback.answer("❌ Недостаточно средств!", show_alert=True)
        return
    
    # Проверка активного щита
    if user.get("shield_active") and user["shield_active"] > datetime.now():
        await callback.answer("⚠️ Щит уже активен!", show_alert=True)
        return
    
    # Обновление данных
    user["balance"] -= price
    user["shield_active"] = datetime.now() + timedelta(hours=24)
    user["shop_purchases"] = user.get("shop_purchases", 0) + 1
    await update_user(user_id, user)
    
    await callback.answer(f"🛡 Щит активен до {user['shield_active'].strftime('%d.%m %H:%M')}!", show_alert=True)
    await shop_handler(callback)

@dp.callback_query(F.data == "select_shackles")
async def select_shackles(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    user = await get_user(user_id)
    
    if not user or len(user.get("slaves", [])) == 0:
        await callback.answer("❌ У вас нет рабов!", show_alert=True)
        return
    
    buttons = []
    for slave_id in user["slaves"][:10]:  # Ограничение 10 рабов
        slave = await get_user(slave_id)
        if not slave:
            continue
        
        price = await calculate_shackles_price(slave_id)
        buttons.append([
            InlineKeyboardButton(
                text=f"⛓ @{slave['username']} — {price}₽",
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
    _, slave_id, price = callback.data.split("_")
    slave_id = int(slave_id)
    price = int(price)
    user_id = callback.from_user.id
    user = await get_user(user_id)
    
    if user["balance"] < price:
        await callback.answer("❌ Недостаточно средств!", show_alert=True)
        return
    
    # Применение кандалов
    user["balance"] -= price
    if "shackles" not in user:
        user["shackles"] = {}
    user["shackles"][slave_id] = datetime.now() + timedelta(hours=48)
    await update_user(user_id, user)
    
    await callback.answer(
        f"⛓ Кандалы применены к @{slave['username']} на 48 часов!",
        show_alert=True
    )
    await select_shackles(callback)

@dp.callback_query(F.data.startswith(UPGRADE_PREFIX))
async def upgrade_handler(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    upgrade_id = callback.data.replace(UPGRADE_PREFIX, "")
    user = await get_user(user_id)
    
    if not user or upgrade_id not in upgrades:
        await callback.answer("❌ Ошибка!", show_alert=True)
        return
    
    level = user.get("upgrades", {}).get(upgrade_id, 0)
    data = upgrades[upgrade_id]
    price = int(data["base_price"] * (data["multiplier"] ** level))
    
    if user["balance"] < price:
        await callback.answer(f"❌ Нужно {price}₽!", show_alert=True)
        return
    
    # Применение улучшения
    user["balance"] -= price
    user["upgrades"][upgrade_id] = level + 1
    
    # Спецэффекты
    if upgrade_id == "barracks":
        max_slaves = 5 + user["upgrades"]["barracks"] * 3
        if len(user["slaves"]) > max_slaves:
            user["slaves"] = user["slaves"][:max_slaves]
    
    await update_user(user_id, user)
    
    await callback.message.edit_reply_markup(
        reply_markup=await upgrades_keyboard(user_id)
    )
    await callback.answer(f"✅ {data['name']} улучшен до уровня {level + 1}!")
    # commands.py (Part 5/5)
@dp.callback_query(F.data == PROFILE)
async def profile_handler(callback: types.CallbackQuery):
    try:
        user_id = callback.from_user.id
        user = await get_user(user_id)
        
        if not user:
            await callback.answer("❌ Сначала зарегистрируйтесь!", show_alert=True)
            return
        
        # Расчет пассивного дохода
        passive_income = 1 + user.get("upgrades", {}).get("storage", 0) * 5
        for slave_id in user.get("slaves", []):
            slave = await get_user(slave_id)
            if slave:
                passive_income += 80 * (1 + 0.25 * slave.get("slave_level", 0))
        
        # Расчет цены выкупа
        buyout_price = 0
        if user.get("owner"):
            base_price = user["base_price"]
            buyout_price = int(
                (base_price + user["balance"] * 0.15) * 
                (1 + user["slave_level"] * 0.35)
            )
            buyout_price = max(200, min(25000, buyout_price))
        
        # Формирование профиля
        text = [
            f"👤 <b>Профиль @{user['username']}</b>",
            f"▸ 💰 Баланс: {user['balance']:.1f}₽",
            f"▸ 🕒 Пассив/час: {passive_income * 60:.1f}₽",
            f"▸ 🛠 Улучшения: {sum(user['upgrades'].values())}",
            f"▸ 👥 Рабов: {len(user['slaves'])}/{5 + user['upgrades'].get('barracks', 0)*3}"
        ]
        
        if user.get("owner"):
            owner = await get_user(user["owner"])
            text.append(
                f"\n⚠️ <b>Вы раб @{owner['username'] if owner else 'Система'}</b>\n"
                f"▸ Уровень рабства: {user['slave_level']}\n"
                f"▸ Цена выкупа: {buyout_price}₽"
            )
            kb = [[InlineKeyboardButton(
                text=f"🆓 Выкупиться за {buyout_price}₽", 
                callback_data=f"{BUYOUT_PREFIX}{buyout_price}"
            )]]
        else:
            text.append("\n🌟 <b>Статус:</b> Свободный гражданин")
            kb = []
        
        kb.append([InlineKeyboardButton(text="🔙 Назад", callback_data=MAIN_MENU)])
        
        await callback.message.edit_text(
            "\n".join(text),
            reply_markup=InlineKeyboardMarkup(inline_keyboard=kb),
            parse_mode=ParseMode.HTML
        )
        
    except Exception as e:
        logging.error(f"Profile error: {e}", exc_info=True)
        await callback.answer("❌ Ошибка загрузки профиля", show_alert=True)
    await callback.answer()

@dp.callback_query(F.data == TOP_OWNERS)
async def top_owners_handler(callback: types.CallbackQuery):
    try:
        async with await get_db() as conn:
            records = await conn.fetch("SELECT data FROM bot_users")
            users_data = [deserialize_user_data(r['data']) for r in records]
        
        # Расчет эффективности
        top_list = []
        for user in users_data:
            if slaves_count := len(user.get("slaves", [])):
                efficiency = user.get("total_income", 0) / slaves_count
                top_list.append({
                    "username": user.get("username", "Unknown"),
                    "efficiency": efficiency,
                    "slaves": slaves_count,
                    "income": user.get("total_income", 0)
                })
        
        # Сортировка и выбор топ-15
        sorted_top = sorted(top_list, key=lambda x: x["efficiency"], reverse=True)[:15]
        
        # Формирование сообщения
        text = ["🏆 <b>Топ рабовладельцев</b>\n▸ Эффективность = Доход/Раба\n"]
        for idx, item in enumerate(sorted_top, 1):
            text.append(
                f"{idx}. @{item['username']}\n"
                f"   ▸ Эфф.: {item['efficiency']:.1f}₽\n"
                f"   ▸ Рабов: {item['slaves']} | Доход: {item['income']}₽\n"
            )
        
        await callback.message.edit_text(
            "\n".join(text)[:4096],  # Ограничение Telegram
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[[InlineKeyboardButton(text="🔙 Назад", callback_data=MAIN_MENU)]]
            ),
            parse_mode=ParseMode.HTML
        )
        
    except Exception as e:
        logging.error(f"Top error: {e}", exc_info=True)
        await callback.answer("🌀 Ошибка загрузки топа", show_alert=True)
    await callback.answer()

async def passive_income_task():
    while True:
        await asyncio.sleep(60)
        try:
            async with await get_db() as conn:
                records = await conn.fetch("SELECT user_id, data FROM bot_users")
                for record in records:
                    user = deserialize_user_data(record['data'])
                    if not user.get("last_passive"):
                        continue
                        
                    mins_passed = (datetime.now() - user["last_passive"]).total_seconds() / 60
                    if mins_passed < 1:
                        continue
                    
                    # Расчет дохода
                    income = 1 + user.get("upgrades", {}).get("storage", 0) * 5
                    for slave_id in user.get("slaves", []):
                        slave = await get_user(slave_id)
                        if slave:
                            income += 80 * (1 + 0.25 * slave.get("slave_level", 0))
                    
                    total_income = income * mins_passed
                    user["balance"] += total_income
                    user["total_income"] += total_income
                    user["last_passive"] = datetime.now()
                    
                    # Автосохранение
                    await update_user(record['user_id'], user)
                    
        except Exception as e:
            logging.error(f"Passive income error: {e}")

async def autosave_task():
    while True:
        await asyncio.sleep(300)
        logging.info("Autosave completed")
        
async def on_startup():
    await init_db()
    asyncio.create_task(passive_income_task())
    asyncio.create_task(autosave_task())
    logging.info("Bot started")

async def on_shutdown():
    logging.info("Bot stopped")

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[logging.FileHandler("bot.log"), logging.StreamHandler()]
    )
    
    try:
        asyncio.run(dp.start_polling(bot))
    except KeyboardInterrupt:
        pass
    finally:
        asyncio.run(on_shutdown())
