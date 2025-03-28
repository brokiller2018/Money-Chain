import logging
import asyncio
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
            InlineKeyboardButton(text="🔗 Рефералка", callback_data=REF_LINK)
        ]
    ])

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
                if mins_passed >= 1:
                    income = (1 + user.get("upgrades", {}).get("storage", 0) * 10) * mins_passed
                    user["balance"] += income
                    user["last_passive"] = now
                    user["total_income"] += income
            

# Обработчики команд
@dp.message(Command('start'))
async def start_command(message: Message):
    user_id = message.from_user.id
    username = message.from_user.username
    
    if not await check_subscription(user_id):
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔔 Подписаться", url=CHANNEL_LINK)],
            [InlineKeyboardButton(text="✅ Я подписался", callback_data=f"{CHECK_SUB}{user_id}")]
        ])
        await message.answer("📌 Для доступа подпишитесь на канал:", reply_markup=kb)
        return
    
    if user_id not in users:
        users[user_id] = {
            "balance": 100,
            "slaves": [],
            "owner": None,
            "price": 100,
            "last_work": None,
            "upgrades": {key: 0 for key in upgrades},
            "total_income": 0,
            "username": username,
            "last_passive": datetime.now(),
            "income_per_sec": 0.0167
        }
        
        welcome_msg = """
🎮 <b>ДОБРО ПОЖАЛОВАТЬ В РАБОВЛАДЕЛЬЧЕСКУЮ ИМПЕРИЮ!</b> 👑

⚡️ <b>Основные возможности:</b>
▸ 💼 Зарабатывай монеты работой (каждые 20 мин)
▸ 🛠 Улучшай свои владения
▸ 👥 Покупай других игроков
▸ 📈 Получай пассивный доход (1₽/мин)

💰 <b>Доход в секунду:</b> 0.016₽
        """
        
        await message.answer(welcome_msg, reply_markup=main_keyboard())
    else:
        await message.answer("🔮 Главное меню:", reply_markup=main_keyboard())

@dp.callback_query(F.data.startswith(CHECK_SUB))
async def check_sub_callback(callback: types.CallbackQuery):
    user_id = int(callback.data.replace(CHECK_SUB, ""))
    if await check_subscription(user_id):
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
                "income_per_sec": 0.0167
            }
        await callback.message.edit_text("✅ Регистрация завершена!")
        await callback.message.answer("🔮 Главное меню:", reply_markup=main_keyboard())
    else:
        await callback.answer("❌ Вы не подписаны!", show_alert=True)
    await callback.answer()

@dp.callback_query(F.data == SEARCH_USER)
async def search_user_handler(callback: types.CallbackQuery):
    await callback.message.edit_text(
        "🔍 Введите @username игрока (без собачки):",
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="🔙 Назад", callback_data=BUY_MENU)]
            ]
        )
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
    
    base_income = 50
    slaves_income = len(user["slaves"]) * 100
    upgrades_bonus = sum(
        user.get("upgrades", {}).get(upgrade, 0) * data["income_bonus"]
        for upgrade, data in upgrades.items()
    )
    
    total_income = base_income + slaves_income + upgrades_bonus
    user["balance"] += total_income
    user["last_work"] = now
    user["total_income"] += total_income
    
    await callback.message.edit_text(
        f"💼 Заработано: {total_income}₽\n\n"
        f"📊 Разбивка:\n"
        f"• База: {base_income}₽\n"
        f"• Рабы: {slaves_income}₽\n"
        f"• Улучшения: {upgrades_bonus}₽",
        reply_markup=main_keyboard()
    )
    await callback.answer()
    
@dp.message(F.text & ~F.text.startswith('/'))
async def process_username(message: Message):
    username = message.text.strip().lower().replace('@', '')  # Удаляем @ если есть
    buyer_id = message.from_user.id
    
    found_user = None
    for uid, data in users.items():
        if data.get("username", "").lower() == username:
            found_user = uid
            break
    
    if not found_user:
        return await message.reply("❌ Игрок не найден!")
    
    if found_user == buyer_id:
        return await message.reply("🌀 Нельзя купить самого себя!")
    
    slave = users.get(found_user, {})
    price = slave.get("price", 100)
    
    owner_info = "Свободен"
    if slave.get("owner"):
        owner_data = users.get(slave["owner"], {})
        owner_info = f"@{owner_data.get('username', 'unknown')}"
    
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"💰 Купить за {price}₽", callback_data=f"{SLAVE_PREFIX}{found_user}")],
        [InlineKeyboardButton(text="🔙 Назад", callback_data=BUY_MENU)]
    ])
    
    await message.reply(
        f"🔎 <b>Найден игрок:</b> @{slave.get('username', 'unknown')}\n"
        f"▸ Цена: {price}₽\n"
        f"▸ Владелец: {owner_info}",
        reply_markup=kb
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

@dp.callback_query(F.data.startswith(UPGRADE_PREFIX))
async def upgrade_handler(callback: types.CallbackQuery):
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
    
    current_level = user["upgrades"].get(upgrade_id, 0)
    price = upgrade_data["base_price"] * (current_level + 1)
    
    if user["balance"] < price:
        await callback.answer("❌ Недостаточно средств!", show_alert=True)
        return
    
    user["balance"] -= price
    user["upgrades"][upgrade_id] = current_level + 1
    
    # Обновляем пассивный доход если это склад
    if upgrade_id == "storage":
        user["income_per_sec"] = (1 + user["upgrades"]["storage"] * 10) / 60
    
    await callback.message.edit_reply_markup(reply_markup=upgrades_keyboard(user_id))
    await callback.answer(f"✅ {upgrade_data['name']} улучшен до уровня {current_level + 1}!")

@dp.callback_query(F.data.startswith(SLAVE_PREFIX))
async def buy_slave_handler(callback: types.CallbackQuery):
    buyer_id = callback.from_user.id
    slave_id = int(callback.data.replace(SLAVE_PREFIX, ""))
    
    buyer = users.get(buyer_id)
    slave = users.get(slave_id)
    
    if not buyer or not slave:
        await callback.answer("❌ Ошибка: пользователь не найден", show_alert=True)
        return
    
    if slave_id == buyer_id:
        await callback.answer("❌ Нельзя купить самого себя!", show_alert=True)
        return
    
    if slave["owner"] is not None:
        owner = users.get(slave["owner"])
        await callback.answer(
            f"❌ Этот раб уже принадлежит @{owner.get('username', 'unknown')}",
            show_alert=True
        )
        return
    
    price = slave["price"]
    
    if buyer["balance"] < price:
        await callback.answer("❌ Недостаточно средств!", show_alert=True)
        return
    
    # Совершаем покупку
    buyer["balance"] -= price
    buyer["slaves"].append(slave_id)
    slave["owner"] = buyer_id
    slave["price"] = int(price * 1.5)  # Увеличиваем цену
    
    # Обновляем сообщение
    owner_info = f"@{buyer['username']}" if buyer["username"] else f"ID:{buyer_id}"
    await callback.message.edit_text(
        f"✅ Вы успешно купили @{slave['username']} за {price}₽!\n"
        f"Новая цена раба: {slave['price']}₽",
        reply_markup=main_keyboard()
    )
    await callback.answer()

# Обновленный профиль
@dp.callback_query(F.data == PROFILE)
async def profile_handler(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    user = users.get(user_id)
    
    if not user:
        await callback.answer("❌ Сначала зарегистрируйтесь!", show_alert=True)
        return
    
    slaves_count = len(user.get("slaves", []))
    max_slaves = 5 + user.get("upgrades", {}).get("barracks", 0) * 5
    income_per_sec = (1 + user.get("upgrades", {}).get("storage", 0) * 10) / 60
    
    text = (
        f"👑 <b>Профиль @{user.get('username', 'unknown')}</b>\n\n"
        f"▸ 💰 Баланс: {user.get('balance', 0):.1f}₽\n"
        f"▸ ⚡ Доход/сек: {income_per_sec:.3f}₽\n"
        f"▸ 👥 Рабы: {slaves_count}/{max_slaves}\n"
        f"▸ 🛠 Улучшения: {sum(user.get('upgrades', {}).values())}\n"
        f"▸ 📈 Всего заработано: {user.get('total_income', 0):.1f}₽\n\n"
    )
    
    if user.get("owner"):
        owner_username = users.get(user["owner"], {}).get("username", "unknown")
        text += f"🔗 Владелец: @{owner_username}\n"
    else:
        text += "🔗 Вы свободный человек!\n"
    
    if slaves_count > 0:
        text += "\n<b>Топ рабов:</b>\n"
        for uid in user.get("slaves", [])[:3]:
            slave_data = users.get(uid, {})
            text += f"▸ @{slave_data.get('username', 'unknown')} ({slave_data.get('price', 0)}₽)\n"
    
    await callback.message.edit_text(text, reply_markup=main_keyboard())
    await callback.answer()

async def on_startup():
    asyncio.create_task(passive_income_task())  # Теперь имя функции совпадает

async def main():
    await on_startup()
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
    )
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
