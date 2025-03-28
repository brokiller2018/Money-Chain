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

# Настройки из вашего кода
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

# Улучшения с вашими параметрами + описаниями
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
        "description": "+25% к доходу от рабов"
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

# Клавиатуры (аналогично вашему коду с добавлением поиска)
def main_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💼 Работать", callback_data=WORK)],
        [InlineKeyboardButton(text="🛠 Улучшения", callback_data=UPGRADES),
         InlineKeyboardButton(text="📊 Профиль", callback_data=PROFILE)],
        [InlineKeyboardButton(text="👥 Купить раба", callback_data=BUY_MENU)],
        [InlineKeyboardButton(text="🔗 Рефералка", callback_data=REF_LINK)]
    ])

def buy_menu_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔍 Поиск по юзернейму", callback_data=SEARCH_USER)],
        [InlineKeyboardButton(text="🔙 Назад", callback_data=MAIN_MENU)]
    ])

# Обработчики команд с новыми функциями
@dp.message(Command('start'))
async def start_command(message: Message):
    user_id = message.from_user.id
    
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
            "username": message.from_user.username,
            "last_passive": datetime.now(),
            "income_per_min": 1  # Базовый пассивный доход
        }
        
        guide = """🎮 <b>Добро пожаловать в Slave Empire!</b>

💰 <b>Основные механики:</b>
1. Пассивный доход: получаете монеты автоматически
2. /work - основной заработок (20 мин кд)
3. Улучшения: увеличивают доход
4. Рабы: купите других игроков для прибыли

📊 <b>Доход рассчитывается:</b>
• Работа: 50 + бонусы
• Пассивка: 1/мин + бонусы
• Рабы: 100/раб + бонусы

🛠 <b>Улучшения:</b>
• 📦 Склад: +10/мин за уровень
• ⛓ Кнуты: +25% к рабам
• 🍗 Еда: -10% к кд работы
• 🏠 Бараки: +5 к лимиту рабов"""
        
        await message.answer(guide, reply_markup=main_keyboard())
    else:
        await message.answer("🔮 Главное меню:", reply_markup=main_keyboard())

# Пассивный доход (новый функционал)
async def passive_income():
    while True:
        await asyncio.sleep(60)
        now = datetime.now()
        for user_id, user_data in users.items():
            if "last_passive" in user_data:
                mins_passed = (now - user_data["last_passive"]).total_seconds() / 60
                if mins_passed >= 1:
                    # Базовый 1/мин + 10/мин за каждый уровень склада
                    income = (1 + user_data["upgrades"]["storage"] * 10) * mins_passed
                    user_data["balance"] += income
                    user_data["last_passive"] = now
                    user_data["income_per_min"] = income / mins_passed
                    
                    try:
                        await bot.send_message(
                            user_id,
                            f"⏳ Пассивный доход: +{income:.2f} монет"
                        )
                    except:
                        pass

# Обновлённый профиль
@dp.callback_query(F.data == PROFILE)
async def profile_handler(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    user = users.get(user_id)
    
    if not user:
        await callback.answer("❌ Сначала зарегистрируйтесь!", show_alert=True)
        return
    
    slaves_list = "\n".join([
        f"  ▪️ @{users[uid]['username']}" 
        for uid in user["slaves"][:3]
    ])
    if len(user["slaves"]) > 3:
        slaves_list += f"\n  ...и ещё {len(user['slaves']) - 3}"
    
    profile_text = (
        f"👤 <b>Профиль @{user['username']}</b>\n"
        f"💰 Баланс: {user['balance']:.2f} монет\n"
        f"⏳ Доход: {user['income_per_min']/60:.2f} монет/сек\n"
        f"🧷 Рабы: {len(user['slaves'])}\n{slaves_list}\n"
        f"👑 Владелец: @{users[user['owner']]['username'] if user['owner'] else 'Отсутствует'}\n"
        f"📈 Всего заработано: {user['total_income']}"
    )
    
    await callback.message.edit_text(profile_text, reply_markup=main_keyboard())
    await callback.answer()

# Поиск по юзернейму (новый функционал)
@dp.callback_query(F.data == SEARCH_USER)
async def search_user_handler(callback: types.CallbackQuery):
    await callback.message.edit_text(
        "🔍 Введите @username игрока без собачки:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔙 Назад", callback_data=BUY_MENU)]
        ])
    )
    await callback.answer()

@dp.message(F.text & ~F.text.startswith('/') & ~F.text.startswith('@'))
async def process_username(message: Message):
    username = message.text.lower().strip()
    buyer_id = message.from_user.id
    
    # Ищем пользователя
    found = None
    for uid, data in users.items():
        if data["username"] and data["username"].lower() == username:
            found = uid
            break
    
    if not found:
        return await message.reply("❌ Пользователь не найден")
    
    if found == buyer_id:
        return await message.reply("🤡 Нельзя купить самого себя!")
    
    slave = users[found]
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(
            text=f"💰 Купить за {slave['price']} монет",
            callback_data=f"{SLAVE_PREFIX}{found}"
        )],
        [InlineKeyboardButton(text="🔙 Назад", callback_data=BUY_MENU)]
    ])
    
    await message.reply(
        f"🔎 Найден: @{slave['username']}\n"
        f"💵 Цена: {slave['price']} монет\n"
        f"👑 Владелец: @{users[slave['owner']]['username'] if slave['owner'] else 'Свободен'}",
        reply_markup=kb
    )

# Запуск пассивного дохода
async def on_startup():
    asyncio.create_task(passive_income())

async def main():
    await on_startup()
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
