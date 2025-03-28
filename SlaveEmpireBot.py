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

# Настройки (ваши данные сохранены)
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

# Улучшения с подробными описаниями и эффектами
upgrades = {
    "storage": {
        "name": "📦 Склад",
        "base_price": 500,
        "income_bonus": 10,
        "description": "+10 монет/мин к пассивному доходу за уровень",
        "effect": "passive"
    },
    "whip": {
        "name": "⛓ Кнуты", 
        "base_price": 1000,
        "income_bonus": 25,
        "description": "+25% к доходу от работы за уровень",
        "effect": "work"
    },
    "food": {
        "name": "🍗 Еда",
        "base_price": 2000,
        "income_bonus": 50,
        "description": "-10% к времени ожидания работы за уровень",
        "effect": "cooldown"
    },
    "barracks": {
        "name": "🏠 Бараки",
        "base_price": 5000,
        "income_bonus": 100,
        "description": "+5 к максимальному количеству рабов за уровень",
        "effect": "slaves"
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
    user = users[user_id]
    for upgrade_id, data in upgrades.items():
        level = user["upgrades"].get(upgrade_id, 0)
        price = data["base_price"] * (level + 1)
        effect = ""
        
        if data["effect"] == "passive":
            effect = f"(+{data['income_bonus']*{level+1}/мин)"
        elif data["effect"] == "work":
            effect = f"(+{data['income_bonus']*level}%)"
        
        buttons.append([
            InlineKeyboardButton(
                text=f"{data['name']} ⟠{level} • {price}₭ {effect}",
                callback_data=f"{UPGRADE_PREFIX}{upgrade_id}"
            )
        ])
    buttons.append([InlineKeyboardButton(text="🔙 Назад", callback_data=MAIN_MENU)])
    return InlineKeyboardMarkup(inline_keyboard=buttons)

# Пассивный доход
async def passive_income_task():
    while True:
        await asyncio.sleep(60)
        now = datetime.now()
        for user_id, user in users.items():
            if "last_passive" in user:
                time_diff = (now - user["last_passive"]).total_seconds() / 60
                if time_diff >= 1:
                    # Расчет пассивного дохода
                    passive = (1 + user["upgrades"]["storage"] * 10) * time_diff
                    user["balance"] += passive
                    user["last_passive"] = now
                    user["total_income"] += passive
                    
                    try:
                        await bot.send_message(
                            user_id,
                            f"⏳ Пассивный доход: +{passive:.1f}₭\n"
                            f"▸ Склад: {user['upgrades']['storage']} ур."
                        )
                    except Exception as e:
                        logging.error(f"Ошибка отправки: {e}")

# Приветственное сообщение (полностью переработано)
WELCOME_MESSAGE = """
🎮 <b>ДОБРО ПОЖАЛОВАТЬ В РАБОВЛАДЕЛЬЧЕСКУЮ ИМПЕРИЮ!</b> 👑

⚡️ <i>Стань самым могущественным правителем в этом жестоком мире!</i>

✨ <b>Основные возможности:</b>
▸ 💼 Зарабатывай монеты работой
▸ 🛠 Улучшай свои владения
▸ 👥 Покупай других игроков
▸ 📈 Получай пассивный доход

🚀 <b>Быстрый старт:</b>
1. Используй кнопку <b>💼 Работать</b> каждые 20 мин
2. Вкладывайся в улучшения
3. Приглашай друзей по рефералке
4. Захватывай рабов для дохода

📊 <b>Доход в секунду:</b>
▸ Базовая ставка: <i>0.016₭/сек</i>
▸ Увеличивается улучшениями
▸ Рабы дают бонусы

⚔️ <b>Твой путь к величию начинается сейчас!</b>
"""

# Обработчик старта
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
            "income_per_sec": 0.0167  # 1 монета в минуту
        }
        await message.answer(WELCOME_MESSAGE, reply_markup=main_keyboard())
        await message.answer(
            f"👤 <b>@{username}</b>, твои стартовые ресурсы:\n"
            f"▸ 💰 100 монет\n"
            f"▸ 🛠 0 улучшений\n"
            f"▸ 👥 0 рабов\n\n"
            f"⚡️ Используй меню ниже для управления!",
            reply_markup=main_keyboard()
        )
    else:
        await message.answer("🔮 Главное меню:", reply_markup=main_keyboard())

# Поиск по юзернейму (новая реализация)
@dp.callback_query(F.data == SEARCH_USER)
async def search_user_handler(callback: types.CallbackQuery):
    await callback.message.edit_text(
        "🔍 <b>Поиск раба:</b>\n"
        "Введите @username игрока (без собачки):",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔙 Назад", callback_data=BUY_MENU)]
        ])
    await callback.answer()

@dp.message(F.text & ~F.text.startswith('/'))
async def process_username(message: Message):
    username = message.text.strip().lower()
    buyer_id = message.from_user.id
    
    if not username.startswith('@'):
        username = '@' + username
    
    found_user = None
    for uid, data in users.items():
        if data["username"] and data["username"].lower() == username[1:].lower():
            found_user = uid
            break
    
    if not found_user:
        return await message.reply("❌ Игрок не найден!")
    
    if found_user == buyer_id:
        return await message.reply("🌀 Нельзя купить самого себя!")
    
    slave = users[found_user]
    price = slave["price"]
    
    text = (
        f"🔎 <b>Результаты поиска:</b>\n\n"
        f"▸ Игрок: @{slave['username']}\n"
        f"▸ Стоимость: {price}₭\n"
        f"▸ Владелец: @{users[slave['owner']]['username'] if slave['owner'] else 'Свободен'}\n"
        f"▸ Уровни улучшений: {sum(slave['upgrades'].values()}\n\n"
        f"💡 Цена увеличивается на 50% после покупки"
    )
    
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"💰 Купить за {price}₭", callback_data=f"{SLAVE_PREFIX}{found_user}")],
        [InlineKeyboardButton(text="🔙 Назад", callback_data=BUY_MENU)]
    ])
    
    await message.reply(text, reply_markup=kb)

# Обновленный профиль
@dp.callback_query(F.data == PROFILE)
async def profile_handler(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    user = users.get(user_id)
    
    if not user:
        await callback.answer("❌ Сначала зарегистрируйтесь!", show_alert=True)
        return
    
    slaves_count = len(user["slaves"])
    max_slaves = 5 + user["upgrades"]["barracks"] * 5
    income_per_sec = (1 + user["upgrades"]["storage"] * 10) / 60
    
    text = (
        f"👑 <b>Профиль @{user['username']}</b>\n\n"
        f"▸ 💰 Баланс: {user['balance']:.1f}₭\n"
        f"▸ ⚡ Доход/сек: {income_per_sec:.3f}₭\n"
        f"▸ 👥 Рабы: {slaves_count}/{max_slaves}\n"
        f"▸ 🛠 Улучшения: {sum(user['upgrades'].values())}\n"
        f"▸ 📈 Всего заработано: {user['total_income']:.1f}₭\n\n"
    )
    
    if user["owner"]:
        text += f"🔗 Владелец: @{users[user['owner']]['username']}\n"
    else:
        text += "🔗 Вы свободный человек!\n"
    
    if slaves_count > 0:
        text += "\n<b>Топ рабов:</b>\n"
        for uid in user["slaves"][:3]:
            text += f"▸ @{users[uid]['username']} ({users[uid]['price']}₭)\n"
    
    await callback.message.edit_text(text, reply_markup=main_keyboard())
    await callback.answer()

# Запуск пассивного дохода
async def on_startup():
    asyncio.create_task(passive_income_task())

# Остальные обработчики остаются без изменений...

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(on_startup())
    asyncio.run(dp.start_polling(bot))
