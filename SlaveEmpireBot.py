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

# Инициализация
bot = Bot(token=TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
storage = MemoryStorage()
dp = Dispatcher(storage=storage)

# База данных и улучшения
users = {}
upgrades = {
    "storage": {"name": "📦 Склад", "base_price": 500, "income_bonus": 10},
    "whip": {"name": "⛓ Кнуты", "base_price": 1000, "income_bonus": 25},
    "food": {"name": "🍗 Еда", "base_price": 2000, "income_bonus": 50},
    "barracks": {"name": "🏠 Бараки", "base_price": 5000, "income_bonus": 100}
}

# Клавиатуры
def main_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💼 Работать", callback_data="work")],
        [InlineKeyboardButton(text="🛠 Улучшения", callback_data="upgrades"),
         InlineKeyboardButton(text="📊 Профиль", callback_data="profile")],
        [InlineKeyboardButton(text="🔗 Рефералка", callback_data="ref_link")]
    ])

def upgrades_keyboard(user_id):
    buttons = []
    for upgrade_id, data in upgrades.items():
        level = users[user_id].get("upgrades", {}).get(upgrade_id, 0)
        price = data["base_price"] * (level + 1)
        buttons.append([
            InlineKeyboardButton(
                text=f"{data['name']} (Ур. {level}) - {price} монет",
                callback_data=f"buy_{upgrade_id}"
            )
        ])
    buttons.append([InlineKeyboardButton(text="🔙 Назад", callback_data="main_menu")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)

# Обработчики
@dp.message(Command('start'))
async def start_command(message: Message):
    user_id = message.from_user.id
    
    if not await check_subscription(user_id):
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔔 Подписаться", url=CHANNEL_LINK)],
            [InlineKeyboardButton(text="✅ Я подписался", callback_data=f"check_sub_{user_id}")]
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
            "total_income": 0
        }
        await message.answer("🎮 Добро пожаловать в Slave Empire!", reply_markup=main_keyboard())
    else:
        await message.answer("🔮 Главное меню:", reply_markup=main_keyboard())

@dp.callback_query(F.data.startswith("check_sub_"))
async def check_sub_callback(callback: types.CallbackQuery):
    user_id = int(callback.data.split("_")[-1])
    if await check_subscription(user_id):
        if user_id not in users:
            users[user_id] = {
                "balance": 100,
                "slaves": [],
                "owner": None,
                "price": 100,
                "last_work": None,
                "upgrades": {key: 0 for key in upgrades},
                "total_income": 0
            }
            await callback.message.edit_text("✅ Регистрация завершена!")
            await callback.message.answer("🔮 Главное меню:", reply_markup=main_keyboard())
    else:
        await callback.answer("❌ Вы не подписаны!", show_alert=True)
    await callback.answer()

@dp.callback_query(F.data == "work")
async def work_handler(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    user = users.get(user_id)
    
    if not user:
        await callback.answer("❌ Сначала зарегистрируйтесь!")
        return
    
    now = datetime.now()
    cooldown = timedelta(minutes=20)
    
    if user["last_work"] and (now - user["last_work"]) < cooldown:
        remaining = (user["last_work"] + cooldown - now).seconds // 60
        await callback.answer(f"⏳ Подождите еще {remaining} минут", show_alert=True)
        return
    
    # Расчет дохода
    base_income = 50
    slaves_income = len(user["slaves"]) * 100
    upgrades_bonus = sum(
        users[user_id]["upgrades"][upgrade] * data["income_bonus"]
        for upgrade, data in upgrades.items()
    )
    
    total_income = base_income + slaves_income + upgrades_bonus
    users[user_id]["balance"] += total_income
    users[user_id]["last_work"] = now
    users[user_id]["total_income"] += total_income
    
    await callback.message.edit_text(
        f"💼 Заработано: {total_income} монет\n\n"
        f"📊 Разбивка:\n"
        f"• База: {base_income}\n"
        f"• Рабы: {slaves_income}\n"
        f"• Улучшения: {upgrades_bonus}",
        reply_markup=main_keyboard()
    )
    await callback.answer()

@dp.callback_query(F.data == "upgrades")
async def upgrades_handler(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    if user_id not in users:
        await callback.answer("❌ Сначала зарегистрируйтесь!", show_alert=True)
        return
    
    await callback.message.edit_text( 
        "🛠 Улучшения увеличивают доход от работы:",
        reply_markup=upgrades_keyboard(user_id)  
    )
    await callback.answer()

@dp.callback_query(F.data.startswith("buy_"))
async def buy_upgrade(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    upgrade_id = callback.data.split("_")[1]
    
    if upgrade_id not in upgrades:
        await callback.answer("❌ Улучшение не найдено", show_alert=True)
        return
    
    current_level = users[user_id]["upgrades"].get(upgrade_id, 0)
    price = upgrades[upgrade_id]["base_price"] * (current_level + 1)
    
    if users[user_id]["balance"] < price:
        await callback.answer(f"❌ Не хватает {price - users[user_id]['balance']} монет", show_alert=True)
        return
    
    users[user_id]["balance"] -= price
    users[user_id]["upgrades"][upgrade_id] += 1
    
    await callback.message.edit_text(  
        f"🎉 {upgrades[upgrade_id]['name']} улучшено до уровня {users[user_id]['upgrades'][upgrade_id]}!",
        reply_markup=upgrades_keyboard(user_id)  
    )
    await callback.answer()

@dp.callback_query(F.data == "profile")
async def profile_handler(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    user = users.get(user_id)
    
    if not user:
        await callback.answer("❌ Сначала зарегистрируйтесь!", show_alert=True)
        return
    
    profile_text = (
        f"👤 Профиль:\n"
        f"💰 Баланс: {user['balance']} монет\n"
        f"🧷 Рабов: {len(user['slaves'])}\n"
        f"📈 Всего заработано: {user['total_income']}\n"
        f"🛠 Улучшения: {sum(user['upgrades'].values())}"
    )
    
    await callback.message.edit_text(profile_text, reply_markup=main_keyboard())
    await callback.answer()

@dp.callback_query(F.data == "ref_link")
async def ref_link_handler(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    if user_id not in users:
        await callback.answer("❌ Сначала зарегистрируйтесь!", show_alert=True)
        return
    
    ref_link = f"https://t.me/{(await bot.get_me()).username}?start={user_id}"
    await callback.message.edit_text(
        f"🔗 Ваша реферальная ссылка:\n<code>{ref_link}</code>\n\n"
        f"💎 За каждого приглашенного:\n"
        f"+1 раб и 50 монет",
        reply_markup=main_keyboard(),
        parse_mode=ParseMode.HTML
    )
    await callback.answer()

@dp.callback_query(F.data == "main_menu")
async def main_menu_handler(callback: types.CallbackQuery):
    await callback.message.edit_text("🔮 Главное меню:", reply_markup=main_keyboard())
    await callback.answer()

async def check_subscription(user_id: int):
    try:
        member = await bot.get_chat_member(chat_id=CHANNEL_ID, user_id=user_id)
        return member.status not in ['left', 'kicked']
    except Exception as e:
        logging.error(f"Ошибка проверки подписки: {e}")
        return False

async def main():
    logging.basicConfig(level=logging.INFO)
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
