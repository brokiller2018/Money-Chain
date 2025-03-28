import logging
import asyncio
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.enums import ParseMode
from aiogram.client.default import DefaultBotProperties
from aiogram import F

TOKEN = "8076628423:AAEkp4l3BYkl-6lwz8VAyMw0h7AaAM7J3oM"
CHANNEL_ID = "@memok_da"  
CHANNEL_LINK = "https://t.me/memok_da"

# Инициализация бота
bot = Bot(
    token=TOKEN,
    default=DefaultBotProperties(parse_mode=ParseMode.HTML)
)
storage = MemoryStorage()
dp = Dispatcher(storage=storage)

# База данных
users = {}

@dp.message(Command('start'))
async def start_command(message: Message):
    user_id = message.from_user.id
    
    # Проверка подписки
    try:
        member = await bot.get_chat_member(chat_id=CHANNEL_ID, user_id=user_id)
        if member.status in ['left', 'kicked']:
            keyboard = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="Подписаться на канал", url=CHANNEL_LINK)],
                [InlineKeyboardButton(text="Я подписался", callback_data="check_sub")]
            ])
            await message.answer(
                "❌ Для игры нужно подписаться на наш канал!",
                reply_markup=keyboard
            )
            return
    except Exception as e:
        logging.error(f"Ошибка проверки подписки: {e}")
        await message.answer("⚠️ Ошибка проверки подписки. Попробуйте позже.")
        return

    # Регистрация
    referrer_id = message.text.split()[1] if len(message.text.split()) > 1 else None
    if user_id not in users:
        users[user_id] = {"balance": 100, "slaves": [], "owner": None, "price": 100}
        if referrer_id and referrer_id.isdigit():
            referrer_id = int(referrer_id)
            if referrer_id in users and referrer_id != user_id:
                users[referrer_id]['slaves'].append(user_id)
                users[user_id]['owner'] = referrer_id
                users[referrer_id]['balance'] += 50
                await message.answer(f"Вы зарегистрировались! Теперь вы раб {referrer_id}, он получил 50 монет.")
                return
        await message.answer("✅ Вы успешно зарегистрированы в игре!")
    else:
        await message.answer("Вы уже зарегистрированы!")

@dp.callback_query(F.data == "check_sub")
async def check_subscription(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    try:
        member = await bot.get_chat_member(chat_id=CHANNEL_ID, user_id=user_id)
        if member.status in ['left', 'kicked']:
            await callback.answer("Вы ещё не подписались!", show_alert=True)
            return
    except Exception as e:
        logging.error(f"Ошибка проверки подписки: {e}")
        await callback.answer("Ошибка проверки. Попробуйте позже.", show_alert=True)
        return
    
    if user_id not in users:
        users[user_id] = {"balance": 100, "slaves": [], "owner": None, "price": 100}
        await callback.message.edit_text("✅ Вы успешно зарегистрированы в игре!")
    else:
        await callback.message.edit_text("Вы уже зарегистрированы!")
    await callback.answer()

@dp.message(Command('profile'))
async def profile_command(message: Message):
    user_id = message.from_user.id
    if user_id in users:
        profile_info = (
            f"💰 Баланс: {users[user_id]['balance']} монет\n"
            f"👑 Владелец: {users[user_id]['owner'] or 'Нет'}\n"
            f"🧎 Рабы: {', '.join(map(str, users[user_id]['slaves'])) if users[user_id]['slaves'] else 'Нет'}\n"
            f"🏷️ Цена: {users[user_id]['price']} монет"
        )
        await message.answer(profile_info)
    else:
        await message.answer("Вы не зарегистрированы. Введите /start")

@dp.message(Command('work'))
async def work_command(message: Message):
    user_id = message.from_user.id
    if user_id in users:
        income = 10 + len(users[user_id]['slaves']) * 5
        users[user_id]['balance'] += income
        await message.answer(f"💼 Вы поработали и получили {income} монет!")
    else:
        await message.answer("Вы не зарегистрированы. Введите /start")

@dp.message(Command('buy'))
async def buy_command(message: Message):
    args = message.text.split()
    if len(args) < 2:
        await message.answer("Использование: /buy @username")
        return
    
    user_id = message.from_user.id
    try:
        slave_id = int(args[1].replace('@', ''))
    except ValueError:
        await message.answer("Некорректный ID пользователя")
        return
    
    if slave_id not in users or user_id not in users:
        await message.answer("Ошибка: пользователь не найден.")
        return
    
    if user_id == slave_id:
        await message.answer("Вы не можете купить сами себя!")
        return
    
    slave_price = users[slave_id]['price']
    if users[user_id]['balance'] < slave_price:
        await message.answer("У вас недостаточно монет!")
        return
    
    # Покупка
    old_owner = users[slave_id]['owner']
    if old_owner:
        users[old_owner]['balance'] += slave_price
        users[old_owner]['slaves'].remove(slave_id)
    
    users[user_id]['balance'] -= slave_price
    users[user_id]['slaves'].append(slave_id)
    users[slave_id]['owner'] = user_id
    users[slave_id]['price'] = int(slave_price * 1.5)
    
    await message.answer(
        f"🎉 Вы купили {slave_id} за {slave_price} монет!\n"
        f"Теперь его цена: {users[slave_id]['price']} монет"
    )

async def main():
    logging.basicConfig(level=logging.INFO)
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
