import logging
import asyncio
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

# Инициализация
bot = Bot(token=TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
storage = MemoryStorage()
dp = Dispatcher(storage=storage)

# База данных (временная)
users = {}

async def check_subscription(user_id: int):
    try:
        member = await bot.get_chat_member(chat_id=CHANNEL_ID, user_id=user_id)
        return member.status not in ['left', 'kicked']
    except Exception as e:
        logging.error(f"Ошибка проверки подписки: {e}")
        return False

@dp.message(Command('start'))
async def start_command(message: Message):
    user_id = message.from_user.id
    
    if not await check_subscription(user_id):
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔔 Подписаться", url=CHANNEL_LINK)],
            [InlineKeyboardButton(text="✅ Я подписался", callback_data="check_sub")]
        ])
        await message.answer(
            "📛 Для доступа к игре необходимо подписаться на наш канал:",
            reply_markup=kb
        )
        return

    referrer_id = message.text.split()[1] if len(message.text.split()) > 1 else None
    if user_id not in users:
        users[user_id] = {"balance": 100, "slaves": [], "owner": None, "price": 100}
        
        if referrer_id and referrer_id.isdigit():
            referrer_id = int(referrer_id)
            if referrer_id in users and referrer_id != user_id:
                users[referrer_id]['slaves'].append(user_id)
                users[user_id]['owner'] = referrer_id
                users[referrer_id]['balance'] += 50
                await message.answer(
                    f"🎉 Вы зарегистрировались по приглашению!\n"
                    f"Теперь вы в подчинении у {referrer_id}, он получил 50 монет."
                )
                return
        
        await message.answer("🆕 Вы успешно зарегистрированы в Slave Empire!")
    else:
        await message.answer("ℹ️ Вы уже зарегистрированы.")

@dp.callback_query(F.data == "check_sub")
async def check_subscription_callback(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    if await check_subscription(user_id):
        if user_id not in users:
            users[user_id] = {"balance": 100, "slaves": [], "owner": None, "price": 100}
            await callback.message.edit_text("✅ Регистрация завершена! Доступ к игре открыт.")
        else:
            await callback.message.edit_text("ℹ️ Вы уже зарегистрированы.")
    else:
        await callback.answer("❌ Вы всё ещё не подписаны!", show_alert=True)
    await callback.answer()

@dp.message(Command('profile'))
async def profile_command(message: Message):
    user_id = message.from_user.id
    if user_id in users:
        profile_info = (
            f"👤 Ваш профиль:\n"
            f"💰 Баланс: {users[user_id]['balance']} монет\n"
            f"👑 Владелец: {users[user_id]['owner'] or 'Отсутствует'}\n"
            f"🧎 Ваши рабы: {len(users[user_id]['slaves'])} чел.\n"
            f"🧷 Рефералов: {len(users[user_id]['slaves'])}\n"
            f"🏷️ Ваша цена: {users[user_id]['price']} монет"
        )
        await message.answer(profile_info)
    else:
        await message.answer("❌ Вы не зарегистрированы. Введите /start")

@dp.message(Command('work'))
async def work_command(message: Message):
    user_id = message.from_user.id
    if user_id in users:
        income = 10 + len(users[user_id]['slaves']) * 5
        users[user_id]['balance'] += income
        await message.answer(f"💼 Вы заработали {income} монет!")
    else:
        await message.answer("❌ Вы не зарегистрированы. Введите /start")

@dp.message(Command('buy'))
async def buy_command(message: Message):
    user_id = message.from_user.id
    if user_id not in users:
        await message.answer("❌ Вы не зарегистрированы. Введите /start")
        return
    
    args = message.text.split()
    if len(args) < 2:
        await message.answer("ℹ️ Использование: /buy @username")
        return
    
    try:
        slave_id = int(args[1].replace('@', ''))
    except ValueError:
        await message.answer("❌ Некорректный ID пользователя")
        return
    
    if slave_id not in users:
        await message.answer("❌ Пользователь не найден")
        return
    
    if user_id == slave_id:
        await message.answer("🤦 Вы не можете купить сами себя!")
        return
    
    slave_price = users[slave_id]['price']
    if users[user_id]['balance'] < slave_price:
        await message.answer(f"❌ Недостаточно монет. Нужно: {slave_price}")
        return
    
    old_owner = users[slave_id]['owner']
    if old_owner:
        users[old_owner]['balance'] += slave_price
        users[old_owner]['slaves'].remove(slave_id)
    
    users[user_id]['balance'] -= slave_price
    users[user_id]['slaves'].append(slave_id)
    users[slave_id]['owner'] = user_id
    users[slave_id]['price'] = int(slave_price * 1.5)
    
    await message.answer(
        f"🎉 Вы купили игрока {slave_id} за {slave_price} монет!\n"
        f"Теперь его стоимость: {users[slave_id]['price']} монет"
    )
@dp.message(Command('ref'))
async def ref_command(message: Message):
    user_id = message.from_user.id
    if user_id not in users:
        await message.answer("❌ Сначала зарегистрируйтесь через /start")
        return
    
    ref_link = f"https://t.me/{(await bot.get_me()).username}?start={user_id}"
    
    await message.answer(
        f"🔗 Ваша реферальная ссылка:\n<code>{ref_link}</code>\n\n"
        f"💎 За каждого приглашенного:\n"
        f"- Вы получаете +50 монет\n"
        f"- Он становится вашим рабом\n"
        f"- Увеличивается ваш доход от /work",
        parse_mode=ParseMode.HTML
    )
@dp.message()
async def handle_unknown(message: Message):
    await message.answer(
        "🤖 Я не понимаю эту команду. Доступные команды:\n"
        "/start - Начать игру\n"
        "/profile - Ваш профиль\n"
        "/work - Заработать монеты\n"
        "/buy @username - Купить игрока"
    )

async def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
    )
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
