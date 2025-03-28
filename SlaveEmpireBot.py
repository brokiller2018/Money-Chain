import logging
import asyncio
from aiogram import Bot, Dispatcher, types
from aiogram.types import Message
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.utils import executor


TOKEN = "8076628423:AAEkp4l3BYkl-6lwz8VAyMw0h7AaAM7J3oM"
bot = Bot(token=TOKEN)
storage = MemoryStorage()
dp = Dispatcher(bot, storage=storage)

# Простая база данных (временная, для тестов)
users = {}  # user_id: {"balance": 100, "slaves": [], "owner": None, "price": 100}

@dp.message_handler(commands=['start'])
async def start_command(message: Message):
    user_id = message.from_user.id
    referrer_id = message.get_args()  # Получаем реферала, если есть
    if user_id not in users:
        users[user_id] = {"balance": 100, "slaves": [], "owner": None, "price": 100}
        if referrer_id and referrer_id.isdigit():
            referrer_id = int(referrer_id)
            if referrer_id in users and referrer_id != user_id:
                users[referrer_id]['slaves'].append(user_id)
                users[user_id]['owner'] = referrer_id
                users[referrer_id]['balance'] += 50  # Бонус за привлечение
                await message.answer(f"Вы зарегистрировались! Теперь вы раб {referrer_id}, он получил 50 монет.")
                return
    await message.answer("Вы зарегистрированы в игре!")

@dp.message_handler(commands=['profile'])
async def profile_command(message: Message):
    user_id = message.from_user.id
    if user_id in users:
        profile_info = f"Ваш баланс: {users[user_id]['balance']} монет\n"
        profile_info += f"Владелец: {users[user_id]['owner']}\n"
        profile_info += f"Ваши рабы: {', '.join(map(str, users[user_id]['slaves'])) if users[user_id]['slaves'] else 'Нет'}\n"
        profile_info += f"Стоимость: {users[user_id]['price']} монет"
        await message.answer(profile_info)
    else:
        await message.answer("Вы не зарегистрированы. Введите /start")

@dp.message_handler(commands=['work'])
async def work_command(message: Message):
    user_id = message.from_user.id
    if user_id in users:
        income = 10 + len(users[user_id]['slaves']) * 5  # Больше рабов = больше доход
        users[user_id]['balance'] += income
        await message.answer(f"Вы поработали и получили {income} монет!")
    else:
        await message.answer("Вы не зарегистрированы. Введите /start")

@dp.message_handler(commands=['buy'])
async def buy_command(message: Message):
    args = message.text.split()
    if len(args) < 2:
        await message.answer("Использование: /buy @username")
        return
    
    user_id = message.from_user.id
    slave_id = int(args[1].replace('@', ''))  # Симуляция ID из юзернейма (нужен реальный user_id)
    
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
        users[old_owner]['balance'] += slave_price  # Деньги старому владельцу
        users[old_owner]['slaves'].remove(slave_id)
    
    users[user_id]['balance'] -= slave_price
    users[user_id]['slaves'].append(slave_id)
    users[slave_id]['owner'] = user_id
    users[slave_id]['price'] = int(slave_price * 1.5)  # Цена растёт на 50%
    
    await message.answer(f"Вы купили {slave_id} за {slave_price} монет! Теперь его цена {users[slave_id]['price']}.")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    executor.start_polling(dp, skip_updates=True)
