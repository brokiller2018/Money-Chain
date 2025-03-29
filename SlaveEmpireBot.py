import logging
import asyncio
import json
import os
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
DB_FILE = "users_db.json"

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
    
def save_db():
    with open(DB_FILE, 'w', encoding='utf-8') as f:
        # Конвертируем datetime в строку для сохранения
        db_to_save = {}
        for user_id, user_data in users.items():
            db_to_save[user_id] = {}
            for key, value in user_data.items():
                if isinstance(value, datetime):
                    db_to_save[user_id][key] = value.isoformat()
                else:
                    db_to_save[user_id][key] = value
        json.dump(db_to_save, f, ensure_ascii=False, indent=4)

def load_db():
    if not os.path.exists(DB_FILE):
        return {}
    
    with open(DB_FILE, 'r', encoding='utf-8') as f:
        db_loaded = json.load(f)
        
    # Восстанавливаем datetime объекты
    restored_db = {}
    for user_id, user_data in db_loaded.items():
        restored_db[int(user_id)] = {}
        for key, value in user_data.items():
            if key in ['last_passive', 'last_work'] and value is not None:
                restored_db[int(user_id)][key] = datetime.fromisoformat(value)
            else:
                restored_db[int(user_id)][key] = value
    return restored_db

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
                
                # Доход от рабов
                slaves_income = sum(
                    100 * (1 + 0.3 * users[slave_id].get("slave_level", 0)) * mins_passed
                    for slave_id in user.get("slaves", [])
                    if slave_id in users  # Защита от удаленных пользователей
                )
                
                total_income = base_income + storage_income + slaves_income
                
                user["balance"] += total_income
                user["total_income"] += total_income
                user["last_passive"] = now
            

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
            "base_price": 100,
            "slave_level": 0,
            "price": 100,
            "last_work": None,
            "upgrades": {key: 0 for key in upgrades},
            "total_income": 0,
            "username": username,
            "last_passive": datetime.now(),
            "income_per_sec": 0.0167
        }
        
        welcome_msg = (
    "👑 <b>ДОБРО ПОЖАЛОВАТЬ В РАБОВЛАДЕЛЬЧЕСКУЮ ИМПЕРИЮ!</b>\n\n"
    "⚡️ <b>Основные возможности:</b>\n"
    "▸ 💼 Бонусная работа (раз в 20 мин)\n"
    "▸ 🛠 Улучшай свои владения\n"
    "▸ 👥 Покупай рабов для пассивного дохода\n"
    "▸ 📈 Получай доход каждую минуту\n\n"
    "💰 <b>Базовая пассивка:</b> 1₽/мин"
)
        
        await message.answer(welcome_msg, reply_markup=main_keyboard(), parse_mode=ParseMode.HTML)
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

@dp.callback_query(F.data.startswith(SLAVE_PREFIX))
async def buy_slave_handler(callback: types.CallbackQuery):
    try:
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

        previous_owner_id = slave.get("owner")
        previous_owner = users.get(previous_owner_id) if previous_owner_id else None

        if previous_owner and previous_owner_id != buyer_id:
            await callback.answer(
                f"❌ Этот раб принадлежит @{previous_owner.get('username', 'unknown')}",
                show_alert=True
            )
            return

        price = slave.get("price", 100)
        
        if buyer["balance"] < price:
            await callback.answer("❌ Недостаточно средств!", show_alert=True)
            return

        # Основная логика покупки
        if previous_owner:
            if slave_id in previous_owner.get("slaves", []):
                previous_owner["slaves"].remove(slave_id)
            commission = int(price * 0.1)
            previous_owner["balance"] += commission
            previous_owner["total_income"] += commission

        buyer["balance"] -= price
        buyer["total_income"] -= price
        buyer.setdefault("slaves", []).append(slave_id)

        slave["owner"] = buyer_id
        slave["slave_level"] = slave.get("slave_level", 0) + 1
        slave["price"] = int(slave.get("base_price", 100) * (1.5 ** slave["slave_level"]))

        # Формирование сообщения
        msg = [
            f"✅ Вы купили @{slave.get('username', 'безымянный')} за {price}₽!",
            f"▸ Уровень: {slave['slave_level']}",
            f"▸ Новая цена: {slave['price']}₽"
        ]
        
        if previous_owner:
            msg.append(f"▸ Комиссия предыдущему владельцу: {commission}₽")

        # Сохраняем изменения перед отправкой ответа
        save_db()

    except Exception as e:
        logging.error(f"Ошибка при покупке раба: {str(e)}", exc_info=True)
        await callback.answer("❌ Произошла ошибка при обработке запроса", show_alert=True)
        return

    # Отправка сообщения вне блока try-except
    await callback.message.edit_text("\n".join(msg), reply_markup=main_keyboard())
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
    passive_per_min = 1 + user.get("upgrades", {}).get("storage", 0) * 10
    passive_per_min += sum(
        100 * (1 + 0.3 * users[slave_id].get("slave_level", 0))
        for slave_id in user["slaves"]
    )
    
    text = (
        f"👑 <b>Профиль @{user.get('username', 'unknown')}</b>\n\n"
        f"▸ 💰 Баланс: {user.get('balance', 0):.1f}₽\n"
        f"▸ ⚡ Доход/мин: {passive_per_min:.1f}₽\n"
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
