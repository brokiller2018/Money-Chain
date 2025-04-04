import logging
import asyncio
import json
import os
import psycopg2
import random
import time
from psycopg2.extras import Json
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.utils.keyboard import InlineKeyboardBuilder
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
MAX_SLAVE_LEVEL = 15
DAILY_WORK_LIMIT = 1000
MAX_BARRACKS_LEVEL = 10
MIN_SLAVES_FOR_RANDOM = 3 

# Инициализация
bot = Bot(token=TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
storage = MemoryStorage()
dp = Dispatcher(storage=storage)

# База данных
users = {}
user_search_cache = {
    'awaiting_bet': set()  # Используем set для избежания дубликатов
}
active_games = {}
# Улучшения
upgrades = {
    "storage": {
        "name": "📦 Склад",
        "base_price": 300, 
        "income_bonus": 5,
        "price_multiplier": 1.3,
        "description": "+8 монет/мин к пассивному доходу"
    },
    "whip": {
        "name": "⛓ Кнуты", 
        "base_price": 800,
        "income_bonus": 0.18,  # +18% к работе (было +25%)
        "price_multiplier": 1.3,
        "description": "+18% к доходу от работы"
    },
    "food": {
        "name": "🍗 Еда",
        "base_price": 1500,
        "income_bonus": 0.08,  # -8% времени работы за уровень
        "price_multiplier": 1.5,
        "description": "-8% к времени ожидания работы"
    },
    "barracks": {
        "name": "🏠 Бараки",
        "base_price": 3000,
        "income_bonus": 2,  # +2 к лимиту рабов
        "price_multiplier": 1.6,
        "description": "+2 к лимиту рабов"
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
            InlineKeyboardButton(text="🛒 Магазин", callback_data="shop")
        ],
        [    
            InlineKeyboardButton(text="🎮 Играть в 21", callback_data="play_21"),
            InlineKeyboardButton(text="🔗 Рефералка", callback_data=REF_LINK)
        ],
        [
            InlineKeyboardButton(text="🏆 Топ владельцев", callback_data=TOP_OWNERS)
        ]
    ])

def get_db_connection():
    return psycopg2.connect(os.getenv("DATABASE_URL"))

async def show_bet_selection(message: types.Message):
    """Показывает меню выбора ставки с кастомным вводом"""
    builder = InlineKeyboardBuilder()
    
    # Стандартные ставки
    bets = [500, 1000, 2000, 5000]
    for bet in bets:
        builder.button(text=f"{bet}₽", callback_data=f"bj_bet_{bet}")
    
    # Дополнительные кнопки
    builder.button(text="🎲 Своя ставка", callback_data="bj_custom_bet")
    builder.button(text="🔙 В меню", callback_data=MAIN_MENU)
    
    # Оптимизация расположения
    builder.adjust(2, 2, 1)
    
    await message.edit_text(
        "🎰 Выберите или введите ставку:",
        reply_markup=builder.as_markup()
    )

async def cleanup_games():
    while True:
        await asyncio.sleep(300)
        try:
            current_time = datetime.now()
            expired = []
            for user_id, game in active_games.items():
                # Удаляем только завершенные или зависшие игры
                if game.game_over or (current_time - game.last_action_time).total_seconds() > 1800:
                    expired.append(user_id)

            for user_id in expired:
                if user_id in active_games:
                    del active_games[user_id]
                    logging.info(f"Очищена игра пользователя {user_id}")

        except Exception as e:
            logging.error(f"Ошибка очистки игр: {e}")

async def on_startup():
    global users
    users = load_db()  # Загружаем БД при старте
    asyncio.create_task(passive_income_task())
    asyncio.create_task(autosave_task())
    asyncio.create_task(cleanup_games())  # Добавляем очистку игр
    
    # Сохраняем БД при корректном завершении
    def save_on_exit(*args):
        save_db()
    
    import signal
    signal.signal(signal.SIGTERM, save_on_exit)
    signal.signal(signal.SIGINT, save_on_exit)
    
    # Удаляем ошибочную строку с self
    logging.info("Бот успешно запущен")

class Card:
    def __init__(self, suit, rank):
        self.suit = suit
        self.rank = rank
        
    @property
    def value(self):
        if self.rank in ['J', 'Q', 'K']:
            return 10
        if self.rank == 'A':
            return 11
        return int(self.rank)
        
    def __repr__(self):
        suits = {'Spades': '♠️', 'Hearts': '♥️', 'Diamonds': '♦️', 'Clubs': '♣️'}
        return f"{suits[self.suit]}{self.rank}"

# Класс для игры в Blackjack
class BlackjackGame:
    def __init__(self, user_id: int, bet: int, bot: Bot):
        self.user_id = user_id  # Добавляем сохранение user_id
        self.bet = bet
        self.bot = bot
        self.deck = self.create_deck()
        self.player_hand = []
        self.dealer_hand = []
        self.game_over = False
        self.message = None
        self.last_action_time = datetime.now()  # Добавляем инициализацию времени

    async def start_game(self, message: types.Message):
        try:
            self.message = message
            self.deck = self.create_deck()
            random.shuffle(self.deck)
            self.player_hand = [self.deal_card(), self.deal_card()]
            self.dealer_hand = [self.deal_card(), self.deal_card()]
            
            active_games[self.user_id] = self  # Явное сохранение
            
            await self.update_display()
            
        except Exception as e:
            logging.error(f"Ошибка старта игры: {e}")
            await self.cleanup_game()

    def create_deck(self):
        """Создает и возвращает колоду из 52 карт"""
        suits = ['Spades', 'Hearts', 'Diamonds', 'Clubs']
        ranks = ['2', '3', '4', '5', '6', '7', '8', '9', '10', 'J', 'Q', 'K', 'A']
        return [Card(suit, rank) for suit in suits for rank in ranks]

    def deal_card(self):
        """Выдает одну карту из колоды"""
        return self.deck.pop()

    def calculate_hand(self, hand):
        """Рассчитывает стоимость руки с учетом Ace как 11 или 1"""
        value = sum(card.value for card in hand)
        aces = sum(1 for card in hand if card.rank == 'A')
        
        while value > 21 and aces:
            value -= 10
            aces -= 1
        return value


    async def end_game(self, result: str):
        try:
            self.game_over = True
            player_value = self.calculate_hand(self.player_hand)
            dealer_value = self.calculate_hand(self.dealer_hand)
    
            user = users.get(self.user_id)
            if not user:
                raise ValueError(f"User {self.user_id} not found")
    
            # Удаляем игру из активных перед изменением баланса
            if self.user_id in active_games:
                del active_games[self.user_id]
    
            # Автоматическое определение результата, если не задан
            if result is None:
                if player_value > 21:
                    result = 'lose'
                elif dealer_value > 21:
                    result = 'win'
                elif player_value > dealer_value:
                    result = 'win'
                elif player_value < dealer_value:
                    result = 'lose'
                else:
                    result = 'draw'
    
            # Расчет результата
            if result == 'blackjack':
                win_amount = int(self.bet * 2.5)
                user["balance"] += win_amount
                text = f"🎉 Blackjack! Выигрыш: {win_amount}₽!"
            elif result == 'win':
                user["balance"] += self.bet
                text = f"🎉 Выигрыш: {self.bet}₽!"
            elif result == 'draw':
                text = "🤝 Ничья!"
            else:
                user["balance"] -= self.bet
                text = f"💸 Проигрыш: {self.bet}₽"
    
            save_db()
    
            await self.message.edit_text(
                f"{text}\n\n"
                f"Ваши карты: {self.player_hand} ({player_value})\n"
                f"Карты дилера: {self.dealer_hand} ({dealer_value})",
                reply_markup=main_keyboard()
            )
    
        except Exception as e:
            logging.error(f"Ошибка завершения игры: {e}")
            if self.user_id in active_games:
                del active_games[self.user_id]
            await self.message.answer("⚠️ Игра завершена", reply_markup=main_keyboard())

    async def dealer_turn(self):
        """Обрабатывает ход дилера"""
        try:
            while self.calculate_hand(self.dealer_hand) < 17:
                self.dealer_hand.append(self.deal_card())
            await self.end_game(None)
        except Exception as e:
            logging.error(f"Ошибка хода дилера: {e}")
            await self.cleanup_game()

    async def update_display(self):
        try:
            dealer_status = "Карта дилера: " 
            if self.game_over:
                dealer_status = f"Дилер: {self.calculate_hand(self.dealer_hand)}"
            else:
                dealer_status = f"Дилер: {self.dealer_hand[0]} ?"
                
            await self.message.edit_text(
                f"💰 Ставка: {self.bet}₽\n"
                f"Ваши карты: {self.player_hand} ({self.calculate_hand(self.player_hand)})\n"
                f"{dealer_status}",
                reply_markup=get_game_keyboard(self)  # Используем новую функцию
            )
        except Exception as e:
            logging.error(f"Ошибка отображения: {e}")

    async def cleanup_games():
        while True:
            await asyncio.sleep(300)
            try:
                current_time = datetime.now()
                expired = []
                for user_id, game in active_games.items():
                    # Условие для удаления только завершенных или неактивных игр
                    if game.game_over or (current_time - game.last_action_time).total_seconds() > 1800:
                        expired.append(user_id)
    
                for user_id in expired:
                    del active_games[user_id]
                    
            except Exception as e:
                logging.error(f"Ошибка очистки игр: {e}")


def get_game_keyboard(game: BlackjackGame) -> InlineKeyboardMarkup:
        """Создает клавиатуру для игровых действий"""
        keyboard = InlineKeyboardBuilder()
        
        # Кнопки действий с новыми префиксами
        keyboard.button(
            text="🎯 Взять", 
            callback_data="bj_action_hit"
        )
        keyboard.button(
            text="✋ Стоп", 
            callback_data="bj_action_stand"
        )
        
        if len(game.player_hand) == 2 and not game.game_over:
            keyboard.button(
                text="🔼 Удвоить", 
                callback_data="bj_action_double"
            )
        
        keyboard.adjust(2)
        return keyboard.as_markup()

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
        [InlineKeyboardButton(text="🎲 Случайные рабы (Топ-10)", callback_data="random_slaves")],
        [InlineKeyboardButton(text="🔙 Назад", callback_data=MAIN_MENU)]
    ])
    
def serialize_user_data(user_data: dict) -> dict:
    """Преобразуем datetime объекты в строки для JSON"""
    serialized = {}
    for key, value in user_data.items():
        if isinstance(value, datetime):
            serialized[key] = value.isoformat()
        elif isinstance(value, dict) and key == "shackles":
            # Сериализуем кандалы
            serialized[key] = {
                str(slave_id): end_time.isoformat() 
                for slave_id, end_time in value.items()
            }
        else:
            serialized[key] = value
    return serialized

def deserialize_user_data(data: dict) -> dict:
    """Восстанавливаем datetime из строк"""
    deserialized = {}
    for key, value in data.items():
        if key in ['last_passive', 'last_work', 'shield_active'] and value:
            try:
                deserialized[key] = datetime.fromisoformat(value)
            except (TypeError, ValueError):
                deserialized[key] = None
        elif key == "shackles" and isinstance(value, dict):
            # Десериализуем кандалы
            deserialized[key] = {
                int(slave_id): datetime.fromisoformat(end_time)
                for slave_id, end_time in value.items()
            }
        else:
            deserialized[key] = value
    return deserialized

def save_db():
    conn = None
    try:
        conn = get_db_connection()
        with conn:
            with conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS bot_users (
                        user_id BIGINT PRIMARY KEY,
                        data JSONB NOT NULL,
                        last_updated TIMESTAMP WITH TIME ZONE DEFAULT NOW()
                    )
                """)
                
                # Сериализуем данные перед сохранением
                for user_id, user_data in users.items():
                    serialized_data = serialize_user_data(user_data)
                    cur.execute("""
                        INSERT INTO bot_users (user_id, data)
                        VALUES (%s, %s)
                        ON CONFLICT (user_id) 
                        DO UPDATE SET 
                            data = EXCLUDED.data,
                            last_updated = NOW()
                    """, (user_id, Json(serialized_data)))
        
    except psycopg2.Error as e:
        logging.error(f"Database error in save_db: {e}")
        raise
    finally:
        if conn:
            conn.close()


def load_db():
    conn = None
    try:
        conn = get_db_connection()
        with conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_name = 'bot_users'
                    )
                """)
                table_exists = cur.fetchone()[0]
                
                if not table_exists:
                    return {}
                
                cur.execute("SELECT user_id, data FROM bot_users")
                rows = cur.fetchall()
                
                loaded_users = {}
                for user_id, data in rows:
                    # Десериализуем данные при загрузке
                    loaded_users[user_id] = deserialize_user_data(data)
                
                return loaded_users
                
    except psycopg2.Error as e:
        logging.error(f"Database error in load_db: {e}")
        return {}
    finally:
        if conn:
            conn.close()

def passive_income(user):
    base = 1 + user["upgrades"].get("storage", 0) * 5
    slaves = sum(
        50 * (1 + 0.2 * slave_level(slave_id)) 
        for slave_id in user.get("slaves", [])
    )
    return base + slaves * (1 + 0.05 * user["upgrades"].get("barracks", 0))

def calculate_shield_price(user_id):
    user = users[user_id]
    # Базовый доход (1 + склад) в минуту
    passive_per_min = 1 + user.get("upgrades", {}).get("storage", 0) * 10
    # Доход от рабов в минуту
    passive_per_min += sum(
        100 * (1 + 0.3 * users[slave_id].get("slave_level", 0))
        for slave_id in user.get("slaves", [])
        if slave_id in users
    )
    # Цена = 50% дохода за 12 часов, округлено до 10
    base_price = passive_per_min * 60 * 6  # 6 часов
    # Получаем количество покупок щита
    shop_purchases = user.get("shop_purchases", 0)
    price = base_price * (1.1 ** shop_purchases) 
    price = max(500, min(8000, price))  # Новые лимиты
    # Скидка за первую покупку
    if user.get("shop_purchases", 0) == 0:
        price = int(price * 0.7)
    return price

def calculate_shackles_price(owner_id):
    owner = users[owner_id]
    
    # 1. Считаем пассивный доход владельца в час
    passive_income = (
        1 + owner.get("upgrades", {}).get("storage", 0) * 10  # Склад
    ) * 60  # В час
    
    # 2. Добавляем доход от всех рабов
    for slave_id in owner.get("slaves", []):
        if slave_id in users:
            slave = users[slave_id]
            passive_income += 100 * (1 + 0.3 * slave.get("slave_level", 0))
    
    # 3. Цена = 150% от часового дохода, округлено до 100
    price = int(passive_income * 1.5 / 100) * 100
    
    # 4. Ограничиваем диапазон (300–10 000₽)
    return max(300, min(10_000, price))

def slave_price(slave_data: dict) -> int:
    return int(500 * (1.5 ** min(slave_data.get("slave_level", 0), MAX_SLAVE_LEVEL)))

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
        try:
            now = datetime.now()
            for user_id, user in users.items():
                if "last_passive" not in user:
                    user["last_passive"] = now
                    continue
                    
                mins_passed = (now - user["last_passive"]).total_seconds() / 60
                mins_passed = min(mins_passed, 180)  # Максимум 3 часа, даже если бот был выключен
                
                # 1. Базовый доход (1₽/мин)
                base_income = 1 * mins_passed
                
                # 2. Доход от склада (10₽/мин за уровень)
                storage_level = user.get("upgrades", {}).get("storage", 0)
                storage_income = storage_level * 10 * mins_passed
                
                # 3. Доход от рабов с налогом
                slaves_income = 0
                if user.get("slaves"):
                    for slave_id in user["slaves"]:
                        if slave_id in users:
                            slave = users[slave_id]
                            
                            # Доход раба (100₽/час базовый + 30% за уровень)
                            slave_income = 100 * (1 + 0.3 * slave.get("slave_level", 0)) * (mins_passed / 60)
                            
                            # Налог зависит от УРОВНЯ РАБА (а не владельца!)
                            slave_level = slave.get("slave_level", 0)
                            tax_rate = min(0.1 + 0.05 * slave_level, 0.3)
                            tax = int(slave_income * tax_rate)
                            
                            # Раб получает 70-90% дохода
                            slave["balance"] = min(slave.get("balance", 0) + slave_income - tax, 100_000)
                            
                            # Владелец получает налог 10-30%
                            slaves_income += tax
                
                # Итоговый доход (база + склад + налоги с рабов)
                total_income = base_income + storage_income + slaves_income
                
                # Защита от переполнения
                user["balance"] = min(user.get("balance", 0) + total_income, 10_000_000)
                user["total_income"] = user.get("total_income", 0) + total_income
                user["last_passive"] = now
            
            # Сохраняем каждые 5 минут (не на каждой итерации)
            if int(time.time()) % 300 == 0:
                save_db()
                
        except Exception as e:
            logging.error(f"Ошибка в passive_income_task: {e}", exc_info=True)
            await asyncio.sleep(10)  # Пауза при ошибке

# Обработчики команд
@dp.message(Command('start'))
async def start_command(message: Message):
    user_id = message.from_user.id
    username = message.from_user.username
    
    # Получаем referrer_id из параметров команды (если есть)
    referrer_id = None
    if len(message.text.split()) > 1:
        try:
            referrer_id = int(message.text.split()[1])
            # Проверяем, что это не сам пользователь и реферер существует
            if referrer_id == user_id or referrer_id not in users:
                referrer_id = None
        except (ValueError, IndexError):
            referrer_id = None

    if not await check_subscription(user_id):
        # Сохраняем реферала сразу, даже если пользователь еще не подписался
        if referrer_id:
            users.setdefault(user_id, {})["referrer"] = referrer_id
            save_db()
            
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔔 Подписаться", url=CHANNEL_LINK)],
            [InlineKeyboardButton(text="✅ Я подписался", callback_data=f"{CHECK_SUB}{user_id}")]
        ])
        await message.answer("📌 Для доступа подпишитесь на канал:", reply_markup=kb)
        return
    
    if user_id not in users:
        # Создаем нового пользователя с учетом реферала
        users[user_id] = {
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
            "shield_active": None,  # Время окончания щита
            "shackles": {},  # {slave_id: end_time}
            "shop_purchases": 0,  # Счетчик покупок за день
            "last_passive": datetime.now(),
            "income_per_sec": 0.0167,
            "referrer": None  # Сохраняем реферала
        }
        
        # Начисляем бонус рефералу
        if referrer_id and referrer_id in users:
            if referrer_id not in user.get("referrals", []):
                user["referrals"].append(referrer_id)
                bonus = min(100, int(new_user["balance"] * 0.05))
                users[referrer_id]["balance"] += bonus
                users[referrer_id]["total_income"] += bonus
            try:
                await bot.send_message(
                    referrer_id,
                    f"🎉 Вам начислено {bonus}₽ за приглашение @{username}!"
                )
            except Exception:
                pass  # Если не удалось отправить сообщение
        
        welcome_msg = (
            "👑 <b>ДОБРО ПОЖАЛОВАТЬ В РАБОВЛАДЕЛЬЧЕСКУЮ ИМПЕРИЮ!</b>\n\n"
            "⚡️ <b>Основные возможности:</b>\n"
            "▸ 💼 Бонусная работа (раз в 20 мин)\n"
            "▸ 🛠 Улучшай свои владения\n"
            "▸ 👥 Покупай рабов для пассивного дохода\n"
            "▸ 📈 Получай доход каждую минуту\n\n"
        )
        
        if referrer_id:
            referrer_name = users.get(referrer_id, {}).get("username", "друг")
            welcome_msg += f"🤝 Вас пригласил: @{referrer_name}\n\n"
        
        welcome_msg += "💰 <b>Базовая пассивка:</b> 1₽/мин"
        
        save_db()
        await message.answer(welcome_msg, reply_markup=main_keyboard(), parse_mode=ParseMode.HTML)
    else:
        await message.answer("🔮 Главное меню:", reply_markup=main_keyboard())

@dp.message(Command('fix_economy'))
async def fix_economy(message: Message):
    if message.from_user.id != ADMIN_ID:
        return
    
    for user_id in users:
        users[user_id]["balance"] = min(users[user_id]["balance"], 10_000)  # Макс 10к
    
    save_db()
    await message.answer("Экономика исправлена!")

@dp.message(Command("top_user"))
async def handle_top_user_command(message: types.Message):
    print(f"Получена команда /top_user от {message.from_user.id}")  # Логирование
    
    try:
        # Получаем топ-5 владельцев по количеству рабов
        top_owners = sorted(
            [u for u in users.values() if u.get('slaves')],
            key=lambda x: len(x['slaves']),
            reverse=True
        )[:5]

        if not top_owners:
            await message.reply("😢 Пока нет ни одного рабовладельца")
            return

        # Формируем текст ответа
        response = ["🏆 <b>Топ рабовладельцев:</b>\n"]
        for i, owner in enumerate(top_owners, 1):
            response.append(
                f"{i}. @{owner.get('username', 'unknown')} "
                f"- {len(owner['slaves'])} рабов"
            )

        await message.reply("\n".join(response), parse_mode="HTML")
        
    except Exception as e:
        print(f"Ошибка в /top_user: {e}")  # Логирование ошибки
        await message.reply("⚠️ Произошла ошибка при формировании топа")


@dp.callback_query(F.data == "random_slaves")
async def show_random_slaves(callback: types.CallbackQuery):
    try:
        user_id = callback.from_user.id
        if user_id not in users:
            await callback.answer("❌ Сначала зарегистрируйтесь!", show_alert=True)
            return

        user = users[user_id]
        barracks_level = user.get("upgrades", {}).get("barracks", 0)
        slave_limit = 5 + 2 * barracks_level
        
        if len(user.get("slaves", [])) >= slave_limit:
            await callback.answer(f"❌ Лимит рабов ({slave_limit})", show_alert=True)
            return

        available = []
        for slave_id, slave_data in users.items():
            # Проверяем основные условия
            if not slave_data or slave_id == user_id:
                continue
                
            # Проверяем что slave_data содержит все нужные поля
            if not all(key in slave_data for key in ['owner', 'shield_active', 'last_purchased', 'username']):
                continue
                
            # Проверка владельца
            if slave_data['owner'] == user_id:
                continue

            # Проверка щита
            shield = slave_data['shield_active']
            if shield:
                try:
                    if isinstance(shield, str):
                        shield = datetime.fromisoformat(shield)
                    if shield and shield > datetime.now():
                        continue
                except:
                    continue

            # Проверка времени перекупа
            last_purchased = slave_data['last_purchased']
            if last_purchased:
                try:
                    if isinstance(last_purchased, str):
                        last_purchased = datetime.fromisoformat(last_purchased)
                    if (datetime.now() - last_purchased) < timedelta(hours=3):
                        continue
                except:
                    continue
            
            available.append((slave_id, slave_data))

        if not available:
            await callback.answer("😢 Нет доступных рабов", show_alert=True)
            return
            
        # Сортировка и выбор топ-10
        available.sort(
            key=lambda x: x[1].get('price', 100) * (1 + 0.5 * x[1].get('slave_level', 0)),
            reverse=True
        )
        selected = available[:10]

        # Формируем кнопки
        buttons = []
        for slave_id, slave_data in selected:
            try:
                price = slave_data.get('price', 100)
                level = slave_data.get('slave_level', 0)
                income = int(100 * (1 + 0.3 * level))
                username = slave_data.get('username', 'unknown')[:20]
                
                buttons.append([InlineKeyboardButton(
                    text=f"👤 Ур.{level} @{username} | {price}₽",
                    callback_data=f"{SLAVE_PREFIX}{slave_id}"
                )])
            except Exception as e:
                logging.error(f"Ошибка формирования кнопки: {e}")
                continue

        buttons.append([
            InlineKeyboardButton(text="🔄 Обновить", callback_data="random_slaves"),
            InlineKeyboardButton(text="🔙 Назад", callback_data=BUY_MENU)
        ])

        await callback.message.edit_text(
            "🎲 Доступные рабы (Топ-10):",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons)
        )
        await callback.answer()

    except Exception as e:
        logging.error(f"Ошибка в random_slaves: {e}", exc_info=True)
        await callback.answer("⚠️ Ошибка загрузки списка", show_alert=True)

    
@dp.callback_query(F.data.startswith("bj_action_"))
async def blackjack_action_handler(callback: types.CallbackQuery):
    try:
        user_id = callback.from_user.id
        action = callback.data.split("_")[2]  # hit, stand, double
        
        if user_id not in active_games:
            await callback.answer("❌ Начните новую игру через меню!", show_alert=True)
            return

        game = active_games[user_id]
        
        if game.game_over:
            await callback.answer("Эта игра уже завершена")
            return

        # Обновляем время последнего действия
        game.last_action_time = datetime.now()
        
        # Обработка действий
        if action == 'hit':
            game.player_hand.append(game.deal_card())
            if game.calculate_hand(game.player_hand) > 21:
                await game.end_game('lose')
            else:
                await game.update_display()

        elif action == 'stand':
            await game.dealer_turn()

        elif action == 'double':
            if len(game.player_hand) == 2:
                game.bet *= 2
                game.player_hand.append(game.deal_card())
                await game.dealer_turn()

        await callback.answer()

    except Exception as e:
        logging.error(f"Ошибка в игре: {e}")
        await callback.answer("⚠️ Произошла ошибка, игра перезапущена!")
        if user_id in active_games:
            del active_games[user_id]




@dp.callback_query(F.data == SEARCH_USER)
async def search_user_handler(callback: types.CallbackQuery):
    await callback.message.edit_text(
        "🔍 Введите @username игрока:\n"
        "Пример: <code>@username123</code>",
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
    cooldown = timedelta(minutes=30)  # Увеличен кулдаун
    
    if user["last_work"] and (now - user["last_work"]) < cooldown:
        remaining = (user["last_work"] + cooldown - now).seconds // 60
        await callback.answer(f"⏳ Подождите еще {remaining} минут", show_alert=True)
        return
    if user.get("work_count", 0) >= DAILY_WORK_LIMIT:
        await callback.answer("❌ Достигнут дневной лимит!")
        return
    user["work_count"] = user.get("work_count", 0) + 1
    # Рассчитываем текущий пассивный доход в минуту
    passive_per_min = 1 + user.get("upgrades", {}).get("storage", 0) * 10
    passive_per_min += sum(
        100 * (1 + 0.3 * users[slave_id].get("slave_level", 0))
        for slave_id in user.get("slaves", [])
        if slave_id in users
    ) / 60
    
    # Бонус = 20 минут пассивного дохода * множитель кнутов
    whip_bonus = 1 + user.get("upgrades", {}).get("whip", 0) * 0.25
    work_bonus = passive_per_min * 10 * (1 + whip_bonus)
    
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

@dp.message(
    F.text & 
    F.text.startswith('@') &
    (F.chat.type == "private")  # Только ЛС
)

@dp.message(F.text & F.chat.type == "private")
async def handle_custom_bet_input(message: Message):
    try:
        user_id = message.from_user.id
        if user_id not in users:
            return

        # Проверяем, ожидаем ли мы ставку от этого пользователя
        if user_id not in user_search_cache['awaiting_bet']:
            return  # Игнорируем сообщения не в контексте ставки

        # Удаляем из ожидания сразу после получения сообщения
        user_search_cache['awaiting_bet'].discard(user_id)

        # Валидация ввода
        if not message.text.strip().isdigit():
            await message.reply("❌ Введите только цифры (например: 1000)")
            return

        bet = int(message.text)
        MIN_BET = 100
        MAX_BET = 20000

        if not (MIN_BET <= bet <= MAX_BET):
            await message.reply(
                f"❌ Ставка должна быть от {MIN_BET}₽ до {MAX_BET}₽",
                reply_markup=main_keyboard()
            )
            return

        # Проверка баланса
        if users[user_id]["balance"] < bet:
            await message.reply(
                f"❌ Недостаточно средств! Ваш баланс: {users[user_id]['balance']}₽",
                reply_markup=main_keyboard()
            )
            return

        # Очистка предыдущей игры
        if user_id in active_games:
            del active_games[user_id]

        # Создание новой игры
        game = BlackjackGame(user_id, bet, bot)
        active_games[user_id] = game
        await game.start_game(message)

    except Exception as e:
        logging.error(f"Ошибка ввода ставки: {e}", exc_info=True)
        await message.answer("❌ Ошибка обработки ставки", reply_markup=main_keyboard())

async def process_username(message: Message):
    try:
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
        
        # Проверка щита защиты
        shield_active = slave.get("shield_active")
        if isinstance(shield_active, str):
            try:
                shield_active = datetime.fromisoformat(shield_active)
            except ValueError:
                shield_active = None

        if shield_active and shield_active > datetime.now():
            shield_time = shield_active.strftime("%d.%m %H:%M")
            await message.reply(
                f"🛡 Цель защищена щитом до {shield_time}",
                reply_markup=kb
            )
            return

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

    except Exception as e:
        logging.error(f"Ошибка поиска: {e}")
        await message.reply("⚠️ Произошла ошибка при поиске")

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

@dp.callback_query(F.data == TOP_OWNERS)
async def top_owners_handler(callback: types.CallbackQuery):
    try:
        current_user_id = callback.from_user.id
        
        # Рассчитываем эффективность для всех пользователей
        users_list = []
        for user_id, user_data in users.items():
            slaves_count = len(user_data.get("slaves", []))
            total_income = user_data.get("total_income", 0)
            efficiency = total_income / slaves_count if slaves_count > 0 else 0
            users_list.append({
                "user_id": user_id,
                "username": user_data.get("username", "Unknown"),
                "slaves": slaves_count,
                "total_income": total_income,
                "efficiency": efficiency
            })

        # Сортируем по эффективности (по убыванию)
        sorted_users = sorted(
            users_list,
            key=lambda x: x["efficiency"],
            reverse=True
        )

        # Формируем топ-10
        top_10 = sorted_users[:10]
        
        # Находим позицию текущего пользователя
        user_position = None
        for idx, user in enumerate(sorted_users, 1):
            if user["user_id"] == current_user_id:
                user_position = idx
                break

        # Формируем текст
        text = "🏆 <b>Топ рабовладельцев по эффективности:</b>\n\n"
        text += "<i>Рейтинг рассчитывается как доход на одного раба</i>\n\n"
        
        # Выводим топ-10
        for idx, user in enumerate(top_10, 1):
            if user["efficiency"] > 0:
                text += (
                    f"{idx}. @{user['username']}\n"
                    f"   ▸ Эффективность: {user['efficiency']:.1f}₽/раб\n"
                    f"   ▸ Рабов: {user['slaves']} | Доход: {user['total_income']:.1f}₽\n\n"
                )

        # Добавляем позицию пользователя
        if user_position:
            if user_position <= 10:
                text += f"\n🎉 Ваша позиция в топе: {user_position}"
            else:
                user_efficiency = next((u["efficiency"] for u in sorted_users if u["user_id"] == current_user_id), 0)
                text += f"\n📊 Ваша эффективность: {user_efficiency:.1f}₽/раб (позиция #{user_position})"
        else:
            text += "\nℹ️ Вы пока не участвуете в рейтинге"

        await callback.message.edit_text(
            text,
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[
                    [InlineKeyboardButton(text="🔙 Назад", callback_data=MAIN_MENU)]
                ]
            ),
            parse_mode=ParseMode.HTML
        )

    except Exception as e:
        logging.error(f"Top owners error: {e}", exc_info=True)
        await callback.answer("🌀 Ошибка загрузки топа", show_alert=True)
    finally:
        await callback.answer()

@dp.callback_query(F.data == "shop")
async def shop_handler(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    user = users.get(user_id)
    
    if not user:
        await callback.answer("❌ Сначала зарегистрируйтесь!", show_alert=True)
        return

    # Расчет цены щита
    shield_price = calculate_shield_price(user_id)
    
    # Обработка shield_active с проверкой типа данных
    shield_active = user.get("shield_active")
    if isinstance(shield_active, str):
        try:
            shield_active = datetime.fromisoformat(shield_active)
            user["shield_active"] = shield_active  # Обновляем значение в словаре
        except (ValueError, TypeError):
            shield_active = None
    
    # Проверка активности щита
    shield_status = "🟢 Активен" if shield_active and shield_active > datetime.now() else "🔴 Неактивен"
    
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

@dp.callback_query(F.data.startswith(SHIELD_PREFIX))
async def buy_shield(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    user = users.get(user_id)
    price = int(callback.data.replace(SHIELD_PREFIX, ""))
    
    # Проверяем тип данных
    current_shield = user.get("shield_active")
    if current_shield and isinstance(current_shield, str):
        try:
            current_shield = datetime.fromisoformat(current_shield)
            user["shield_active"] = current_shield
        except ValueError:
            current_shield = None
    
    if current_shield and current_shield > datetime.now():
        await callback.answer("❌ У вас уже есть активный щит!", show_alert=True)
        return
        
    if user.get("balance", 0) < price:
        await callback.answer("❌ Недостаточно средств!", show_alert=True)
        return
    
    user["balance"] -= price
    user["shield_active"] = datetime.now() + timedelta(hours=12)
    user["shop_purchases"] = user.get("shop_purchases", 0) + 1
    save_db()
    
    await callback.answer(f"🛡 Щит активирован до {user['shield_active'].strftime('%H:%M')}!", show_alert=True)
    await shop_handler(callback)
    
@dp.callback_query(F.data == "play_21")
async def play_21_handler(callback: types.CallbackQuery):
    try:
        user_id = callback.from_user.id
        if user_id in active_games:
            game = active_games[user_id]
            if not game.game_over:
                await game.update_display()
                return
        
        await show_bet_selection(callback.message)

    except Exception as e:
        logging.error(f"Ошибка меню игры: {e}")
        await callback.answer("⚠️ Ошибка запуска")


@dp.message(Command("bj_stop"))
async def stop_blackjack(message: types.Message):
    user_id = message.from_user.id
    if user_id in active_games:
        del active_games[user_id]
        await message.answer("Игра завершена!", reply_markup=main_keyboard())
    else:
        await message.answer("У вас нет активных игр")

@dp.callback_query(F.data.startswith("bj_bet_"))
async def blackjack_bet_handler(callback: types.CallbackQuery):
    try:
        user_id = callback.from_user.id
        bet = int(callback.data.split("_")[2])
        
        # Удаляем старые игры
        if user_id in active_games:
            del active_games[user_id]

        # Проверка баланса
        if users[user_id]["balance"] < bet:
            await callback.answer("❌ Недостаточно средств!", show_alert=True)
            return

        # Создаем новую игру
        game = BlackjackGame(
            user_id=user_id,
            bet=bet,
            bot=bot
        )
        active_games[user_id] = game
        
        # Запускаем игру
        await game.start_game(callback.message)
        
    except Exception as e:
        logging.error(f"Ошибка создания игры: {e}")
        await callback.answer("❌ Ошибка при старте игры!")

@dp.callback_query(F.data == "select_shackles")
async def select_shackles(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    user = users.get(user_id)
    
    if not user or not user.get("slaves"):
        await callback.answer("❌ У вас нет рабов!", show_alert=True)
        return
    
    buttons = []
    for slave_id in user["slaves"][:5]:  # Максимум 5 первых рабов
        slave = users.get(slave_id, {})
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

# Команда для админа (/fix_economy), чтобы сбросить аномальные балансы
@dp.callback_query(F.data == "bj_custom_bet")
async def handle_custom_bet(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    user_search_cache['awaiting_bet'].add(user_id)  # Добавляем в ожидание
    
    await callback.message.edit_text(
        "💎 Введите сумму ставки цифрами (мин 100₽, макс 20000₽):",
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[[InlineKeyboardButton(text="🔙 Назад", callback_data="bj_cancel_bet")]]
        )
    )
    await callback.answer()

@dp.callback_query(F.data.startswith(SHACKLES_PREFIX))
async def buy_shackles(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    user = users.get(user_id)
    _, slave_id, price = callback.data.split("_")
    slave_id = int(slave_id)
    price = int(price)
    
    if slave_id not in user.get("slaves", []):
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
    save_db()
    
    await callback.answer(
        f"⛓ Кандалы применены к @{users[slave_id].get('username', 'unknown')} на 24ч!",
        show_alert=True
    )
    await select_shackles(callback)  # Возврат к выбору

@dp.callback_query(F.data.startswith(SLAVE_PREFIX))
async def buy_slave_handler(callback: types.CallbackQuery):
    try:
        buyer_id = callback.from_user.id
        slave_id = int(callback.data.replace(SLAVE_PREFIX, ""))
        
        # 1. Проверка существования пользователей
        if buyer_id not in users:
            await callback.answer("❌ Вы не зарегистрированы!", show_alert=True)
            return
            
        if slave_id not in users:
            await callback.answer("❌ Раб не найден в системе", show_alert=True)
            return

        buyer = users[buyer_id]
        slave = users[slave_id]

        # 2. Проверка лимита рабов
        barracks_level = buyer.get("upgrades", {}).get("barracks", 0)
        slave_limit = 5 + 2 * barracks_level
        if len(buyer.get("slaves", [])) >= slave_limit:
            await callback.answer(
                f"❌ Лимит рабов ({slave_limit}). Улучшите бараки!",
                show_alert=True
            )
            return

        # 3. Проверка щита защиты (с улучшенной обработкой)
        shield_active = slave.get("shield_active")
        if shield_active:
            try:
                if isinstance(shield_active, str):
                    shield_active = datetime.fromisoformat(shield_active)
                if shield_active and shield_active > datetime.now():
                    await callback.answer(
                        f"🛡 Цель защищена щитом до {shield_active.strftime('%d.%m %H:%M')}",
                        show_alert=True
                    )
                    return
            except Exception as e:
                logging.error(f"Ошибка обработки щита: {e}")

        # 4. Проверка времени перекупа
        last_purchased = slave.get("last_purchased")
        if last_purchased:
            try:
                if isinstance(last_purchased, str):
                    last_purchased = datetime.fromisoformat(last_purchased)
                if (datetime.now() - last_purchased) < timedelta(hours=3):
                    remaining = timedelta(hours=3) - (datetime.now() - last_purchased)
                    hours = remaining.seconds // 3600
                    minutes = (remaining.seconds % 3600) // 60
                    await callback.answer(
                        f"⌛ Раб доступен для перекупа через {hours}ч {minutes}м",
                        show_alert=True
                    )
                    return
            except Exception as e:
                logging.error(f"Ошибка обработки last_purchased: {e}")

        # 5. Проверка на покупку самого себя
        if slave_id == buyer_id:
            await callback.answer("❌ Нельзя купить самого себя!", show_alert=True)
            return

        # 6. Проверка иерархии
        if buyer.get("owner") == slave_id:
            await callback.answer("❌ Нельзя купить своего владельца!", show_alert=True)
            return

        # 7. Проверка двойного владения
        if slave.get("owner") == buyer_id:
            await callback.answer("❌ Этот раб уже принадлежит вам!", show_alert=True)
            return

        # 8. Проверка текущего владельца
        previous_owner_id = slave.get("owner")
        if previous_owner_id and previous_owner_id != buyer_id:
            await callback.answer(
                f"❌ Раб принадлежит @{users[previous_owner_id].get('username', 'unknown')}",
                show_alert=True
            )
            return

        # 9. Расчет цены
        try:
            price = slave_price(slave)
        except Exception as e:
            logging.error(f"Ошибка расчета цены: {e}")
            await callback.answer("❌ Ошибка расчета цены", show_alert=True)
            return

        # 10. Проверка баланса
        if buyer.get("balance", 0) < price:
            await callback.answer(
                f"❌ Недостаточно средств! Нужно {price}₽",
                show_alert=True
            )
            return

        # 11. Процесс покупки
        try:
            # Если был предыдущий владелец
            if previous_owner_id and previous_owner_id in users:
                previous_owner = users[previous_owner_id]
                
                # Удаляем из списка рабов
                if slave_id in previous_owner.get("slaves", []):
                    previous_owner["slaves"].remove(slave_id)
                
                # Начисляем комиссию (30%)
                commission = int(price * 0.3)
                previous_owner["balance"] += commission
                previous_owner["total_income"] += commission

            # Обновляем данные покупателя
            buyer["balance"] -= price
            buyer.setdefault("slaves", []).append(slave_id)

            # Обновляем данные раба
            slave["owner"] = buyer_id
            slave["slave_level"] = min(slave.get("slave_level", 0) + 1, MAX_SLAVE_LEVEL)
            slave["price"] = slave_price(slave)
            slave["enslaved_date"] = datetime.now()
            slave["last_purchased"] = datetime.now()

            # Формируем сообщение об успехе
            message_text = [
                f"✅ Куплен @{slave.get('username', 'unknown')} за {price}₽",
                f"▸ Уровень: {slave['slave_level']}",
                f"▸ Новая цена: {slave['price']}₽",
                f"▸ Доход/час: {int(100 * (1 + 0.3 * slave['slave_level']))}₽"
            ]
            
            if previous_owner_id:
                message_text.append(f"▸ Комиссия предыдущему владельцу: {commission}₽")

            # Уведомление раба
            try:
                await bot.send_message(
                    slave_id,
                    f"⚡ Вы приобретены @{buyer.get('username', 'unknown')} "
                    f"за {price}₽ (уровень {slave['slave_level']})"
                )
            except Exception as e:
                logging.error(f"Не удалось уведомить раба: {e}")

            save_db()
            await callback.message.edit_text(
                "\n".join(message_text),
                reply_markup=main_keyboard()
            )
            await callback.answer()

        except Exception as e:
            logging.error(f"Критическая ошибка при покупке: {e}", exc_info=True)
            await callback.answer("❌ Критическая ошибка при покупке", show_alert=True)

    except Exception as e:
        logging.error(f"Ошибка в обработчике покупки: {e}", exc_info=True)
        await callback.answer("⚠️ Произошла непредвиденная ошибка", show_alert=True)

@dp.callback_query(F.data == "bj_cancel_bet")
async def cancel_bet_handler(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    if user_id in user_search_cache['awaiting_bet']:
        user_search_cache['awaiting_bet'].remove(user_id)
    
    await show_bet_selection(callback.message)
    await callback.answer()

@dp.callback_query(F.data.startswith(BUYOUT_PREFIX))
async def buyout_handler(callback: types.CallbackQuery):
    try:
        user_id = callback.from_user.id
        user = users.get(user_id)
        
        # Базовые проверки
        if not user:
            await callback.answer("❌ Сначала зарегистрируйтесь!", show_alert=True)
            return
            
        if not user.get("owner"):
            await callback.answer("❌ Вы и так свободны!", show_alert=True)
            return

        # Проверка кандалов
        owner = users.get(user["owner"], {})
        if owner.get("shackles", {}).get(user_id):
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
        
        # Формула: (база + 5% капитала) * (1 + 0.3 за уровень)
        buyout_price = int(
            (base_price + user["balance"] * 0.05) * 
            (1 + slave_level * 0.3)
        )
        
        # Ограничения цены
        buyout_price = max(100, min(20000, buyout_price))  # 100-20k
        
        # Проверка баланса (с запасом 1%)
        if user["balance"] < buyout_price * 0.99:
            await callback.answer(
                f"❌ Не хватает {buyout_price - user['balance']:.0f}₽\n"
                f"Требуется: {buyout_price}₽",
                show_alert=True
            )
            return

        # Процесс выкупа
        owner_id = user["owner"]
        user["balance"] -= buyout_price
        user["owner"] = None
        user["price"] = base_price  # Сброс цены
        
        # Владелец получает 60% от выкупа (вместо 50%)
        if owner_id in users:
            owner_income = int(buyout_price * 0.6)
            users[owner_id]["balance"] += owner_income
            users[owner_id]["total_income"] += owner_income
            
            # Удаляем из списка рабов
            if user_id in users[owner_id].get("slaves", []):
                users[owner_id]["slaves"].remove(user_id)
            
            # Уведомление владельца
            try:
                await bot.send_message(
                    owner_id,
                    f"🔓 Раб @{user.get('username', 'unknown')} "
                    f"выкупился за {buyout_price}₽\n"
                    f"Ваш доход: {owner_income}₽"
                )
            except Exception:
                pass

        # Обновление статистики
        user["total_spent"] = user.get("total_spent", 0) + buyout_price
        user["buyout_count"] = user.get("buyout_count", 0) + 1
        
        # Сообщение об успехе
        await callback.message.edit_text(
            f"🎉 <b>Вы свободны!</b>\n"
            f"▸ Потрачено: {buyout_price}₽\n"
            f"▸ Сохранён уровень: {slave_level}\n"
            f"▸ Новая цена: {base_price}₽\n\n"
            f"<i>Теперь вы не платите 30% налог владельцу</i>",
            reply_markup=main_keyboard(),
            parse_mode=ParseMode.HTML
        )
        
        save_db()
        await callback.answer()

    except Exception as e:
        logging.error(f"Buyout error: {e}", exc_info=True)
        await callback.answer("🌀 Произошла ошибка при выкупе", show_alert=True)
# Обновленный профиль
@dp.callback_query(F.data == PROFILE)
async def profile_handler(callback: types.CallbackQuery):
    try:
        user_id = callback.from_user.id
        user = users.get(user_id)
        
        if not user:
            await callback.answer("❌ Сначала зарегистрируйтесь!", show_alert=True)
            return

        # Инициализация отсутствующих полей
        user.setdefault("balance", 0)
        user.setdefault("slave_level", 0)
        user.setdefault("upgrades", {})

        # Расчет цены выкупа
        buyout_price = 0
        if user.get("owner"):
            try:
                base_price = user.get("base_price", 100)
                balance = float(user.get("balance", 0))
                slave_level = user.get("slave_level", 0)
                buyout_price = int((base_price + balance * 0.1) * (1 + slave_level * 0.5))
                buyout_price = max(100, min(10000, buyout_price))
            except Exception as e:
                logging.error(f"Ошибка расчета выкупа: {e}")
                buyout_price = "Ошибка"

        # Уровни улучшений
        storage_level = user["upgrades"].get("storage", 0)
        barracks_level = user["upgrades"].get("barracks", 0)
        whip_level = user["upgrades"].get("whip", 0)

        # Пассивный доход
        passive_per_min = 1 + storage_level * 10
        slave_income = 0
        for slave_id in user.get("slaves", []):
            if slave_id in users:
                slave = users[slave_id]
                slave_level = slave.get("slave_level", 0)
                slave_income += 100 * (1 + 0.3 * slave_level)
        
        passive_per_min += slave_income / 60

        # Формирование текста
        text = [
            f"👑 <b>Профиль @{user.get('username', 'unknown')}</b>",
            f"▸ 💰 Баланс: {float(user['balance']):.1f}₽",
            f"▸ 💸 Пассивка: {passive_per_min:.1f}₽/мин",
            f"▸ 👥 Ур.раба: {user['slave_level']}",
            f"▸ 📦 Склад: ур.{storage_level}",
            f"▸ 🏠 Бараки: ур.{barracks_level}",
        ]

        # Информация о владельце
        if user.get("owner"):
            owner_id = user["owner"]
            owner = users.get(owner_id)
            owner_name = f"@{owner['username']}" if owner else "неизвестный"
            text.append(
                f"\n⚠️ <b>Налог рабства:</b> 30% → {owner_name}\n"
                f"▸ Цена выкупа: {buyout_price}₽"
            )
        else:
            text.append("\n🔗 Вы свободный человек")

        # Клавиатура выкупа
        keyboard = []
        if user.get("owner") and isinstance(buyout_price, int):
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
        await callback.answer("❌ Ошибка загрузки. Попробуйте позже.", show_alert=True)



async def autosave_task():
    while True:
        await asyncio.sleep(300)  # 5 минут
        save_db()
        

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
        await dp.start_polling(
        bot, 
        allowed_updates=dp.resolve_used_update_types(),
        skip_updates=True
    )
        
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
