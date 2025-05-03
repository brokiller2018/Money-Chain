import logging
import asyncio
import json
import os
import psycopg2
import random
import time
import math
from psycopg2.extras import Json
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, FSInputFile
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.enums import ParseMode
from aiogram.client.default import DefaultBotProperties
from aiogram import F
from datetime import datetime, timedelta, timezone
from io import BytesIO
from PIL import Image, ImageDraw, ImageFont
import matplotlib.pyplot as plt
import numpy as np

# ======== CONFIGURATION ========
TOKEN = "8076628423:AAEkp4l3BYkl-6lwz8VAyMw0h7AaAM7J3oM"
CHANNEL_ID = "@memok_da"
CHANNEL_LINK = "https://t.me/memok_da"
ADMIN_IDS = [123456789]  # Add your admin IDs here

# ======== CONSTANTS ========
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
GAMES_MENU = "games_menu"
DAILY_BONUS = "daily_bonus"
ACHIEVEMENTS = "achievements"
CASINO_PREFIX = "casino_"
SLOTS_PREFIX = "slots_"
DICE_PREFIX = "dice_"
LOTTERY_PREFIX = "lottery_"
RACE_PREFIX = "race_"
CLAN_PREFIX = "clan_"
MARKET_PREFIX = "market_"

# Game constants
MAX_SLAVE_LEVEL = 20  # Increased from 15
DAILY_WORK_LIMIT = 5  # Reduced to make other income sources more important
MAX_BARRACKS_LEVEL = 15  # Increased from 10
MIN_SLAVES_FOR_RANDOM = 3
MAX_BET = 100000  # Maximum bet limit

# ======== INITIALIZATION ========
bot = Bot(token=TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
storage = MemoryStorage()
dp = Dispatcher(storage=storage)

# ======== DATABASE ========
users = {}
user_search_cache = {
    'awaiting_bet': set(),
    'awaiting_username': set(),
    'awaiting_dice_bet': set(),
    'awaiting_slots_bet': set(),
    'awaiting_lottery_bet': set(),
    'awaiting_race_bet': set(),
}
active_games = {}
market_listings = []
clans = {}
lottery_pool = 0
lottery_participants = []
lottery_end_time = None
race_active = False
race_participants = []
race_end_time = None
race_prize_pool = 0

# ======== UPGRADES ========
upgrades = {
    "storage": {
        "name": "📦 Склад",
        "base_price": 300, 
        "income_bonus": 10,  # Increased from 5
        "price_multiplier": 1.25,  # Reduced from 1.3
        "description": "+10 монет/мин к пассивному доходу",
        "max_level": 20,
        "emoji": "📦"
    },
    "whip": {
        "name": "⛓ Кнуты", 
        "base_price": 800,
        "income_bonus": 0.2,  # Increased from 0.18
        "price_multiplier": 1.25,  # Reduced from 1.3
        "description": "+20% к доходу от работы",
        "max_level": 15,
        "emoji": "⛓"
    },
    "food": {
        "name": "🍗 Еда",
        "base_price": 1500,
        "income_bonus": 0.1,  # Increased from 0.08
        "price_multiplier": 1.4,  # Reduced from 1.5
        "description": "-10% к времени ожидания работы",
        "max_level": 10,
        "emoji": "🍗"
    },
    "barracks": {
        "name": "🏠 Бараки",
        "base_price": 3000,
        "income_bonus": 2,
        "price_multiplier": 1.5,  # Reduced from 1.6
        "description": "+2 к лимиту рабов",
        "max_level": 15,
        "emoji": "🏠"
    },
    "training": {
        "name": "🏋️ Тренировка",
        "base_price": 5000,
        "income_bonus": 0.15,  # +15% slave income per level
        "price_multiplier": 1.4,
        "description": "+15% к доходу от рабов",
        "max_level": 10,
        "emoji": "🏋️"
    },
    "security": {
        "name": "🔒 Охрана",
        "base_price": 8000,
        "income_bonus": 0.1,  # -10% chance of slave escape per level
        "price_multiplier": 1.5,
        "description": "-10% шанс побега рабов",
        "max_level": 10,
        "emoji": "🔒"
    },
    "market": {
        "name": "🏪 Рынок",
        "base_price": 12000,
        "income_bonus": 0.05,  # +5% to market sales per level
        "price_multiplier": 1.6,
        "description": "+5% к продажам на рынке",
        "max_level": 10,
        "emoji": "🏪"
    },
    "luck": {
        "name": "🍀 Удача",
        "base_price": 15000,
        "income_bonus": 0.05,  # +5% luck in games per level
        "price_multiplier": 1.7,
        "description": "+5% удачи в играх",
        "max_level": 10,
        "emoji": "🍀"
    }
}

# ======== ACHIEVEMENTS ========
achievements = {
    "slave_collector": {
        "name": "Коллекционер рабов",
        "description": "Владеть {target} рабами одновременно",
        "tiers": [5, 10, 25, 50, 100],
        "rewards": [500, 1500, 5000, 15000, 50000],
        "emoji": "👥"
    },
    "rich_master": {
        "name": "Богатый хозяин",
        "description": "Накопить {target}₽",
        "tiers": [10000, 50000, 250000, 1000000, 10000000],
        "rewards": [1000, 5000, 25000, 100000, 1000000],
        "emoji": "💰"
    },
    "work_addict": {
        "name": "Трудоголик",
        "description": "Работать {target} раз",
        "tiers": [10, 50, 100, 500, 1000],
        "rewards": [300, 1500, 3000, 15000, 30000],
        "emoji": "💼"
    },
    "upgrade_master": {
        "name": "Мастер улучшений",
        "description": "Приобрести {target} улучшений",
        "tiers": [5, 15, 30, 50, 100],
        "rewards": [500, 2000, 5000, 10000, 25000],
        "emoji": "🛠"
    },
    "gambler": {
        "name": "Азартный игрок",
        "description": "Сыграть в игры {target} раз",
        "tiers": [10, 50, 100, 500, 1000],
        "rewards": [500, 2500, 5000, 25000, 50000],
        "emoji": "🎮"
    },
    "lucky_winner": {
        "name": "Счастливчик",
        "description": "Выиграть в играх {target} раз",
        "tiers": [5, 25, 50, 250, 500],
        "rewards": [1000, 5000, 10000, 50000, 100000],
        "emoji": "🍀"
    },
    "referral_king": {
        "name": "Король рефералов",
        "description": "Пригласить {target} друзей",
        "tiers": [3, 10, 25, 50, 100],
        "rewards": [1000, 5000, 15000, 35000, 100000],
        "emoji": "👑"
    },
    "freedom_fighter": {
        "name": "Борец за свободу",
        "description": "Выкупиться {target} раз",
        "tiers": [1, 5, 10, 25, 50],
        "rewards": [500, 2500, 5000, 15000, 35000],
        "emoji": "⚔️"
    }
}

# ======== DAILY REWARDS ========
daily_rewards = [
    {"day": 1, "reward": 500, "description": "500₽"},
    {"day": 2, "reward": 1000, "description": "1,000₽"},
    {"day": 3, "reward": 1500, "description": "1,500₽"},
    {"day": 4, "reward": 2000, "description": "2,000₽"},
    {"day": 5, "reward": 3000, "description": "3,000₽"},
    {"day": 6, "reward": 4000, "description": "4,000₽"},
    {"day": 7, "reward": 5000, "description": "5,000₽ + Щит на 24ч"}
]

# ======== KEYBOARDS ========
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
            InlineKeyboardButton(text="🎮 Мини-игры", callback_data=GAMES_MENU),
            InlineKeyboardButton(text="🔗 Рефералка", callback_data=REF_LINK)
        ],
        [
            InlineKeyboardButton(text="🏆 Топ владельцев", callback_data=TOP_OWNERS),
            InlineKeyboardButton(text="🎯 Достижения", callback_data=ACHIEVEMENTS)
        ],
        [
            InlineKeyboardButton(text="🎁 Ежедневный бонус", callback_data=DAILY_BONUS)
        ]
    ])

def games_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="🎲 Кости", callback_data=f"{DICE_PREFIX}menu"),
            InlineKeyboardButton(text="🎰 Слоты", callback_data=f"{SLOTS_PREFIX}menu")
        ],
        [
            InlineKeyboardButton(text="🃏 Блэкджек", callback_data="play_21"),
            InlineKeyboardButton(text="🏇 Скачки", callback_data=f"{RACE_PREFIX}menu")
        ],
        [
            InlineKeyboardButton(text="🎫 Лотерея", callback_data=f"{LOTTERY_PREFIX}menu"),
            InlineKeyboardButton(text="🎯 Дартс", callback_data="darts_menu")
        ],
        [InlineKeyboardButton(text="🔙 Назад", callback_data=MAIN_MENU)]
    ])

def get_db_connection():
    try:
        return psycopg2.connect(os.getenv("DATABASE_URL"))
    except Exception as e:
        logging.error(f"Database connection error: {e}")
        raise

async def show_bet_selection(message: types.Message, game_type="blackjack"):
    """Shows bet selection menu with custom input"""
    builder = InlineKeyboardBuilder()
    
    # Standard bets
    bets = [500, 1000, 2500, 5000, 10000]
    for bet in bets:
        if game_type == "blackjack":
            builder.button(text=f"{bet}₽", callback_data=f"bj_bet_{bet}")
        elif game_type == "dice":
            builder.button(text=f"{bet}₽", callback_data=f"{DICE_PREFIX}bet_{bet}")
        elif game_type == "slots":
            builder.button(text=f"{bet}₽", callback_data=f"{SLOTS_PREFIX}bet_{bet}")
        elif game_type == "lottery":
            builder.button(text=f"{bet}₽", callback_data=f"{LOTTERY_PREFIX}bet_{bet}")
        elif game_type == "race":
            builder.button(text=f"{bet}₽", callback_data=f"{RACE_PREFIX}bet_{bet}")
    
    # Additional buttons
    if game_type == "blackjack":
        builder.button(text="🎲 Своя ставка", callback_data="bj_custom_bet")
        builder.button(text="🔙 В меню", callback_data=GAMES_MENU)
    elif game_type == "dice":
        builder.button(text="🎲 Своя ставка", callback_data=f"{DICE_PREFIX}custom_bet")
        builder.button(text="🔙 В меню", callback_data=GAMES_MENU)
    elif game_type == "slots":
        builder.button(text="🎲 Своя ставка", callback_data=f"{SLOTS_PREFIX}custom_bet")
        builder.button(text="🔙 В меню", callback_data=GAMES_MENU)
    elif game_type == "lottery":
        builder.button(text="🎲 Своя ставка", callback_data=f"{LOTTERY_PREFIX}custom_bet")
        builder.button(text="🔙 В меню", callback_data=GAMES_MENU)
    elif game_type == "race":
        builder.button(text="🎲 Своя ставка", callback_data=f"{RACE_PREFIX}custom_bet")
        builder.button(text="🔙 В меню", callback_data=GAMES_MENU)
    
    # Layout optimization
    builder.adjust(3, 2, 1)
    
    game_titles = {
        "blackjack": "🃏 Блэкджек",
        "dice": "🎲 Кости",
        "slots": "🎰 Слоты",
        "lottery": "🎫 Лотерея",
        "race": "🏇 Скачки"
    }
    
    title = game_titles.get(game_type, "Игра")
    
    await message.edit_text(
        f"{title}\n\n🎰 Выберите или введите ставку:",
        reply_markup=builder.as_markup()
    )

async def cleanup_games():
    while True:
        await asyncio.sleep(300)  # Check every 5 minutes
        try:
            current_time = datetime.now()
            expired = []
            for user_id, game in active_games.items():
                # Remove only completed or stuck games
                if game.game_over or (current_time - game.last_action_time).total_seconds() > 1800:
                    expired.append(user_id)

            for user_id in expired:
                if user_id in active_games:
                    del active_games[user_id]
                    logging.info(f"Cleaned up game for user {user_id}")

        except Exception as e:
            logging.error(f"Game cleanup error: {e}")

async def check_lottery():
    global lottery_pool, lottery_participants, lottery_end_time
    
    while True:
        await asyncio.sleep(60)  # Check every minute
        
        try:
            now = datetime.now()
            
            # If lottery is active and time is up
            if lottery_end_time and now > lottery_end_time:
                # Draw winner
                if lottery_participants:
                    winner_id = random.choice(lottery_participants)
                    winner = users.get(winner_id)
                    
                    if winner:
                        # Award prize
                        winner["balance"] += lottery_pool
                        winner["total_income"] += lottery_pool
                        winner["lottery_wins"] = winner.get("lottery_wins", 0) + 1
                        
                        # Update stats for achievement tracking
                        winner["games_won"] = winner.get("games_won", 0) + 1
                        winner["games_played"] = winner.get("games_played", 0) + 1
                        
                        # Notify winner
                        try:
                            await bot.send_message(
                                winner_id,
                                f"🎉 Поздравляем! Вы выиграли в лотерее {lottery_pool}₽!"
                            )
                        except Exception as e:
                            logging.error(f"Failed to notify lottery winner: {e}")
                    
                    # Notify all participants
                    for participant_id in lottery_participants:
                        if participant_id != winner_id:
                            try:
                                await bot.send_message(
                                    participant_id,
                                    f"🎫 Лотерея завершена! Победитель: @{winner.get('username', 'unknown')}\n"
                                    f"Выигрыш: {lottery_pool}₽\n\n"
                                    f"Удачи в следующий раз!"
                                )
                            except Exception:
                                pass
                
                # Reset lottery
                lottery_pool = 0
                lottery_participants = []
                lottery_end_time = now + timedelta(hours=6)  # Next lottery in 6 hours
                
        except Exception as e:
            logging.error(f"Lottery check error: {e}")

async def check_race():
    global race_active, race_participants, race_end_time, race_prize_pool
    
    while True:
        await asyncio.sleep(60)  # Check every minute
        
        try:
            now = datetime.now()
            
            # If race is active and time is up
            if race_active and race_end_time and now > race_end_time:
                # Determine winners
                if race_participants:
                    # Shuffle participants to randomize race results
                    random.shuffle(race_participants)
                    
                    # Calculate prizes (60% to 1st, 30% to 2nd, 10% to 3rd)
                    prizes = [
                        int(race_prize_pool * 0.6),
                        int(race_prize_pool * 0.3),
                        int(race_prize_pool * 0.1)
                    ]
                    
                    # Determine number of winners (up to 3)
                    num_winners = min(3, len(race_participants))
                    
                    # Award prizes to winners
                    for i in range(num_winners):
                        winner_id = race_participants[i]
                        winner = users.get(winner_id)
                        
                        if winner:
                            # Award prize
                            winner["balance"] += prizes[i]
                            winner["total_income"] += prizes[i]
                            winner["race_wins"] = winner.get("race_wins", 0) + 1
                            
                            # Update stats for achievement tracking
                            winner["games_won"] = winner.get("games_won", 0) + 1
                            winner["games_played"] = winner.get("games_played", 0) + 1
                            
                            # Notify winner
                            try:
                                await bot.send_message(
                                    winner_id,
                                    f"🏇 Поздравляем! Ваша лошадь пришла {i+1}-й!\n"
                                    f"Выигрыш: {prizes[i]}₽!"
                                )
                            except Exception as e:
                                logging.error(f"Failed to notify race winner: {e}")
                    
                    # Notify all participants
                    winners_text = "\n".join([
                        f"{i+1}. @{users.get(race_participants[i], {}).get('username', 'unknown')} - {prizes[i]}₽"
                        for i in range(num_winners)
                    ])
                    
                    for participant_id in race_participants:
                        if participant_id not in race_participants[:num_winners]:
                            try:
                                await bot.send_message(
                                    participant_id,
                                    f"🏇 Скачки завершены!\n\n"
                                    f"Победители:\n{winners_text}\n\n"
                                    f"К сожалению, ваша лошадь не заняла призовое место.\n"
                                    f"Удачи в следующий раз!"
                                )
                            except Exception:
                                pass
                
                # Reset race
                race_active = False
                race_participants = []
                race_prize_pool = 0
                race_end_time = None
                
                # Schedule next race
                next_race_time = now + timedelta(hours=4)  # Next race in 4 hours
                race_end_time = next_race_time
                race_active = True
                
        except Exception as e:
            logging.error(f"Race check error: {e}")

async def check_slave_escapes():
    while True:
        await asyncio.sleep(3600)  # Check every hour
        
        try:
            for user_id, user in users.items():
                if not user.get("slaves"):
                    continue
                    
                # Check each slave for possible escape
                for slave_id in list(user.get("slaves", [])):
                    if slave_id not in users:
                        continue
                        
                    slave = users[slave_id]
                    
                    # Base escape chance: 5%
                    escape_chance = 0.05
                    
                    # Reduce chance based on security upgrade
                    security_level = user.get("upgrades", {}).get("security", 0)
                    escape_chance -= security_level * 0.01  # -1% per level
                    
                    # Increase chance based on slave level
                    slave_level = slave.get("slave_level", 0)
                    escape_chance += slave_level * 0.005  # +0.5% per level
                    
                    # Ensure chance is between 1% and 15%
                    escape_chance = max(0.01, min(0.15, escape_chance))
                    
                    # Roll for escape
                    if random.random() < escape_chance:
                        # Slave escapes!
                        if slave_id in user.get("slaves", []):
                            user["slaves"].remove(slave_id)
                        
                        # Update slave data
                        slave["owner"] = None
                        slave["enslaved_date"] = None
                        
                        # Notify owner
                        try:
                            await bot.send_message(
                                user_id,
                                f"⚠️ Ваш раб @{slave.get('username', 'unknown')} сбежал!\n"
                                f"Улучшите охрану, чтобы снизить шанс побега."
                            )
                        except Exception:
                            pass
                        
                        # Notify slave
                        try:
                            await bot.send_message(
                                slave_id,
                                f"🎉 Вам удалось сбежать от @{user.get('username', 'unknown')}!\n"
                                f"Теперь вы свободны."
                            )
                        except Exception:
                            pass
                        
        except Exception as e:
            logging.error(f"Slave escape check error: {e}")

async def on_startup():
    global users, lottery_end_time, race_end_time, race_active
    
    users = load_db()  # Load DB at startup
    
    # Initialize lottery
    lottery_end_time = datetime.now() + timedelta(hours=6)
    
    # Initialize race
    race_end_time = datetime.now() + timedelta(hours=4)
    race_active = True
    
    # Start background tasks
    asyncio.create_task(passive_income_task())
    asyncio.create_task(autosave_task())
    asyncio.create_task(cleanup_games())
    asyncio.create_task(check_lottery())
    asyncio.create_task(check_race())
    asyncio.create_task(check_slave_escapes())
    
    # Save DB on proper shutdown
    def save_on_exit(*args):
        save_db()
    
    import signal
    signal.signal(signal.SIGTERM, save_on_exit)
    signal.signal(signal.SIGINT, save_on_exit)
    
    logging.info("Bot successfully started")

# ======== GAME CLASSES ========
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

class BlackjackGame:
    def __init__(self, user_id: int, bet: int, bot: Bot):
        self.user_id = user_id
        self.bet = bet
        self.bot = bot
        self.deck = self.create_deck()
        self.player_hand = []
        self.dealer_hand = []
        self.game_over = False
        self.message = None
        self.last_action_time = datetime.now()

    async def start_game(self, message: types.Message):
        try:
            self.message = message
            self.deck = self.create_deck()
            random.shuffle(self.deck)
            self.player_hand = [self.deal_card(), self.deal_card()]
            self.dealer_hand = [self.deal_card(), self.deal_card()]
            
            # Check for blackjack
            player_value = self.calculate_hand(self.player_hand)
            dealer_value = self.calculate_hand(self.dealer_hand)
            
            if player_value == 21 and dealer_value == 21:
                # Both have blackjack - push
                await self.end_game('draw')
                return
            elif player_value == 21:
                # Player has blackjack
                await self.end_game('blackjack')
                return
            elif dealer_value == 21:
                # Dealer has blackjack
                await self.end_game('lose')
                return
            
            active_games[self.user_id] = self  # Explicit save
            
            await self.update_display()
            
        except Exception as e:
            logging.error(f"Game start error: {e}")
            await self.cleanup_game()

    def create_deck(self):
        """Creates and returns a 52-card deck"""
        suits = ['Spades', 'Hearts', 'Diamonds', 'Clubs']
        ranks = ['2', '3', '4', '5', '6', '7', '8', '9', '10', 'J', 'Q', 'K', 'A']
        return [Card(suit, rank) for suit in suits for rank in ranks]

    def deal_card(self):
        """Deals one card from the deck"""
        if not self.deck:
            # Recreate deck if empty
            self.deck = self.create_deck()
            random.shuffle(self.deck)
        return self.deck.pop()

    def calculate_hand(self, hand):
        """Calculates hand value considering Ace as 11 or 1"""
        value = sum(card.value for card in hand)
        aces = sum(1 for card in hand if card.rank == 'A')
        
        while value > 21 and aces:
            value -= 10
            aces -= 1
        return value

    async def cleanup_game(self):
        """Cleans up game resources"""
        if self.user_id in active_games:
            del active_games[self.user_id]
        self.game_over = True

    async def end_game(self, result: str):
        try:
            self.game_over = True
            player_value = self.calculate_hand(self.player_hand)
            dealer_value = self.calculate_hand(self.dealer_hand)
    
            user = users.get(self.user_id)
            if not user:
                raise ValueError(f"User {self.user_id} not found")
    
            # Remove game from active before changing balance
            if self.user_id in active_games:
                del active_games[self.user_id]
    
            # Automatic result determination if not specified
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
    
            # Result calculation
            if result == 'blackjack':
                win_amount = int(self.bet * 2.5)
                user["balance"] += win_amount
                user["total_income"] += win_amount
                text = f"🎉 Blackjack! Выигрыш: {win_amount}₽!"
            elif result == 'win':
                win_amount = self.bet
                user["balance"] += win_amount
                user["total_income"] += win_amount
                text = f"🎉 Выигрыш: {win_amount}₽!"
            elif result == 'draw':
                text = "🤝 Ничья!"
            else:
                user["balance"] -= self.bet
                text = f"💸 Проигрыш: {self.bet}₽"
            
            # Update stats for achievement tracking
            user["games_played"] = user.get("games_played", 0) + 1
            if result in ['blackjack', 'win']:
                user["games_won"] = user.get("games_won", 0) + 1
    
            save_db()
    
            await self.message.edit_text(
                f"{text}\n\n"
                f"Ваши карты: {self.player_hand} ({player_value})\n"
                f"Карты дилера: {self.dealer_hand} ({dealer_value})",
                reply_markup=main_keyboard()
            )
    
        except Exception as e:
            logging.error(f"Game end error: {e}")
            if self.user_id in active_games:
                del active_games[self.user_id]
            await self.message.answer("⚠️ Игра завершена", reply_markup=main_keyboard())

    async def dealer_turn(self):
        """Handles dealer's turn"""
        try:
            while self.calculate_hand(self.dealer_hand) < 17:
                self.dealer_hand.append(self.deal_card())
            await self.end_game(None)
        except Exception as e:
            logging.error(f"Dealer turn error: {e}")
            await self.cleanup_game()

    async def update_display(self):
        try:
            if self.game_over:
                dealer_status = f"Дилер: {self.calculate_hand(self.dealer_hand)}"
            else:
                dealer_status = f"Дилер: {self.dealer_hand[0]} ?"
                
            await self.message.edit_text(
                f"🃏 <b>Блэкджек</b>\n\n"
                f"💰 Ставка: {self.bet}₽\n"
                f"Ваши карты: {self.player_hand} ({self.calculate_hand(self.player_hand)})\n"
                f"{dealer_status}",
                reply_markup=get_game_keyboard(self),
                parse_mode=ParseMode.HTML
            )
        except Exception as e:
            logging.error(f"Display update error: {e}")

class DiceGame:
    def __init__(self, user_id: int, bet: int, bot: Bot):
        self.user_id = user_id
        self.bet = bet
        self.bot = bot
        self.player_roll = None
        self.house_roll = None
        self.game_over = False
        self.message = None
        
    async def start_game(self, message: types.Message):
        try:
            self.message = message
            
            # Roll dice
            self.player_roll = random.randint(1, 6) + random.randint(1, 6)
            self.house_roll = random.randint(1, 6) + random.randint(1, 6)
            
            # Apply luck bonus
            user = users.get(self.user_id)
            if user:
                luck_level = user.get("upgrades", {}).get("luck", 0)
                if luck_level > 0 and random.random() < luck_level * 0.05:
                    # Lucky roll - increase player's roll by 1-2
                    self.player_roll = min(12, self.player_roll + random.randint(1, 2))
            
            # Determine result
            if self.player_roll > self.house_roll:
                result = 'win'
                win_amount = self.bet
                user["balance"] += win_amount
                user["total_income"] += win_amount
                text = f"🎲 Вы выиграли! {self.player_roll} > {self.house_roll}\n\n🎉 Выигрыш: {win_amount}₽!"
            elif self.player_roll < self.house_roll:
                result = 'lose'
                user["balance"] -= self.bet
                text = f"🎲 Вы проиграли! {self.player_roll} < {self.house_roll}\n\n💸 Проигрыш: {self.bet}₽"
            else:
                result = 'draw'
                text = f"🎲 Ничья! {self.player_roll} = {self.house_roll}\n\n🤝 Ставка возвращена"
            
            # Update stats for achievement tracking
            user["games_played"] = user.get("games_played", 0) + 1
            if result == 'win':
                user["games_won"] = user.get("games_won", 0) + 1
            
            save_db()
            
            await message.edit_text(
                text,
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="🎲 Играть снова", callback_data=f"{DICE_PREFIX}menu")],
                    [InlineKeyboardButton(text="🔙 В меню игр", callback_data=GAMES_MENU)]
                ])
            )
            
        except Exception as e:
            logging.error(f"Dice game error: {e}")
            await message.edit_text(
                "⚠️ Произошла ошибка при игре в кости",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="🔙 В меню игр", callback_data=GAMES_MENU)]
                ])
            )

class SlotsGame:
    def __init__(self, user_id: int, bet: int, bot: Bot):
        self.user_id = user_id
        self.bet = bet
        self.bot = bot
        self.reels = None
        self.game_over = False
        self.message = None
        
    async def start_game(self, message: types.Message):
        try:
            self.message = message
            
            # Define symbols and their payouts
            symbols = ["🍒", "🍋", "🍊", "🍇", "🍉", "💎", "7️⃣", "🎰"]
            payouts = {
                "🍒": 1.5,  # Cherry
                "🍋": 1.8,  # Lemon
                "🍊": 2.0,  # Orange
                "🍇": 2.5,  # Grapes
                "🍉": 3.0,  # Watermelon
                "💎": 5.0,  # Diamond
                "7️⃣": 7.0,  # Seven
                "🎰": 10.0   # Jackpot
            }
            
            # Weights for symbols (higher = more common)
            weights = [20, 18, 15, 12, 10, 5, 3, 1]
            
            # Apply luck bonus
            user = users.get(self.user_id)
            luck_level = user.get("upgrades", {}).get("luck", 0)
            if luck_level > 0:
                # Slightly improve weights for better symbols
                for i in range(4, len(weights)):
                    weights[i] += luck_level * 0.5
            
            # Spin the reels
            self.reels = []
            for _ in range(3):
                self.reels.append(random.choices(symbols, weights=weights, k=1)[0])
            
            # Check for wins
            if self.reels[0] == self.reels[1] == self.reels[2]:
                # All three match - big win
                multiplier = payouts[self.reels[0]]
                win_amount = int(self.bet * multiplier)
                user["balance"] += win_amount
                user["total_income"] += win_amount
                text = f"🎰 ДЖЕКПОТ! Три {self.reels[0]}!\n\n🎉 Выигрыш: {win_amount}₽! (x{multiplier})"
                result = 'win'
            elif self.reels[0] == self.reels[1] or self.reels[1] == self.reels[2] or self.reels[0] == self.reels[2]:
                # Two match - small win
                if self.reels[0] == self.reels[1]:
                    matching = self.reels[0]
                elif self.reels[1] == self.reels[2]:
                    matching = self.reels[1]
                else:
                    matching = self.reels[0]
                
                multiplier = payouts[matching] * 0.5
                win_amount = int(self.bet * multiplier)
                user["balance"] += win_amount
                user["total_income"] += win_amount
                text = f"🎰 Совпадение! Два {matching}!\n\n🎉 Выигрыш: {win_amount}₽! (x{multiplier})"
                result = 'win'
            else:
                # No matches - lose
                user["balance"] -= self.bet
                text = f"🎰 Нет совпадений!\n\n💸 Проигрыш: {self.bet}₽"
                result = 'lose'
            
            # Update stats for achievement tracking
            user["games_played"] = user.get("games_played", 0) + 1
            if result == 'win':
                user["games_won"] = user.get("games_won", 0) + 1
            
            save_db()
            
            # Display reels
            reels_display = " | ".join(self.reels)
            
            await message.edit_text(
                f"🎰 <b>Слоты</b>\n\n"
                f"[ {reels_display} ]\n\n"
                f"{text}",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="🎰 Крутить снова", callback_data=f"{SLOTS_PREFIX}menu")],
                    [InlineKeyboardButton(text="🔙 В меню игр", callback_data=GAMES_MENU)]
                ]),
                parse_mode=ParseMode.HTML
            )
            
        except Exception as e:
            logging.error(f"Slots game error: {e}")
            await message.edit_text(
                "⚠️ Произошла ошибка при игре в слоты",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="🔙 В меню игр", callback_data=GAMES_MENU)]
                ])
            )

def get_game_keyboard(game: BlackjackGame) -> InlineKeyboardMarkup:
    """Creates keyboard for game actions"""
    keyboard = InlineKeyboardBuilder()
    
    # Action buttons with new prefixes
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
    user = users[user_id]
    
    for upgrade_id, data in upgrades.items():
        level = user.get("upgrades", {}).get(upgrade_id, 0)
        max_level = data.get("max_level", 10)
        
        if level >= max_level:
            # Max level reached
            buttons.append([
                InlineKeyboardButton(
                    text=f"{data['emoji']} {data['name']} (Ур. {level}) - МАКС",
                    callback_data=f"max_{upgrade_id}"
                )
            ])
        else:
            # Calculate price with exponential growth
            price = int(data["base_price"] * (data["price_multiplier"] ** level))
            
            # Format price with commas
            formatted_price = f"{price:,}".replace(",", " ")
            
            buttons.append([
                InlineKeyboardButton(
                    text=f"{data['emoji']} {data['name']} (Ур. {level}) - {formatted_price}₽ | {data['description']}",
                    callback_data=f"{UPGRADE_PREFIX}{upgrade_id}"
                )
            ])
    
    buttons.append([InlineKeyboardButton(text="🔙 Назад", callback_data=MAIN_MENU)])
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def buy_menu_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔍 Поиск по юзернейму", callback_data=SEARCH_USER)],
        [InlineKeyboardButton(text="🎲 Случайные рабы (Топ-10)", callback_data="random_slaves")],
        [InlineKeyboardButton(text="📊 Рынок рабов", callback_data=f"{MARKET_PREFIX}view")],
        [InlineKeyboardButton(text="🔙 Назад", callback_data=MAIN_MENU)]
    ])

def serialize_user_data(user_data: dict) -> dict:
    """Convert datetime objects to strings for JSON"""
    serialized = {}
    for key, value in user_data.items():
        if isinstance(value, datetime):
            serialized[key] = value.isoformat()
        elif isinstance(value, dict) and key == "shackles":
            # Serialize shackles
            serialized[key] = {
                str(slave_id): end_time.isoformat() 
                for slave_id, end_time in value.items()
            }
        elif isinstance(value, dict) and key == "achievements":
            # Serialize achievements
            serialized[key] = {
                achievement_id: {
                    "level": level,
                    "claimed": claimed,
                    "progress": progress
                }
                for achievement_id, (level, claimed, progress) in value.items()
            }
        else:
            serialized[key] = value
    return serialized

def deserialize_user_data(data: dict) -> dict:
    """Restore datetime from strings"""
    deserialized = {}
    for key, value in data.items():
        if key in ['last_passive', 'last_work', 'shield_active', 'last_purchased', 'enslaved_date', 'last_daily', 'clan_joined'] and value:
            try:
                deserialized[key] = datetime.fromisoformat(value)
            except (TypeError, ValueError):
                deserialized[key] = None
        elif key == "shackles" and isinstance(value, dict):
            # Deserialize shackles
            deserialized[key] = {
                int(slave_id): datetime.fromisoformat(end_time)
                for slave_id, end_time in value.items()
            }
        elif key == "achievements" and isinstance(value, dict):
            # Deserialize achievements
            deserialized[key] = {
                achievement_id: (
                    achievement_data.get("level", 0),
                    achievement_data.get("claimed", False),
                    achievement_data.get("progress", 0)
                )
                for achievement_id, achievement_data in value.items()
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
                
                # Serialize data before saving
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
                
                # Save clans
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS bot_clans (
                        clan_id TEXT PRIMARY KEY,
                        data JSONB NOT NULL,
                        last_updated TIMESTAMP WITH TIME ZONE DEFAULT NOW()
                    )
                """)
                
                for clan_id, clan_data in clans.items():
                    cur.execute("""
                        INSERT INTO bot_clans (clan_id, data)
                        VALUES (%s, %s)
                        ON CONFLICT (clan_id) 
                        DO UPDATE SET 
                            data = EXCLUDED.data,
                            last_updated = NOW()
                    """, (clan_id, Json(clan_data)))
                
                # Save market listings
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS bot_market (
                        listing_id SERIAL PRIMARY KEY,
                        data JSONB NOT NULL,
                        created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
                    )
                """)
                
                # Clear old listings
                cur.execute("DELETE FROM bot_market")
                
                # Insert new listings
                for listing in market_listings:
                    cur.execute("""
                        INSERT INTO bot_market (data)
                        VALUES (%s)
                    """, (Json(listing),))
                
        logging.info("Database saved successfully")
        
    except psycopg2.Error as e:
        logging.error(f"Database error in save_db: {e}")
    finally:
        if conn:
            conn.close()

def load_db():
    conn = None
    try:
        conn = get_db_connection()
        with conn:
            with conn.cursor() as cur:
                # Load users
                cur.execute("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_name = 'bot_users'
                    )
                """)
                table_exists = cur.fetchone()[0]
                
                loaded_users = {}
                
                if table_exists:
                    cur.execute("SELECT user_id, data FROM bot_users")
                    rows = cur.fetchall()
                    
                    for user_id, data in rows:
                        # Deserialize data when loading
                        loaded_users[user_id] = deserialize_user_data(data)
                
                # Load clans
                cur.execute("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_name = 'bot_clans'
                    )
                """)
                table_exists = cur.fetchone()[0]
                
                global clans
                clans = {}
                
                if table_exists:
                    cur.execute("SELECT clan_id, data FROM bot_clans")
                    rows = cur.fetchall()
                    
                    for clan_id, data in rows:
                        clans[clan_id] = data
                
                # Load market listings
                cur.execute("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_name = 'bot_market'
                    )
                """)
                table_exists = cur.fetchone()[0]
                
                global market_listings
                market_listings = []
                
                if table_exists:
                    cur.execute("SELECT data FROM bot_market")
                    rows = cur.fetchall()
                    
                    for (data,) in rows:
                        market_listings.append(data)
                
                logging.info(f"Loaded {len(loaded_users)} users, {len(clans)} clans, and {len(market_listings)} market listings from database")
                return loaded_users
                
    except psycopg2.Error as e:
        logging.error(f"Database error in load_db: {e}")
        return {}
    finally:
        if conn:
            conn.close()

def slave_level(slave_id):
    """Get slave level safely"""
    if slave_id in users:
        return users[slave_id].get("slave_level", 0)
    return 0

def calculate_shield_price(user_id):
    user = users[user_id]
    # Base income (1 + storage) per minute
    passive_per_min = 1 + user.get("upgrades", {}).get("storage", 0) * 10
    # Income from slaves per minute
    passive_per_min += sum(
        100 * (1 + 0.3 * slave_level(slave_id))
        for slave_id in user.get("slaves", [])
    ) / 60
    # Price = 50% of income for 6 hours, rounded
    base_price = passive_per_min * 60 * 6
    # Get shield purchase count
    shop_purchases = user.get("shop_purchases", 0)
    price = base_price * (1.1 ** shop_purchases) 
    price = max(500, min(15000, price))  # New limits
    # First purchase discount
    if user.get("shop_purchases", 0) == 0:
        price = int(price * 0.7)
    return int(price)

def calculate_shackles_price(owner_id):
    owner = users[owner_id]
    
    # 1. Calculate owner's passive income per hour
    passive_income = (
        1 + owner.get("upgrades", {}).get("storage", 0) * 10  # Storage
    ) * 60  # Per hour
    
    # 2. Add income from all slaves
    for slave_id in owner.get("slaves", []):
        if slave_id in users:
            slave = users[slave_id]
            passive_income += 100 * (1 + 0.3 * slave.get("slave_level", 0))
    
    # 3. Price = 150% of hourly income, rounded to 100
    price = int(passive_income * 1.5 / 100) * 100
    
    # 4. Limit range (300–15,000₽)
    return max(300, min(15_000, price))

def slave_price(slave_data: dict) -> int:
    """Calculate slave price based on level"""
    base_price = 500
    level = min(slave_data.get("slave_level", 0), MAX_SLAVE_LEVEL)
    
    # Exponential growth with level
    price = int(base_price * (1.5 ** level))
    
    # Add bonus for upgrades
    for upgrade_id, upgrade_level in slave_data.get("upgrades", {}).items():
        if upgrade_level > 0:
            price += upgrade_level * 200  # 200₽ per upgrade level
    
    return price

def format_time(seconds):
    """Format seconds into a human-readable time string"""
    if seconds < 60:
        return f"{seconds}с"
    elif seconds < 3600:
        minutes = seconds // 60
        seconds %= 60
        return f"{minutes}м {seconds}с"
    else:
        hours = seconds // 3600
        minutes = (seconds % 3600) // 60
        return f"{hours}ч {minutes}м"

def format_number(number):
    """Format number with thousand separators"""
    return f"{number:,}".replace(",", " ")

def check_achievement_progress(user_id):
    """Check and update achievement progress for a user"""
    user = users.get(user_id)
    if not user:
        return
    
    # Initialize achievements if not present
    if "achievements" not in user:
        user["achievements"] = {}
    
    # Check each achievement
    for achievement_id, achievement_data in achievements.items():
        # Get current level, claimed status, and progress
        current = user["achievements"].get(achievement_id, (0, False, 0))
        current_level, claimed, progress = current
        
        # Skip if already at max level and claimed
        if current_level >= len(achievement_data["tiers"]) and claimed:
            continue
        
        # Calculate progress based on achievement type
        if achievement_id == "slave_collector":
            progress = len(user.get("slaves", []))
        elif achievement_id == "rich_master":
            progress = user.get("balance", 0)
        elif achievement_id == "work_addict":
            progress = user.get("work_count", 0)
        elif achievement_id == "upgrade_master":
            progress = sum(level for level in user.get("upgrades", {}).values())
        elif achievement_id == "gambler":
            progress = user.get("games_played", 0)
        elif achievement_id == "lucky_winner":
            progress = user.get("games_won", 0)
        elif achievement_id == "referral_king":
            progress = len(user.get("referrals", []))
        elif achievement_id == "freedom_fighter":
            progress = user.get("buyout_count", 0)
        
        # Update progress
        user["achievements"][achievement_id] = (current_level, claimed, progress)

# Helper functions
async def check_subscription(user_id: int):
    try:
        member = await bot.get_chat_member(chat_id=CHANNEL_ID, user_id=user_id)
        return member.status not in ['left', 'kicked']
    except Exception as e:
        logging.error(f"Subscription check error: {e}")
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
                mins_passed = min(mins_passed, 180)  # Maximum 3 hours, even if bot was off
                
                # 1. Base income (1₽/min)
                base_income = 1 * mins_passed
                
                # 2. Storage income (10₽/min per level)
                storage_level = user.get("upgrades", {}).get("storage", 0)
                storage_income = storage_level * 10 * mins_passed
                
                # 3. Slave income with tax
                slaves_income = 0
                if user.get("slaves"):
                    for slave_id in user["slaves"]:
                        if slave_id in users:
                            slave = users[slave_id]
                            
                            # Slave income (100₽/hour base + 30% per level)
                            slave_income = 100 * (1 + 0.3 * slave.get("slave_level", 0)) * (mins_passed / 60)
                            
                            # Apply training bonus
                            training_level = user.get("upgrades", {}).get("training", 0)
                            if training_level > 0:
                                slave_income *= (1 + training_level * 0.15)
                            
                            # Tax depends on SLAVE LEVEL (not owner's!)
                            slave_level = slave.get("slave_level", 0)
                            tax_rate = min(0.1 + 0.05 * slave_level, 0.3)
                            tax = int(slave_income * tax_rate)
                            
                            # Slave gets 70-90% of income
                            slave["balance"] = min(slave.get("balance", 0) + slave_income - tax, 1_000_000)
                            
                            # Owner gets 10-30% tax
                            slaves_income += tax
                
                # 4. Clan bonus (if any)
                clan_bonus = 0
                if user.get("clan_id") and user["clan_id"] in clans:
                    clan = clans[user["clan_id"]]
                    clan_level = clan.get("level", 0)
                    clan_bonus = (base_income + storage_income + slaves_income) * (clan_level * 0.05)
                
                # Total income (base + storage + slave taxes + clan bonus)
                total_income = base_income + storage_income + slaves_income + clan_bonus
                
                # Overflow protection
                user["balance"] = min(user.get("balance", 0) + total_income, 1_000_000_000)
                user["total_income"] = user.get("total_income", 0) + total_income
                user["last_passive"] = now
                
                # Check achievement progress
                check_achievement_progress(user_id)
            
            # Save every 5 minutes (not every iteration)
            if int(time.time()) % 300 == 0:
                save_db()
                
        except Exception as e:
            logging.error(f"Error in passive_income_task: {e}", exc_info=True)
            await asyncio.sleep(10)  # Pause on error

# Command handlers
@dp.callback_query(F.data == "bj_custom_bet")
async def handle_custom_bet(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    user_search_cache['awaiting_bet'].add(user_id)  # Add to waiting
    
    await callback.message.edit_text(
        "💎 Введите сумму ставки цифрами (мин 100₽, макс 100,000₽):",
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[[InlineKeyboardButton(text="🔙 Назад", callback_data="bj_cancel_bet")]]
        )
    )
    await callback.answer("Введите сумму ставки в чат")

@dp.callback_query(F.data == GAMES_MENU)
async def games_menu_handler(callback: types.CallbackQuery):
    await callback.message.edit_text(
        "🎮 <b>Мини-игры</b>\n\n"
        "Выберите игру:",
        reply_markup=games_keyboard(),
        parse_mode=ParseMode.HTML
    )
    await callback.answer()

@dp.callback_query(F.data == f"{DICE_PREFIX}menu")
async def dice_menu_handler(callback: types.CallbackQuery):
    await callback.message.edit_text(
        "🎲 <b>Кости</b>\n\n"
        "Бросьте кости и выиграйте, если ваше число больше, чем у дилера!\n"
        "Каждый бросает два кубика (2-12 очков).",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🎲 Сделать ставку", callback_data=f"{DICE_PREFIX}bet_menu")],
            [InlineKeyboardButton(text="🔙 Назад", callback_data=GAMES_MENU)]
        ]),
        parse_mode=ParseMode.HTML
    )
    await callback.answer()

@dp.callback_query(F.data == f"{DICE_PREFIX}bet_menu")
async def dice_bet_menu_handler(callback: types.CallbackQuery):
    await show_bet_selection(callback.message, game_type="dice")
    await callback.answer()

@dp.callback_query(F.data.startswith(f"{DICE_PREFIX}bet_"))
async def dice_bet_handler(callback: types.CallbackQuery):
    try:
        user_id = callback.from_user.id
        bet = int(callback.data.split("_")[2])
        
        # Balance check
        if users[user_id]["balance"] < bet:
            await callback.answer("❌ Недостаточно средств!", show_alert=True)
            return
        
        # Start game
        game = DiceGame(user_id, bet, bot)
        await game.start_game(callback.message)
        
    except Exception as e:
        logging.error(f"Dice bet error: {e}")
        await callback.answer("❌ Ошибка при старте игры!")

@dp.callback_query(F.data == f"{DICE_PREFIX}custom_bet")
async def dice_custom_bet_handler(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    user_search_cache['awaiting_dice_bet'].add(user_id)
    
    await callback.message.edit_text(
        "💎 Введите сумму ставки цифрами (мин 100₽, макс 100,000₽):",
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[[InlineKeyboardButton(text="🔙 Назад", callback_data=f"{DICE_PREFIX}cancel_bet")]]
        )
    )
    await callback.answer("Введите сумму ставки в чат")

@dp.callback_query(F.data == f"{SLOTS_PREFIX}menu")
async def slots_menu_handler(callback: types.CallbackQuery):
    await callback.message.edit_text(
        "🎰 <b>Слоты</b>\n\n"
        "Крутите барабаны и выигрывайте!\n"
        "Три одинаковых символа - джекпот!\n"
        "Два одинаковых - малый выигрыш!",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🎰 Сделать ставку", callback_data=f"{SLOTS_PREFIX}bet_menu")],
            [InlineKeyboardButton(text="🔙 Назад", callback_data=GAMES_MENU)]
        ]),
        parse_mode=ParseMode.HTML
    )
    await callback.answer()

@dp.callback_query(F.data == f"{SLOTS_PREFIX}bet_menu")
async def slots_bet_menu_handler(callback: types.CallbackQuery):
    await show_bet_selection(callback.message, game_type="slots")
    await callback.answer()

@dp.callback_query(F.data.startswith(f"{SLOTS_PREFIX}bet_"))
async def slots_bet_handler(callback: types.CallbackQuery):
    try:
        user_id = callback.from_user.id
        bet = int(callback.data.split("_")[2])
        
        # Balance check
        if users[user_id]["balance"] < bet:
            await callback.answer("❌ Недостаточно средств!", show_alert=True)
            return
        
        # Start game
        game = SlotsGame(user_id, bet, bot)
        await game.start_game(callback.message)
        
    except Exception as e:
        logging.error(f"Slots bet error: {e}")
        await callback.answer("❌ Ошибка при старте игры!")

@dp.callback_query(F.data == f"{LOTTERY_PREFIX}menu")
async def lottery_menu_handler(callback: types.CallbackQuery):
    global lottery_pool, lottery_end_time
    
    # Format time remaining
    time_remaining = "Неизвестно"
    if lottery_end_time:
        seconds_remaining = max(0, (lottery_end_time - datetime.now()).total_seconds())
        time_remaining = format_time(int(seconds_remaining))
    
    await callback.message.edit_text(
        "🎫 <b>Лотерея</b>\n\n"
        f"Текущий призовой фонд: {format_number(lottery_pool)}₽\n"
        f"Розыгрыш через: {time_remaining}\n\n"
        "Купите билет и выиграйте весь призовой фонд!",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🎫 Купить билет", callback_data=f"{LOTTERY_PREFIX}bet_menu")],
            [InlineKeyboardButton(text="🔙 Назад", callback_data=GAMES_MENU)]
        ]),
        parse_mode=ParseMode.HTML
    )
    await callback.answer()

@dp.callback_query(F.data == f"{LOTTERY_PREFIX}bet_menu")
async def lottery_bet_menu_handler(callback: types.CallbackQuery):
    await show_bet_selection(callback.message, game_type="lottery")
    await callback.answer()

@dp.callback_query(F.data.startswith(f"{LOTTERY_PREFIX}bet_"))
async def lottery_bet_handler(callback: types.CallbackQuery):
    try:
        global lottery_pool, lottery_participants
        
        user_id = callback.from_user.id
        bet = int(callback.data.split("_")[2])
        
        # Balance check
        if users[user_id]["balance"] < bet:
            await callback.answer("❌ Недостаточно средств!", show_alert=True)
            return
        
        # Check if already participating
        if user_id in lottery_participants:
            await callback.answer("❌ Вы уже участвуете в лотерее!", show_alert=True)
            return
        
        # Add to lottery
        users[user_id]["balance"] -= bet
        lottery_pool += bet
        lottery_participants.append(user_id)
        
        # Update stats for achievement tracking
        users[user_id]["games_played"] = users[user_id].get("games_played", 0) + 1
        
        save_db()
        
        await callback.message.edit_text(
            "🎫 <b>Лотерея</b>\n\n"
            f"✅ Вы купили билет за {bet}₽!\n"
            f"Текущий призовой фонд: {format_number(lottery_pool)}₽\n\n"
            "Результаты будут объявлены автоматически.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🔙 В меню игр", callback_data=GAMES_MENU)]
            ]),
            parse_mode=ParseMode.HTML
        )
        
    except Exception as e:
        logging.error(f"Lottery bet error: {e}")
        await callback.answer("❌ Ошибка при покупке билета!")

@dp.callback_query(F.data == f"{RACE_PREFIX}menu")
async def race_menu_handler(callback: types.CallbackQuery):
    global race_active, race_end_time, race_prize_pool
    
    # Format time remaining
    time_remaining = "Неизвестно"
    if race_end_time:
        seconds_remaining = max(0, (race_end_time - datetime.now()).total_seconds())
        time_remaining = format_time(int(seconds_remaining))
    
    status = "🟢 Активны" if race_active else "🔴 Ожидание"
    
    await callback.message.edit_text(
        "🏇 <b>Скачки</b>\n\n"
        f"Статус: {status}\n"
        f"Призовой фонд: {format_number(race_prize_pool)}₽\n"
        f"Старт через: {time_remaining}\n\n"
        "Сделайте ставку на свою лошадь и выиграйте приз!\n"
        "1 место: 60% фонда\n"
        "2 место: 30% фонда\n"
        "3 место: 10% фонда",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🏇 Сделать ставку", callback_data=f"{RACE_PREFIX}bet_menu")],
            [InlineKeyboardButton(text="🔙 Назад", callback_data=GAMES_MENU)]
        ]),
        parse_mode=ParseMode.HTML
    )
    await callback.answer()

@dp.callback_query(F.data == f"{RACE_PREFIX}bet_menu")
async def race_bet_menu_handler(callback: types.CallbackQuery):
    if not race_active:
        await callback.answer("❌ Скачки сейчас не проводятся!", show_alert=True)
        return
    
    await show_bet_selection(callback.message, game_type="race")
    await callback.answer()

@dp.callback_query(F.data.startswith(f"{RACE_PREFIX}bet_"))
async def race_bet_handler(callback: types.CallbackQuery):
    try:
        global race_participants, race_prize_pool
        
        if not race_active:
            await callback.answer("❌ Скачки сейчас не проводятся!", show_alert=True)
            return
        
        user_id = callback.from_user.id
        bet = int(callback.data.split("_")[2])
        
        # Balance check
        if users[user_id]["balance"] < bet:
            await callback.answer("❌ Недостаточно средств!", show_alert=True)
            return
        
        # Check if already participating
        if user_id in race_participants:
            await callback.answer("❌ Вы уже участвуете в скачках!", show_alert=True)
            return
        
        # Add to race
        users[user_id]["balance"] -= bet
        race_prize_pool += bet
        race_participants.append(user_id)
        
        # Update stats for achievement tracking
        users[user_id]["games_played"] = users[user_id].get("games_played", 0) + 1
        
        save_db()
        
        await callback.message.edit_text(
            "🏇 <b>Скачки</b>\n\n"
            f"✅ Вы сделали ставку {bet}₽!\n"
            f"Текущий призовой фонд: {format_number(race_prize_pool)}₽\n\n"
            "Результаты будут объявлены автоматически.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🔙 В меню игр", callback_data=GAMES_MENU)]
            ]),
            parse_mode=ParseMode.HTML
        )
        
    except Exception as e:
        logging.error(f"Race bet error: {e}")
        await callback.answer("❌ Ошибка при ставке на скачки!")

@dp.callback_query(F.data == "darts_menu")
async def darts_menu_handler(callback: types.CallbackQuery):
    await callback.message.edit_text(
        "🎯 <b>Дартс</b>\n\n"
        "Попадите в цель и выиграйте приз!\n"
        "Чем ближе к центру, тем больше выигрыш!",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🎯 Играть за 1,000₽", callback_data="darts_play")],
            [InlineKeyboardButton(text="🔙 Назад", callback_data=GAMES_MENU)]
        ]),
        parse_mode=ParseMode.HTML
    )
    await callback.answer()

@dp.callback_query(F.data == "darts_play")
async def darts_play_handler(callback: types.CallbackQuery):
    try:
        user_id = callback.from_user.id
        bet = 1000
        
        # Balance check
        if users[user_id]["balance"] < bet:
            await callback.answer("❌ Недостаточно средств!", show_alert=True)
            return
        
        # Deduct bet
        users[user_id]["balance"] -= bet
        
        # Generate dart throw
        # Distance from center (0-100, where 0 is bullseye)
        luck_level = users[user_id].get("upgrades", {}).get("luck", 0)
        luck_bonus = luck_level * 5  # 5% better aim per luck level
        
        distance = random.randint(0, 100 - luck_bonus)
        
        # Calculate winnings based on distance
        if distance < 10:  # Bullseye
            multiplier = 5.0
            result_text = "🎯 ЯБЛОЧКО! Идеальное попадание!"
        elif distance < 30:
            multiplier = 3.0
            result_text = "🎯 Отличное попадание!"
        elif distance < 50:
            multiplier = 2.0
            result_text = "🎯 Хорошее попадание!"
        elif distance < 70:
            multiplier = 1.0
            result_text = "🎯 Неплохое попадание!"
        else:
            multiplier = 0.0
            result_text = "🎯 Мимо! Попробуйте еще раз!"
        
        # Calculate winnings
        winnings = int(bet * multiplier)
        
        # Update balance
        if winnings > 0:
            users[user_id]["balance"] += winnings
            users[user_id]["total_income"] += winnings
            
            # Update stats for achievement tracking
            users[user_id]["games_won"] = users[user_id].get("games_won", 0) + 1
        
        users[user_id]["games_played"] = users[user_id].get("games_played", 0) + 1
        
        save_db()
        
        # Create darts board image
        img = Image.new('RGB', (500, 500), color=(255, 255, 255))
        draw = ImageDraw.Draw(img)
        
        # Draw target circles
        center = (250, 250)
        colors = [(255, 0, 0), (0, 0, 255), (255, 0, 0), (0, 0, 255), (255, 0, 0)]
        radii = [200, 160, 120, 80, 40]
        
        for i, (color, radius) in enumerate(zip(colors, radii)):
            draw.ellipse(
                (center[0] - radius, center[1] - radius, 
                 center[0] + radius, center[1] + radius), 
                fill=color
            )
        
        # Draw bullseye
        draw.ellipse((center[0] - 20, center[1] - 20, center[0] + 20, center[1] + 20), fill=(0, 0, 0))
        
        # Calculate dart position
        angle = random.uniform(0, 2 * math.pi)
        dart_x = center[0] + int(distance * math.cos(angle))
        dart_y = center[1] + int(distance * math.sin(angle))
        
        # Draw dart
        draw.ellipse((dart_x - 5, dart_y - 5, dart_x + 5, dart_y + 5), fill=(255, 255, 0))
        
        # Save image to buffer
        buffer = BytesIO()
        img.save(buffer, format='PNG')
        buffer.seek(0)
        
        # Send result with image
        await bot.send_photo(
            chat_id=user_id,
            photo=buffer,
            caption=f"{result_text}\n\n"
                   f"Ставка: {bet}₽\n"
                   f"Выигрыш: {winnings}₽ (x{multiplier})",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🎯 Играть снова", callback_data="darts_play")],
                [InlineKeyboardButton(text="🔙 В меню игр", callback_data=GAMES_MENU)]
            ])
        )
        
        # Delete original message
        await callback.message.delete()
        
    except Exception as e:
        logging.error(f"Darts game error: {e}")
        await callback.answer("❌ Ошибка при игре в дартс!")

@dp.message(Command('start'))
async def start_command(message: Message):
    user_id = message.from_user.id
    username = message.from_user.username or f"user{user_id}"
    
    # Get referrer_id from command parameters (if any)
    referrer_id = None
    if len(message.text.split()) > 1:
        try:
            referrer_id = int(message.text.split()[1])
            # Check it's not the user themselves and referrer exists
            if referrer_id == user_id or referrer_id not in users:
                referrer_id = None
        except (ValueError, IndexError):
            referrer_id = None

    if not await check_subscription(user_id):
        # Save referral immediately, even if user hasn't subscribed yet
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
        # Create new user with referral
        users[user_id] = {
            "balance": 1000,  # Increased starting balance
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
            "shield_active": None,  # Shield end time
            "shackles": {},  # {slave_id: end_time}
            "shop_purchases": 0,  # Purchase counter
            "last_passive": datetime.now(),
            "income_per_sec": 0.0167,
            "referrer": referrer_id,  # Save referrer
            "referrals": [],  # List of referrals
            "achievements": {},  # Achievement progress
            "last_daily": None,  # Last daily bonus claim
            "daily_streak": 0,  # Daily bonus streak
            "clan_id": None,  # Clan ID
            "clan_role": None,  # Role in clan
            "clan_joined": None,  # When joined clan
            "games_played": 0,  # Games played
            "games_won": 0,  # Games won
            "work_count": 0,  # Work count
        }
        
        # Award bonus to referrer
        if referrer_id and referrer_id in users:
            referrer = users[referrer_id]
            if "referrals" not in referrer:
                referrer["referrals"] = []
            
            if user_id not in referrer["referrals"]:
                referrer["referrals"].append(user_id)
                bonus = 500  # Increased referral bonus
                referrer["balance"] += bonus
                referrer["total_income"] += bonus
                
                # Check achievement progress
                check_achievement_progress(referrer_id)
                
                try:
                    await bot.send_message(
                        referrer_id,
                        f"🎉 Вам начислено {bonus}₽ за приглашение @{username}!"
                    )
                except Exception:
                    pass  # If message couldn't be sent
        
        welcome_msg = (
            "👑 <b>ДОБРО ПОЖАЛОВАТЬ В РАБОВЛАДЕЛЬЧЕСКУЮ ИМПЕРИЮ!</b>\n\n"
            "⚡️ <b>Основные возможности:</b>\n"
            "▸ 💼 Бонусная работа (раз в 30 мин)\n"
            "▸ 🛠 Улучшай свои владения\n"
            "▸ 👥 Покупай рабов для пассивного дохода\n"
            "▸ 🎮 Играй в мини-игры и выигрывай\n"
            "▸ 🎁 Получай ежедневные бонусы\n"
            "▸ 🏆 Выполняй достижения\n"
            "▸ 📈 Получай доход каждую минуту\n\n"
        )
        
        if referrer_id:
            referrer_name = users.get(referrer_id, {}).get("username", "друг")
            welcome_msg += f"🤝 Вас пригласил: @{referrer_name}\n\n"
        
        welcome_msg += "💰 <b>Стартовый баланс:</b> 1,000₽\n"
        welcome_msg += "💰 <b>Базовая пассивка:</b> 1₽/мин"
        
        save_db()
        await message.answer(welcome_msg, reply_markup=main_keyboard(), parse_mode=ParseMode.HTML)
    else:
        await message.answer("🔮 Главное меню:", reply_markup=main_keyboard())

@dp.message(F.text)
async def handle_text_message(message: Message):
    try:
        user_id = message.from_user.id
        if user_id not in users:
            return

        # Check if we're expecting a bet from this user
        if user_id in user_search_cache['awaiting_bet']:
            # Remove from waiting immediately after receiving message
            user_search_cache['awaiting_bet'].discard(user_id)

            # Input validation
            if not message.text.strip().isdigit():
                await message.reply("❌ Введите только цифры (например: 1000)")
                return

            bet = int(message.text)
            
            MIN_BET = 100
            MAX_BET = 100000  # Maximum bet limit

            if not (MIN_BET <= bet <= MAX_BET):
                await message.reply(
                    f"❌ Ставка должна быть от {MIN_BET}₽ до {MAX_BET}₽",
                    reply_markup=main_keyboard()
                )
                return

            # Balance check
            if users[user_id]["balance"] < bet:
                await message.reply(
                    f"❌ Недостаточно средств! Ваш баланс: {users[user_id]['balance']}₽",
                    reply_markup=main_keyboard()
                )
                return

            # Clear previous game
            if user_id in active_games:
                del active_games[user_id]

            # Create new game
            game = BlackjackGame(user_id, bet, bot)
            active_games[user_id] = game
            await game.start_game(await message.answer("Начинаем игру..."))
            
        # Check if we're expecting a dice bet
        elif user_id in user_search_cache['awaiting_dice_bet']:
            user_search_cache['awaiting_dice_bet'].discard(user_id)
            
            # Input validation
            if not message.text.strip().isdigit():
                await message.reply("❌ Введите только цифры (например: 1000)")
                return

            bet = int(message.text)
            
            MIN_BET = 100
            MAX_BET = 100000  # Maximum bet limit

            if not (MIN_BET <= bet <= MAX_BET):
                await message.reply(
                    f"❌ Ставка должна быть от {MIN_BET}₽ до {MAX_BET}₽",
                    reply_markup=main_keyboard()
                )
                return

            # Balance check
            if users[user_id]["balance"] < bet:
                await message.reply(
                    f"❌ Недостаточно средств! Ваш баланс: {users[user_id]['balance']}₽",
                    reply_markup=main_keyboard()
                )
                return
                
            # Start dice game
            game = DiceGame(user_id, bet, bot)
            await game.start_game(await message.answer("Бросаем кости..."))
            
        # Check if we're expecting a slots bet
        elif user_id in user_search_cache['awaiting_slots_bet']:
            user_search_cache['awaiting_slots_bet'].discard(user_id)
            
            # Input validation
            if not message.text.strip().isdigit():
                await message.reply("❌ Введите только цифры (например: 1000)")
                return

            bet = int(message.text)
            
            MIN_BET = 100
            MAX_BET = 100000  # Maximum bet limit

            if not (MIN_BET <= bet <= MAX_BET):
                await message.reply(
                    f"❌ Ставка должна быть от {MIN_BET}₽ до {MAX_BET}₽",
                    reply_markup=main_keyboard()
                )
                return

            # Balance check
            if users[user_id]["balance"] < bet:
                await message.reply(
                    f"❌ Недостаточно средств! Ваш баланс: {users[user_id]['balance']}₽",
                    reply_markup=main_keyboard()
                )
                return
                
            # Start slots game
            game = SlotsGame(user_id, bet, bot)
            await game.start_game(await message.answer("Крутим барабаны..."))
            
        # Check if we're expecting a username search
        elif user_id in user_search_cache['awaiting_username']:
            user_search_cache['awaiting_username'].discard(user_id)
            await process_username(message)

    except Exception as e:
        logging.error(f"Text message handling error: {e}", exc_info=True)
        await message.answer("❌ Ошибка обработки сообщения", reply_markup=main_keyboard())

async def process_username(message: Message):
    try:
        # Normalize username (remove @ and extra spaces)
        username = message.text.strip().lower().replace('@', '')
        
        # Find user
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
        
        # Check protection shield
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
        
        # Calculate hourly income
        hourly_income = int(100 * (1 + 0.3 * slave.get('slave_level', 0)))
        
        # Apply training bonus if buying
        training_level = users[buyer_id].get("upgrades", {}).get("training", 0)
        if training_level > 0:
            bonus_income = int(hourly_income * training_level * 0.15)
            total_income = hourly_income + bonus_income
            income_text = f"{hourly_income}₽ + {bonus_income}₽ (бонус) = {total_income}₽"
        else:
            income_text = f"{hourly_income}₽"
        
        await message.reply(
            f"🔎 <b>Найден раб:</b>\n"
            f"▸ Ник: @{slave['username']}\n"
            f"▸ Уровень: {slave.get('slave_level', 0)}\n"
            f"▸ Цена: {slave['price']}₽\n"
            f"▸ Владелец: {owner_info}\n\n"
            f"💡 <i>Доход от этого раба: {income_text} в час</i>",
            reply_markup=kb,
            parse_mode=ParseMode.HTML
        )

    except Exception as e:
        logging.error(f"Search error: {e}")
        await message.reply("⚠️ Произошла ошибка при поиске")

@dp.callback_query(F.data == SEARCH_USER)
async def search_user_handler(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    user_search_cache['awaiting_username'].add(user_id)
    
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

@dp.callback_query(F.data.startswith(CHECK_SUB))
async def check_sub_handler(callback: types.CallbackQuery):
    user_id = int(callback.data.replace(CHECK_SUB, ""))
    
    if user_id != callback.from_user.id:
        await callback.answer("❌ Неверный ID пользователя", show_alert=True)
        return
        
    if not await check_subscription(user_id):
        await callback.answer("❌ Вы не подписались на канал!", show_alert=True)
        return
        
    # If user is already registered, just show main menu
    if user_id in users:
        await callback.message.edit_text("🔮 Главное меню:", reply_markup=main_keyboard())
        await callback.answer("✅ Подписка проверена!")
        return
        
    # Create new user
    username = callback.from_user.username or f"user{user_id}"
    referrer_id = users.get(user_id, {}).get("referrer")
    
    users[user_id] = {
        "balance": 1000,  # Increased starting balance
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
        "shield_active": None,
        "shackles": {},
        "shop_purchases": 0,
        "last_passive": datetime.now(),
        "income_per_sec": 0.0167,
        "referrer": referrer_id,
        "referrals": [],
        "achievements": {},
        "last_daily": None,
        "daily_streak": 0,
        "clan_id": None,
        "clan_role": None,
        "clan_joined": None,
        "games_played": 0,
        "games_won": 0,
        "work_count": 0,
    }
    
    # Award bonus to referrer
    if referrer_id and referrer_id in users:
        referrer = users[referrer_id]
        if "referrals" not in referrer:
            referrer["referrals"] = []
        
        if user_id not in referrer["referrals"]:
            referrer["referrals"].append(user_id)
            bonus = 500  # Increased referral bonus
            referrer["balance"] += bonus
            referrer["total_income"] += bonus
            
            # Check achievement progress
            check_achievement_progress(referrer_id)
            
            try:
                await bot.send_message(
                    referrer_id,
                    f"🎉 Вам начислено {bonus}₽ за приглашение @{username}!"
                )
            except Exception:
                pass
    
    welcome_msg = (
        "👑 <b>ДОБРО ПОЖАЛОВАТЬ В РАБОВЛАДЕЛЬЧЕСКУЮ ИМПЕРИЮ!</b>\n\n"
        "⚡️ <b>Основные возможности:</b>\n"
        "▸ 💼 Бонусная работа (раз в 30 мин)\n"
        "▸ 🛠 Улучшай свои владения\n"
        "▸ 👥 Покупай рабов для пассивного дохода\n"
        "▸ 🎮 Играй в мини-игры и выигрывай\n"
        "▸ 🎁 Получай ежедневные бонусы\n"
        "▸ 🏆 Выполняй достижения\n"
        "▸ 📈 Получай доход каждую минуту\n\n"
    )
    
    if referrer_id:
        referrer_name = users.get(referrer_id, {}).get("username", "друг")
        welcome_msg += f"🤝 Вас пригласил: @{referrer_name}\n\n"
    
    welcome_msg += "💰 <b>Стартовый баланс:</b> 1,000₽\n"
    welcome_msg += "💰 <b>Базовая пассивка:</b> 1₽/мин"
    
    save_db()
    await callback.message.edit_text(welcome_msg, reply_markup=main_keyboard(), parse_mode=ParseMode.HTML)
    await callback.answer("✅ Регистрация успешна!")

@dp.callback_query(F.data == WORK)
async def work_handler(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    user = users.get(user_id)
    
    if not user:
        await callback.answer("❌ Сначала зарегистрируйтесь!", show_alert=True)
        return

    now = datetime.now()
    
    # 🔧 Food upgrade - reduces cooldown
    food_level = user.get("upgrades", {}).get("food", 0)
    reduction = 1 - 0.1 * food_level  # 10% per level (increased from 8%)
    reduction = max(0.2, reduction)    # Minimum 20% of original cooldown
    
    cooldown = timedelta(minutes=30 * reduction)

    # ⏳ Cooldown check
    if user.get("last_work") and (now - user["last_work"]) < cooldown:
        remaining = (user["last_work"] + cooldown - now).seconds // 60
        await callback.answer(f"⏳ Подождите еще {remaining} минут", show_alert=True)
        return

    if user.get("work_count", 0) >= DAILY_WORK_LIMIT:
        await callback.answer("❌ Достигнут дневной лимит!")
        return

    user["work_count"] = user.get("work_count", 0) + 1

    # 📈 Calculate passive income per minute
    passive_per_min = 1 + user.get("upgrades", {}).get("storage", 0) * 10
    passive_per_min += sum(
        100 * (1 + 0.3 * slave_level(slave_id))
        for slave_id in user.get("slaves", [])
    ) / 60

    # 📦 Work bonus = 20 minutes of passive × whip multiplier
    whip_bonus = 1 + user.get("upgrades", {}).get("whip", 0) * 0.2  # Increased from 0.18
    work_bonus = int(passive_per_min * 20 * whip_bonus)

    # Add random bonus (10-30% extra)
    random_bonus = int(work_bonus * random.uniform(0.1, 0.3))
    total_bonus = work_bonus + random_bonus

    user["balance"] += total_bonus
    user["total_income"] += total_bonus
    user["last_work"] = now

    # Check achievement progress
    check_achievement_progress(user_id)

    await callback.message.edit_text(
        f"💼 <b>Бонусная работа</b>\n\n"
        f"💰 Основной доход: {work_bonus}₽\n"
        f"✨ Случайный бонус: +{random_bonus}₽\n"
        f"🎉 Итого получено: {total_bonus}₽\n\n"
        f"📊 <b>Статистика:</b>\n"
        f"▸ Пассивный доход: {passive_per_min:.1f}₽/мин\n"
        f"▸ Бонус от кнутов: +{int(whip_bonus*100-100)}%\n"
        f"▸ Кулдаун: {int(cooldown.total_seconds() // 60)} минут\n"
        f"▸ Работ сегодня: {user['work_count']}/{DAILY_WORK_LIMIT}",
        reply_markup=main_keyboard(),
        parse_mode=ParseMode.HTML
    )
    await callback.answer()

@dp.callback_query(F.data == UPGRADES)
async def upgrades_handler(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    if user_id not in users:
        await callback.answer("❌ Сначала зарегистрируйтесь!", show_alert=True)
        return
    
    await callback.message.edit_text(
        "🛠 <b>Улучшения</b>\n\n"
        "Выберите улучшение для покупки:",
        reply_markup=upgrades_keyboard(user_id),
        parse_mode=ParseMode.HTML
    )
    await callback.answer()

@dp.callback_query(F.data == REF_LINK)
async def ref_link_handler(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    ref_link = f"https://t.me/{(await bot.get_me()).username}?start={user_id}"
    
    # Count referrals and their income
    referrals = users[user_id].get("referrals", [])
    ref_count = len(referrals)
    
    # Create referral stats chart
    if ref_count > 0:
        # Get referral data
        ref_data = []
        for ref_id in referrals:
            if ref_id in users:
                ref_user = users[ref_id]
                ref_data.append({
                    "username": ref_user.get("username", f"user{ref_id}"),
                    "balance": ref_user.get("balance", 0),
                    "slaves": len(ref_user.get("slaves", [])),
                    "level": sum(level for level in ref_user.get("upgrades", {}).values())
                })
        
        # Sort by balance
        ref_data.sort(key=lambda x: x["balance"], reverse=True)
        
        # Create chart
        plt.figure(figsize=(10, 6))
        
        # Bar chart of top 5 referrals by balance
        top_refs = ref_data[:5]
        usernames = [f"@{ref['username']}" for ref in top_refs]
        balances = [ref["balance"] for ref in top_refs]
        
        plt.bar(usernames, balances, color='skyblue')
        plt.title('Топ рефералов по балансу')
        plt.xlabel('Пользователь')
        plt.ylabel('Баланс (₽)')
        plt.xticks(rotation=45, ha='right')
        plt.tight_layout()
        
        # Save chart to buffer
        buffer = BytesIO()
        plt.savefig(buffer, format='png')
        buffer.seek(0)
        
        # Send chart
        await bot.send_photo(
            chat_id=user_id,
            photo=buffer,
            caption=f"📊 <b>Статистика ваших рефералов</b>\n\n"
                   f"Всего рефералов: {ref_count}\n"
                   f"Бонус за каждого: 500₽\n\n"
                   f"🔗 <b>Ваша реферальная ссылка:</b>\n<code>{ref_link}</code>\n\n"
                   f"<i>Приглашайте друзей и получайте бонусы!</i>",
            parse_mode=ParseMode.HTML
        )
        
        # Delete original message
        await callback.message.delete()
        
    else:
        await callback.message.edit_text(
            f"🔗 <b>Ваша реферальная ссылка:</b>\n<code>{ref_link}</code>\n\n"
            f"👥 Приглашено друзей: {ref_count}\n"
            f"💰 Бонус за каждого: 500₽\n\n"
            f"<i>Приглашайте друзей и получайте бонусы!</i>",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🔙 Назад", callback_data=MAIN_MENU)]
            ]),
            parse_mode=ParseMode.HTML
        )
    
    await callback.answer()

@dp.callback_query(F.data == BUY_MENU)
async def buy_menu_handler(callback: types.CallbackQuery):
    await callback.message.edit_text(
        "👥 <b>Меню покупки рабов</b>\n\n"
        "Выберите способ поиска рабов:",
        reply_markup=buy_menu_keyboard(),
        parse_mode=ParseMode.HTML
    )
    await callback.answer()

@dp.callback_query(F.data == MAIN_MENU)
async def main_menu_handler(callback: types.CallbackQuery):
    await callback.message.edit_text("🔮 Главное меню:", reply_markup=main_keyboard())
    await callback.answer()

@dp.callback_query(F.data == TOP_OWNERS)
async def top_owners_handler(callback: types.CallbackQuery):
    try:
        current_user_id = callback.from_user.id
        
        # Calculate efficiency for all users
        users_list = []
        for user_id, user_data in users.items():
            slaves_count = len(user_data.get("slaves", []))
            total_income = user_data.get("total_income", 0)
            efficiency = total_income / max(slaves_count, 1)  # Avoid division by zero
            users_list.append({
                "user_id": user_id,
                "username": user_data.get("username", "Unknown"),
                "slaves": slaves_count,
                "total_income": total_income,
                "efficiency": efficiency,
                "balance": user_data.get("balance", 0)
            })

        # Sort by efficiency (descending)
        sorted_users = sorted(
            users_list,
            key=lambda x: x["efficiency"],
            reverse=True
        )

        # Get top 10
        top_10 = sorted_users[:10]
        
        # Find current user's position
        user_position = None
        for idx, user in enumerate(sorted_users, 1):
            if user["user_id"] == current_user_id:
                user_position = idx
                break

        # Create chart of top 5 users
        plt.figure(figsize=(10, 6))
        
        # Bar chart of top 5 users by efficiency
        top_5 = top_10[:5]
        usernames = [f"@{user['username']}" for user in top_5]
        efficiencies = [user["efficiency"] for user in top_5]
        
        plt.bar(usernames, efficiencies, color='gold')
        plt.title('Топ 5 рабовладельцев по эффективности')
        plt.xlabel('Пользователь')
        plt.ylabel('Эффективность (₽/раб)')
        plt.xticks(rotation=45, ha='right')
        plt.tight_layout()
        
        # Save chart to buffer
        buffer = BytesIO()
        plt.savefig(buffer, format='png')
        buffer.seek(0)
        
        # Format text
        text = "🏆 <b>Топ рабовладельцев по эффективности:</b>\n\n"
        text += "<i>Рейтинг рассчитывается как доход на одного раба</i>\n\n"
        
        # Show top 10
        for idx, user in enumerate(top_10, 1):
            if user["efficiency"] > 0:
                text += (
                    f"{idx}. @{user['username']}\n"
                    f"   ▸ Эффективность: {user['efficiency']:.1f}₽/раб\n"
                    f"   ▸ Рабов: {user['slaves']} | Доход: {format_number(int(user['total_income']))}₽\n\n"
                )

        # Add user's position
        if user_position:
            if user_position <= 10:
                text += f"\n🎉 Ваша позиция в топе: {user_position}"
            else:
                user_efficiency = next((u["efficiency"] for u in sorted_users if u["user_id"] == current_user_id), 0)
                text += f"\n📊 Ваша эффективность: {user_efficiency:.1f}₽/раб (позиция #{user_position})"
        else:
            text += "\nℹ️ Вы пока не участвуете в рейтинге"
        
        # Send chart with text
        await bot.send_photo(
            chat_id=current_user_id,
            photo=buffer,
            caption=text,
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[
                    [InlineKeyboardButton(text="🔙 Назад", callback_data=MAIN_MENU)]
                ]
            ),
            parse_mode=ParseMode.HTML
        )
        
        # Delete original message
        await callback.message.delete()

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

    # Calculate shield price
    shield_price = calculate_shield_price(user_id)
    
    # Handle shield_active with type checking
    shield_active = user.get("shield_active")
    if isinstance(shield_active, str):
        try:
            shield_active = datetime.fromisoformat(shield_active)
            user["shield_active"] = shield_active  # Update value in dictionary
        except (ValueError, TypeError):
            shield_active = None
    
    # Check shield activity
    shield_status = "🟢 Активен" if shield_active and shield_active > datetime.now() else "🔴 Неактивен"
    
    # Format shield time remaining
    shield_time = "—"
    if shield_active and shield_active > datetime.now():
        seconds_remaining = (shield_active - datetime.now()).total_seconds()
        shield_time = format_time(int(seconds_remaining))
    
    text = [
        "🛒 <b>Магический рынок</b>\n",
        f"💰 Ваш баланс: {format_number(int(user['balance']))}₽\n",
        f"🛡 <b>Щит свободы</b> {shield_status}",
        f"▸ Защита от порабощения на 12ч",
        f"▸ Осталось: {shield_time}",
        f"▸ Цена: {format_number(shield_price)}₽",
        "",
        "⛓ <b>Квантовые кандалы</b>",
        "▸ Увеличивают время выкупа раба на 24ч",
        "▸ Цена зависит от раба",
        "",
        "🧪 <b>Зелье опыта</b>",
        "▸ +1 уровень рабу",
        "▸ Цена: 5,000₽",
        "",
        "💎 <b>Кристалл удачи</b>",
        "▸ +5% к выигрышам в играх на 24ч",
        "▸ Цена: 10,000₽"
    ]
    
    buttons = [
        [InlineKeyboardButton(
            text=f"🛒 Купить щит - {format_number(shield_price)}₽",
            callback_data=f"{SHIELD_PREFIX}{shield_price}"
        )],
        [InlineKeyboardButton(
            text="⛓ Выбрать раба для кандал",
            callback_data="select_shackles"
        )],
        [InlineKeyboardButton(
            text="🧪 Купить зелье опыта - 5,000₽",
            callback_data="buy_exp_potion"
        )],
        [InlineKeyboardButton(
            text="💎 Купить кристалл удачи - 10,000₽",
            callback_data="buy_luck_crystal"
        )],
        [InlineKeyboardButton(text="🔙 Назад", callback_data=MAIN_MENU)]
    ]
    
    await callback.message.edit_text(
        "\n".join(text),
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
        parse_mode=ParseMode.HTML
    )
    await callback.answer()

@dp.callback_query(F.data == "buy_exp_potion")
async def buy_exp_potion_handler(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    user = users.get(user_id)
    
    if not user:
        await callback.answer("❌ Сначала зарегистрируйтесь!", show_alert=True)
        return
    
    # Check balance
    if user.get("balance", 0) < 5000:
        await callback.answer("❌ Недостаточно средств!", show_alert=True)
        return
    
    # Check if user has slaves
    if not user.get("slaves"):
        await callback.answer("❌ У вас нет рабов для улучшения!", show_alert=True)
        return
    
    # Deduct cost
    user["balance"] -= 5000
    
    # Add potion to inventory
    user["exp_potions"] = user.get("exp_potions", 0) + 1
    
    save_db()
    
    # Show slave selection for potion use
    buttons = []
    for slave_id in user["slaves"][:5]:  # Maximum 5 first slaves
        slave = users.get(slave_id, {})
        buttons.append([
            InlineKeyboardButton(
                text=f"🧪 @{slave.get('username', 'unknown')} (Ур. {slave.get('slave_level', 0)})",
                callback_data=f"use_potion_{slave_id}"
            )
        ])
    
    buttons.append([InlineKeyboardButton(text="🔙 Назад", callback_data="shop")])
    
    await callback.message.edit_text(
        "🧪 <b>Зелье опыта</b>\n\n"
        f"✅ Вы купили зелье опыта за 5,000₽!\n"
        f"У вас {user.get('exp_potions', 0)} зелий.\n\n"
        "Выберите раба для применения зелья:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
        parse_mode=ParseMode.HTML
    )
    await callback.answer()

@dp.callback_query(F.data.startswith("use_potion_"))
async def use_potion_handler(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    user = users.get(user_id)
    slave_id = int(callback.data.split("_")[2])
    
    if not user:
        await callback.answer("❌ Сначала зарегистрируйтесь!", show_alert=True)
        return
    
    # Check if user has potions
    if user.get("exp_potions", 0) <= 0:
        await callback.answer("❌ У вас нет зелий опыта!", show_alert=True)
        return
    
    # Check if slave belongs to user
    if slave_id not in user.get("slaves", []):
        await callback.answer("❌ Этот раб вам не принадлежит!", show_alert=True)
        return
    
    # Get slave
    slave = users.get(slave_id)
    if not slave:
        await callback.answer("❌ Раб не найден!", show_alert=True)
        return
    
    # Check if slave is at max level
    if slave.get("slave_level", 0) >= MAX_SLAVE_LEVEL:
        await callback.answer(f"❌ Раб уже достиг максимального уровня ({MAX_SLAVE_LEVEL})!", show_alert=True)
        return
    
    # Use potion
    user["exp_potions"] -= 1
    old_level = slave.get("slave_level", 0)
    slave["slave_level"] = min(old_level + 1, MAX_SLAVE_LEVEL)
    
    # Update slave price
    slave["price"] = slave_price(slave)
    
    save_db()
    
    # Notify slave
    try:
        await bot.send_message(
            slave_id,
            f"🧪 Ваш владелец @{user.get('username', 'unknown')} использовал зелье опыта!\n"
            f"Ваш уровень повышен с {old_level} до {slave['slave_level']}."
        )
    except Exception:
        pass
    
    await callback.message.edit_text(
        "🧪 <b>Зелье опыта</b>\n\n"
        f"✅ Вы использовали зелье опыта на @{slave.get('username', 'unknown')}!\n"
        f"Уровень раба повышен с {old_level} до {slave['slave_level']}.\n"
        f"Новая цена раба: {slave['price']}₽\n"
        f"Новый доход: {int(100 * (1 + 0.3 * slave['slave_level']))}₽/час\n\n"
        f"У вас осталось {user.get('exp_potions', 0)} зелий.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔙 В магазин", callback_data="shop")]
        ]),
        parse_mode=ParseMode.HTML
    )
    await callback.answer()

@dp.callback_query(F.data == "buy_luck_crystal")
async def buy_luck_crystal_handler(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    user = users.get(user_id)
    
    if not user:
        await callback.answer("❌ Сначала зарегистрируйтесь!", show_alert=True)
        return
    
    # Check balance
    if user.get("balance", 0) < 10000:
        await callback.answer("❌ Недостаточно средств!", show_alert=True)
        return
    
    # Check if already has active crystal
    luck_boost_until = user.get("luck_boost_until")
    if luck_boost_until and isinstance(luck_boost_until, str):
        try:
            luck_boost_until = datetime.fromisoformat(luck_boost_until)
            user["luck_boost_until"] = luck_boost_until
        except (ValueError, TypeError):
            luck_boost_until = None
    
    if luck_boost_until and luck_boost_until > datetime.now():
        await callback.answer("❌ У вас уже активен кристалл удачи!", show_alert=True)
        return
    
    # Deduct cost
    user["balance"] -= 10000
    
    # Apply luck boost
    user["luck_boost_until"] = datetime.now() + timedelta(hours=24)
    
    save_db()
    
    await callback.message.edit_text(
        "💎 <b>Кристалл удачи</b>\n\n"
        f"✅ Вы купили кристалл удачи за 10,000₽!\n"
        f"Бонус +5% к выигрышам в играх активен на 24 часа.\n\n"
        f"Удачи в играх!",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔙 В магазин", callback_data="shop")]
        ]),
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
        max_level = upgrade_data.get("max_level", 10)
        
        if current_level >= max_level:
            await callback.answer(f"❌ Достигнут максимальный уровень ({max_level})!", show_alert=True)
            return
        
        price = int(upgrade_data["base_price"] * (upgrade_data["price_multiplier"] ** current_level))
        
        if user.get("balance", 0) < price:
            await callback.answer("❌ Недостаточно средств!", show_alert=True)
            return

        # Apply upgrade
        user["balance"] -= price
        user.setdefault("upgrades", {})[upgrade_id] = current_level + 1
        
        # Update passive income for storage
        if upgrade_id == "storage":
            user["income_per_sec"] = (1 + user["upgrades"].get("storage", 0) * 10) / 60

        # Track total upgrades for achievements
        user["total_upgrades"] = user.get("total_upgrades", 0) + 1
        
        # Check achievement progress
        check_achievement_progress(user_id)
        
        # Save changes to DB
        save_db()

        # Update keyboard
        try:
            await callback.message.edit_reply_markup(
                reply_markup=upgrades_keyboard(user_id)
            )
            await callback.answer(f"✅ {upgrade_data['name']} улучшен до уровня {current_level + 1}!")
        except Exception as e:
            logging.error(f"Keyboard update error: {str(e)}")
            await callback.answer("✅ Улучшение применено!", show_alert=True)

    except Exception as e:
        logging.error(f"Upgrade handler error: {str(e)}", exc_info=True)
        await callback.answer("❌ Произошла ошибка при улучшении", show_alert=True)

@dp.callback_query(F.data.startswith(SHIELD_PREFIX))
async def buy_shield(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    user = users.get(user_id)
    price = int(callback.data.replace(SHIELD_PREFIX, ""))
    
    # Check data type
    current_shield = user.get("shield_active")
    if isinstance(current_shield, str):
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
                await callback.answer()
                return

        await show_bet_selection(callback.message)
        await callback.answer() 

    except Exception as e:
        logging.error(f"Game menu error: {e}")
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
        
        # Remove old games
        if user_id in active_games:
            del active_games[user_id]

        # Balance check
        if users[user_id]["balance"] < bet:
            await callback.answer("❌ Недостаточно средств!", show_alert=True)
            return

        # Create new game
        game = BlackjackGame(
            user_id=user_id,
            bet=bet,
            bot=bot
        )
        active_games[user_id] = game
        
        # Update stats for achievement tracking
        users[user_id]["games_played"] = users[user_id].get("games_played", 0) + 1
        
        # Start game
        await game.start_game(callback.message)
        
    except Exception as e:
        logging.error(f"Game creation error: {e}")
        await callback.answer("❌ Ошибка при старте игры!")

@dp.callback_query(F.data == "select_shackles")
async def select_shackles(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    user = users.get(user_id)
    
    if not user or not user.get("slaves"):
        await callback.answer("❌ У вас нет рабов!", show_alert=True)
        return
    
    buttons = []
    for slave_id in user["slaves"][:5]:  # Maximum 5 first slaves
        slave = users.get(slave_id, {})
        price = calculate_shackles_price(slave_id)
        buttons.append([
            InlineKeyboardButton(
                text=f"⛓ @{slave.get('username', 'unknown')} - {format_number(price)}₽",
                callback_data=f"{SHACKLES_PREFIX}{slave_id}_{price}"
            )
        ])
    
    buttons.append([InlineKeyboardButton(text="🔙 Назад", callback_data="shop")])
    
    await callback.message.edit_text(
        "⛓ <b>Квантовые кандалы</b>\n\n"
        "Выберите раба для применения кандал:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
        parse_mode=ParseMode.HTML
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
    
    # Apply shackles
    user["balance"] -= price
    if "shackles" not in user:
        user["shackles"] = {}
    user["shackles"][slave_id] = datetime.now() + timedelta(hours=24)
    save_db()
    
    await callback.answer(
        f"⛓ Кандалы применены к @{users[slave_id].get('username', 'unknown')} на 24ч!",
        show_alert=True
    )
    await select_shackles(callback)  # Return to selection

@dp.callback_query(F.data.startswith(SLAVE_PREFIX))
async def buy_slave_handler(callback: types.CallbackQuery):
    try:
        buyer_id = callback.from_user.id
        slave_id = int(callback.data.replace(SLAVE_PREFIX, ""))
        
        # 1. Check user existence
        if buyer_id not in users:
            await callback.answer("❌ Вы не зарегистрированы!", show_alert=True)
            return
            
        if slave_id not in users:
            await callback.answer("❌ Раб не найден в системе", show_alert=True)
            return

        buyer = users[buyer_id]
        slave = users[slave_id]

        # 2. Check slave limit
        barracks_level = buyer.get("upgrades", {}).get("barracks", 0)
        slave_limit = 5 + 2 * barracks_level
        if len(buyer.get("slaves", [])) >= slave_limit:
            await callback.answer(
                f"❌ Лимит рабов ({slave_limit}). Улучшите бараки!",
                show_alert=True
            )
            return

        # 3. Check protection shield (with improved handling)
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
                logging.error(f"Shield processing error: {e}")

        # 4. Check repurchase time
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
                logging.error(f"last_purchased processing error: {e}")

        # 5. Check for buying oneself
        if slave_id == buyer_id:
            await callback.answer("❌ Нельзя купить самого себя!", show_alert=True)
            return

        # 6. Check hierarchy
        if buyer.get("owner") == slave_id:
            await callback.answer("❌ Нельзя купить своего владельца!", show_alert=True)
            return

        # 7. Check double ownership
        if slave.get("owner") == buyer_id:
            await callback.answer("❌ Этот раб уже принадлежит вам!", show_alert=True)
            return

        # 8. Check current owner
        owner_id = slave.get("owner")
        if owner_id:
            shackles = users.get(owner_id, {}).get("shackles", {})
            if slave_id in shackles:
                until = shackles[slave_id]
                if isinstance(until, str):
                    until = datetime.fromisoformat(until)
                if until > datetime.now():
                    await callback.answer(
                        f"⛓ Раб в кандалах до {until.strftime('%d.%m %H:%M')}, покупка невозможна!",
                        show_alert=True
                    )
                    return

        # 9. Calculate price
        try:
            price = slave_price(slave)
        except Exception as e:
            logging.error(f"Price calculation error: {e}")
            await callback.answer("❌ Ошибка расчета цены", show_alert=True)
            return

        # 10. Check balance
        if buyer.get("balance", 0) < price:
            await callback.answer(
                f"❌ Недостаточно средств! Нужно {price}₽",
                show_alert=True
            )
            return

        # 11. Purchase process
        try:
            # If there was a previous owner
            previous_owner_id = None
            if owner_id and owner_id in users:
                previous_owner_id = owner_id
                previous_owner = users[owner_id]
                if slave_id in previous_owner.get("slaves", []):
                    previous_owner["slaves"].remove(slave_id)
                commission = int(price * 0.3)
                previous_owner["balance"] += commission
                previous_owner["total_income"] += commission

            # Update buyer data
            buyer["balance"] -= price
            buyer.setdefault("slaves", []).append(slave_id)

            # Update slave data
            slave["owner"] = buyer_id
            slave["slave_level"] = min(slave.get("slave_level", 0) + 1, MAX_SLAVE_LEVEL)
            slave["price"] = slave_price(slave)
            slave["enslaved_date"] = datetime.now()
            slave["last_purchased"] = datetime.now()

            # Check achievement progress
            check_achievement_progress(buyer_id)

            # Format success message
            message_text = [
                f"✅ Куплен @{slave.get('username', 'unknown')} за {price}₽",
                f"▸ Уровень: {slave['slave_level']}",
                f"▸ Новая цена: {slave['price']}₽",
                f"▸ Доход/час: {int(100 * (1 + 0.3 * slave['slave_level']))}₽"
            ]
            
            if previous_owner_id:
                message_text.append(f"▸ Комиссия предыдущему владельцу: {int(price * 0.3)}₽")

            # Notify slave
            try:
                await bot.send_message(
                    slave_id,
                    f"⚡ Вы приобретены @{buyer.get('username', 'unknown')} "
                    f"за {price}₽ (уровень {slave['slave_level']})"
                )
            except Exception as e:
                logging.error(f"Failed to notify slave: {e}")

            save_db()
            await callback.message.edit_text(
                "\n".join(message_text),
                reply_markup=main_keyboard()
            )
            await callback.answer()

        except Exception as e:
            logging.error(f"Critical purchase error: {e}", exc_info=True)
            await callback.answer("❌ Критическая ошибка при покупке", show_alert=True)

    except Exception as e:
        logging.error(f"Purchase handler error: {e}", exc_info=True)
        await callback.answer("⚠️ Произошла непредвиденная ошибка", show_alert=True)

@dp.callback_query(F.data == "bj_cancel_bet")
async def cancel_bet_handler(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    if user_id in user_search_cache['awaiting_bet']:
        user_search_cache['awaiting_bet'].discard(user_id)
    
    await show_bet_selection(callback.message)
    await callback.answer()

@dp.callback_query(F.data.startswith(BUYOUT_PREFIX))
async def buyout_handler(callback: types.CallbackQuery):
    try:
        user_id = callback.from_user.id
        user = users.get(user_id)
        
        # Basic checks
        if not user:
            await callback.answer("❌ Сначала зарегистрируйтесь!", show_alert=True)
            return
            
        if not user.get("owner"):
            await callback.answer("❌ Вы и так свободны!", show_alert=True)
            return

        # Check shackles
        owner = users.get(user["owner"], {})
        if owner.get("shackles", {}).get(user_id):
            shackles_end = owner["shackles"][user_id]
            if isinstance(shackles_end, str):
                shackles_end = datetime.fromisoformat(shackles_end)
            shackles_end_str = shackles_end.strftime("%d.%m %H:%M")
            await callback.answer(
                f"⛓ Вы в кандалах до {shackles_end_str}!\n"
                f"Выкуп временно невозможен",
                show_alert=True
            )
            return

        # Calculate buyout price
        base_price = user.get("base_price", 100)
        slave_level = user.get("slave_level", 0)
        
        # Formula: (base + 5% capital) * (1 + 0.3 per level)
        buyout_price = int(
            (base_price + user["balance"] * 0.05) * 
            (1 + slave_level * 0.3)
        )
        
        # Price limits
        buyout_price = max(100, min(20000, buyout_price))  # 100-20k
        
        # Balance check (with 1% buffer)
        if user["balance"] < buyout_price * 0.99:
            await callback.answer(
                f"❌ Не хватает {buyout_price - user['balance']:.0f}₽\n"
                f"Требуется: {buyout_price}₽",
                show_alert=True
            )
            return

        # Buyout process
        owner_id = user["owner"]
        user["balance"] -= buyout_price
        user["owner"] = None
        user["price"] = base_price  # Reset price
        
        # Owner gets 60% of buyout (instead of 50%)
        if owner_id in users:
            owner_income = int(buyout_price * 0.6)
            users[owner_id]["balance"] += owner_income
            users[owner_id]["total_income"] += owner_income
            
            # Remove from slave list
            if user_id in users[owner_id].get("slaves", []):
                users[owner_id]["slaves"].remove(user_id)
            
            # Notify owner
            try:
                await bot.send_message(
                    owner_id,
                    f"🔓 Раб @{user.get('username', 'unknown')} "
                    f"выкупился за {buyout_price}₽\n"
                    f"Ваш доход: {owner_income}₽"
                )
            except Exception:
                pass

        # Update statistics
        user["total_spent"] = user.get("total_spent", 0) + buyout_price
        user["buyout_count"] = user.get("buyout_count", 0) + 1
        
        # Check achievement progress
        check_achievement_progress(user_id)
        
        # Success message
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

@dp.callback_query(F.data == PROFILE)
async def profile_handler(callback: types.CallbackQuery):
    try:
        user_id = callback.from_user.id
        user = users.get(user_id)
        
        if not user:
            await callback.answer("❌ Сначала зарегистрируйтесь!", show_alert=True)
            return

        # Initialize missing fields
        user.setdefault("balance", 0)
        user.setdefault("slave_level", 0)
        user.setdefault("upgrades", {})

        # Calculate buyout price
        buyout_price = 0
        if user.get("owner"):
            try:
                base_price = user.get("base_price", 100)
                balance = float(user.get("balance", 0))
                slave_level = user.get("slave_level", 0)
                buyout_price = int((base_price + balance * 0.05) * (1 + slave_level * 0.3))
                buyout_price = max(100, min(20000, buyout_price))
            except Exception as e:
                logging.error(f"Buyout calculation error: {e}")
                buyout_price = "Ошибка"

        # Upgrade levels
        storage_level = user["upgrades"].get("storage", 0)
        barracks_level = user["upgrades"].get("barracks", 0)
        whip_level = user["upgrades"].get("whip", 0)
        food_level = user["upgrades"].get("food", 0)
        training_level = user["upgrades"].get("training", 0)
        security_level = user["upgrades"].get("security", 0)
        market_level = user["upgrades"].get("market", 0)
        luck_level = user["upgrades"].get("luck", 0)

        # Passive income
        passive_per_min = 1 + storage_level * 10
        slave_income = 0
        for slave_id in user.get("slaves", []):
            if slave_id in users:
                slave = users[slave_id]
                slave_level = slave.get("slave_level", 0)
                slave_income += 100 * (1 + 0.3 * slave_level)
        
        # Apply training bonus
        if training_level > 0:
            slave_income *= (1 + training_level * 0.15)
        
        passive_per_min += slave_income / 60

        # Format text
        text = [
            f"👑 <b>Профиль @{user.get('username', 'unknown')}</b>",
            f"▸ 💰 Баланс: {format_number(int(user['balance']))}₽",
            f"▸ 💸 Пассивка: {passive_per_min:.1f}₽/мин",
            f"▸ 👥 Ур.раба: {user['slave_level']}",
            f"▸ 🏆 Достижения: {sum(1 for _, (level, claimed, _) in user.get('achievements', {}).items() if claimed)} из {len(achievements)}",
            "",
            "<b>Улучшения:</b>",
            f"▸ 📦 Склад: ур.{storage_level}",
            f"▸ 🏠 Бараки: ур.{barracks_level} ({5 + 2 * barracks_level} рабов)",
            f"▸ ⛓ Кнуты: ур.{whip_level} (+{whip_level * 20}% к работе)",
            f"▸ 🍗 Еда: ур.{food_level} (-{food_level * 10}% к кулдауну)",
            f"▸ 🏋️ Тренировка: ур.{training_level} (+{training_level * 15}% к доходу от рабов)",
            f"▸ 🔒 Охрана: ур.{security_level} (-{security_level * 10}% шанс побега)",
            f"▸ 🏪 Рынок: ур.{market_level} (+{market_level * 5}% к продажам)",
            f"▸ 🍀 Удача: ур.{luck_level} (+{luck_level * 5}% удачи в играх)",
        ]

        # Owner information
        if user.get("owner"):
            owner_id = user["owner"]
            owner = users.get(owner_id)
            owner_name = f"@{owner['username']}" if owner else "неизвестный"
            text.append(
                f"\n⚠️ <b>Налог рабства:</b> 30% → {owner_name}\n"
                f"▸ Цена выкупа: {format_number(buyout_price)}₽"
            )
        else:
            text.append("\n🔗 Вы свободный человек")

        # Clan information
        if user.get("clan_id") and user["clan_id"] in clans:
            clan = clans[user["clan_id"]]
            text.append(
                f"\n🏰 <b>Клан:</b> {clan.get('name', 'Неизвестный')}\n"
                f"▸ Роль: {user.get('clan_role', 'Участник')}\n"
                f"▸ Бонус: +{clan.get('level', 0) * 5}% к доходу"
            )

        # Buyout keyboard
        keyboard = []
        if user.get("owner") and isinstance(buyout_price, int):
            keyboard.append([
                InlineKeyboardButton(
                    text=f"🆓 Выкупиться за {format_number(buyout_price)}₽",
                    callback_data=f"{BUYOUT_PREFIX}{buyout_price}"
                )
            ])
        
        # Stats button
        keyboard.append([
            InlineKeyboardButton(
                text="📊 Подробная статистика",
                callback_data="detailed_stats"
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
        logging.error(f"Profile error: {e}", exc_info=True)
        await callback.answer("❌ Ошибка загрузки. Попробуйте позже.", show_alert=True)

@dp.callback_query(F.data == "detailed_stats")
async def detailed_stats_handler(callback: types.CallbackQuery):
    try:
        user_id = callback.from_user.id
        user = users.get(user_id)
        
        if not user:
            await callback.answer("❌ Сначала зарегистрируйтесь!", show_alert=True)
            return
        
        # Create stats chart
        plt.figure(figsize=(10, 6))
        
        # Prepare data
        labels = ['Рабы', 'Работа', 'Игры', 'Другое']
        
        # Calculate income sources
        slave_income = sum(
            100 * (1 + 0.3 * slave_level(slave_id))
            for slave_id in user.get("slaves", [])
        )
        
        work_income = user.get("work_count", 0) * 100  # Approximate
        games_income = user.get("games_won", 0) * 500  # Approximate
        other_income = user.get("total_income", 0) - slave_income - work_income - games_income
        
        # Ensure no negative values
        other_income = max(0, other_income)
        
        sizes = [slave_income, work_income, games_income, other_income]
        
        # Create pie chart
        plt.pie(sizes, labels=labels, autopct='%1.1f%%', startangle=90)
        plt.axis('equal')
        plt.title('Источники дохода')
        
        # Save chart to buffer
        buffer = BytesIO()
        plt.savefig(buffer, format='png')
        buffer.seek(0)
        
        # Format detailed stats text
        text = [
            f"📊 <b>Подробная статистика @{user.get('username', 'unknown')}</b>",
            "",
            f"💰 <b>Финансы:</b>",
            f"▸ Текущий баланс: {format_number(int(user['balance']))}₽",
            f"▸ Всего заработано: {format_number(int(user.get('total_income', 0)))}₽",
            f"▸ Всего потрачено: {format_number(int(user.get('total_spent', 0)))}₽",
            "",
            f"👥 <b>Рабы:</b>",
            f"▸ Количество рабов: {len(user.get('slaves', []))}",
            f"▸ Доход от рабов: {format_number(int(slave_income))}₽/час",
            "",
            f"🎮 <b>Игры:</b>",
            f"▸ Всего игр: {user.get('games_played', 0)}",
            f"▸ Выиграно: {user.get('games_won', 0)}",
            f"▸ Процент побед: {user.get('games_won', 0) / max(1, user.get('games_played', 0)) * 100:.1f}%",
            "",
            f"💼 <b>Работа:</b>",
                        f"▸ Всего работ: {user.get('work_count', 0)}",
            f"▸ Заработано работой: {format_number(int(work_income))}₽",
            "",
            f"🔗 <b>Рефералы:</b>",
            f"▸ Приглашено друзей: {len(user.get('referrals', []))}",
            f"▸ Заработано с рефералов: {len(user.get('referrals', [])) * 500}₽",
        ]
        
        # Send chart with text
        await bot.send_photo(
            chat_id=current_user_id,
            photo=buffer,
            caption="\n".join(text),
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[
                    [InlineKeyboardButton(text="🔙 Назад", callback_data=PROFILE)]
                ]
            ),
            parse_mode=ParseMode.HTML
        )
        
        # Delete original message
        await callback.message.delete()
        
    except Exception as e:
        logging.error(f"Detailed stats error: {e}", exc_info=True)
        await callback.answer("❌ Ошибка загрузки статистики", show_alert=True)

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
            # Check basic conditions
            if not slave_data or slave_id == user_id:
                continue
                
            # Check that slave_data contains all needed fields
            if not all(key in slave_data for key in ['owner', 'username']):
                continue
                
            # Check owner
            if slave_data['owner'] == user_id:
                continue

            # Check shield
            shield = slave_data.get('shield_active')
            if shield:
                try:
                    if isinstance(shield, str):
                        shield = datetime.fromisoformat(shield)
                    if shield and shield > datetime.now():
                        continue
                except:
                    continue

            # Check repurchase time
            last_purchased = slave_data.get('last_purchased')
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
            
        # Sort and select top 10
        available.sort(
            key=lambda x: x[1].get('price', 100) * (1 + 0.5 * x[1].get('slave_level', 0)),
            reverse=True
        )
        selected = available[:10]

        # Create buttons
        buttons = []
        for slave_id, slave_data in selected:
            try:
                price = slave_data.get('price', 100)
                level = slave_data.get('slave_level', 0)
                income = int(100 * (1 + 0.3 * level))
                username = slave_data.get('username', 'unknown')[:20]
                
                buttons.append([InlineKeyboardButton(
                    text=f"👤 Ур.{level} @{username} | {format_number(price)}₽",
                    callback_data=f"{SLAVE_PREFIX}{slave_id}"
                )])
            except Exception as e:
                logging.error(f"Button creation error: {e}")
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
        logging.error(f"Error in random_slaves: {e}", exc_info=True)
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

        # Update last action time
        game.last_action_time = datetime.now()
        
        # Process actions
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
                # Check if user has enough balance
                if users[user_id]["balance"] < game.bet:
                    await callback.answer("❌ Недостаточно средств для удвоения!", show_alert=True)
                    return
                    
                # Important fix: Deduct the additional bet amount BEFORE doubling the bet variable
                users[user_id]["balance"] -= game.bet
                
                game.bet *= 2
                game.player_hand.append(game.deal_card())
                await game.dealer_turn()

        await callback.answer()

    except Exception as e:
        logging.error(f"Game error: {e}")
        await callback.answer("⚠️ Произошла ошибка, игра перезапущена!")
        if user_id in active_games:
            del active_games[user_id]

@dp.callback_query(F.data == DAILY_BONUS)
async def daily_bonus_handler(callback: types.CallbackQuery):
    try:
        user_id = callback.from_user.id
        user = users.get(user_id)
        
        if not user:
            await callback.answer("❌ Сначала зарегистрируйтесь!", show_alert=True)
            return
        
        now = datetime.now()
        last_daily = user.get("last_daily")
        
        # Convert string to datetime if needed
        if isinstance(last_daily, str):
            try:
                last_daily = datetime.fromisoformat(last_daily)
                user["last_daily"] = last_daily
            except (ValueError, TypeError):
                last_daily = None
        
        # Check if can claim
        if last_daily and (now - last_daily).total_seconds() < 86400:  # 24 hours
            next_claim = last_daily + timedelta(days=1)
            time_left = next_claim - now
            hours = time_left.seconds // 3600
            minutes = (time_left.seconds % 3600) // 60
            
            await callback.message.edit_text(
                f"🎁 <b>Ежедневный бонус</b>\n\n"
                f"⏳ Вы уже получили бонус сегодня!\n"
                f"Следующий бонус будет доступен через {hours}ч {minutes}м.\n\n"
                f"Текущая серия: {user.get('daily_streak', 0)} дней",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="🔙 Назад", callback_data=MAIN_MENU)]
                ]),
                parse_mode=ParseMode.HTML
            )
            await callback.answer()
            return
        
        # Check streak
        streak_broken = False
        if last_daily and (now - last_daily).total_seconds() > 172800:  # 48 hours
            # Streak broken
            user["daily_streak"] = 0
            streak_broken = True
        
        # Increment streak
        current_streak = user.get("daily_streak", 0)
        user["daily_streak"] = current_streak + 1
        
        # Get reward
        day_in_week = (user["daily_streak"] - 1) % 7
        reward = daily_rewards[day_in_week]
        
        # Apply reward
        user["balance"] += reward["reward"]
        user["total_income"] += reward["reward"]
        user["last_daily"] = now
        
        # Special reward for day 7
        shield_activated = False
        if day_in_week == 6:  # Day 7
            user["shield_active"] = now + timedelta(hours=24)
            shield_activated = True
        
        save_db()
        
        # Prepare message
        message = [
            f"🎁 <b>Ежедневный бонус</b>\n",
            f"✅ Вы получили {reward['description']}!",
        ]
        
        if shield_activated:
            message.append(f"🛡 Бонус 7-го дня: Щит активирован на 24 часа!")
        
        if streak_broken:
            message.append(f"\n⚠️ Ваша серия была сброшена из-за пропуска дня.")
        
        message.append(f"\nТекущая серия: {user['daily_streak']} дней")
        
        # Show next rewards
        message.append("\n<b>Следующие бонусы:</b>")
        for i in range(3):
            next_day = (day_in_week + i + 1) % 7
            next_reward = daily_rewards[next_day]
            message.append(f"День {next_day + 1}: {next_reward['description']}")
        
        await callback.message.edit_text(
            "\n".join(message),
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🔙 Назад", callback_data=MAIN_MENU)]
            ]),
            parse_mode=ParseMode.HTML
        )
        await callback.answer()
        
    except Exception as e:
        logging.error(f"Daily bonus error: {e}", exc_info=True)
        await callback.answer("❌ Ошибка при получении бонуса", show_alert=True)

@dp.callback_query(F.data == ACHIEVEMENTS)
async def achievements_handler(callback: types.CallbackQuery):
    try:
        user_id = callback.from_user.id
        user = users.get(user_id)
        
        if not user:
            await callback.answer("❌ Сначала зарегистрируйтесь!", show_alert=True)
            return
        
        # Check achievement progress
        check_achievement_progress(user_id)
        
        # Prepare message
        message = ["🏆 <b>Достижения</b>\n"]
        buttons = []
        
        # Group achievements by completion status
        completed = []
        in_progress = []
        
        for achievement_id, achievement_data in achievements.items():
            # Get current level, claimed status, and progress
            current = user.get("achievements", {}).get(achievement_id, (0, False, 0))
            current_level, claimed, progress = current
            
            # Get target for current level
            if current_level < len(achievement_data["tiers"]):
                target = achievement_data["tiers"][current_level]
                reward = achievement_data["rewards"][current_level]
                
                # Calculate percentage
                percentage = min(100, int(progress / target * 100))
                
                # Format description
                description = achievement_data["description"].format(target=format_number(target))
                
                # Check if can claim
                if progress >= target and not claimed:
                    # Can claim
                    completed.append({
                        "id": achievement_id,
                        "name": achievement_data["name"],
                        "description": description,
                        "reward": reward,
                        "emoji": achievement_data["emoji"],
                        "level": current_level + 1,
                        "max_level": len(achievement_data["tiers"])
                    })
                else:
                    # In progress
                    in_progress.append({
                        "id": achievement_id,
                        "name": achievement_data["name"],
                        "description": description,
                        "progress": progress,
                        "target": target,
                        "percentage": percentage,
                        "emoji": achievement_data["emoji"],
                        "level": current_level + 1,
                        "max_level": len(achievement_data["tiers"])
                    })
        
        # Add completed achievements
        if completed:
            message.append("<b>✅ Готовы к получению:</b>")
            for achievement in completed:
                message.append(
                    f"{achievement['emoji']} <b>{achievement['name']}</b> "
                    f"(Ур. {achievement['level']}/{achievement['max_level']})"
                )
                buttons.append([
                    InlineKeyboardButton(
                        text=f"🎁 Получить {format_number(achievement['reward'])}₽",
                        callback_data=f"claim_achievement_{achievement['id']}_{achievement['level'] - 1}"
                    )
                ])
            message.append("")
        
        # Add in-progress achievements
        if in_progress:
            message.append("<b>🔄 В процессе:</b>")
            for achievement in in_progress:
                message.append(
                    f"{achievement['emoji']} <b>{achievement['name']}</b> "
                    f"(Ур. {achievement['level']}/{achievement['max_level']})"
                )
                message.append(
                    f"▸ {achievement['description']}"
                )
                message.append(
                    f"▸ Прогресс: {format_number(achievement['progress'])}/{format_number(achievement['target'])} "
                    f"({achievement['percentage']}%)"
                )
                message.append("")
        
        # Add back button
        buttons.append([InlineKeyboardButton(text="🔙 Назад", callback_data=MAIN_MENU)])
        
        await callback.message.edit_text(
            "\n".join(message),
            reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
            parse_mode=ParseMode.HTML
        )
        await callback.answer()
        
    except Exception as e:
        logging.error(f"Achievements error: {e}", exc_info=True)
        await callback.answer("❌ Ошибка при загрузке достижений", show_alert=True)

@dp.callback_query(F.data.startswith("claim_achievement_"))
async def claim_achievement_handler(callback: types.CallbackQuery):
    try:
        user_id = callback.from_user.id
        user = users.get(user_id)
        
        if not user:
            await callback.answer("❌ Сначала зарегистрируйтесь!", show_alert=True)
            return
        
        # Parse data
        _, _, achievement_id, level_str = callback.data.split("_")
        level = int(level_str)
        
        # Check if achievement exists
        if achievement_id not in achievements:
            await callback.answer("❌ Достижение не найдено", show_alert=True)
            return
        
        # Get achievement data
        achievement_data = achievements[achievement_id]
        
        # Check if level is valid
        if level >= len(achievement_data["tiers"]):
            await callback.answer("❌ Неверный уровень достижения", show_alert=True)
            return
        
        # Get current level, claimed status, and progress
        current = user.get("achievements", {}).get(achievement_id, (0, False, 0))
        current_level, claimed, progress = current
        
        # Check if already claimed
        if claimed:
            await callback.answer("❌ Вы уже получили награду за это достижение", show_alert=True)
            return
        
        # Check if progress is sufficient
        target = achievement_data["tiers"][level]
        if progress < target:
            await callback.answer("❌ Недостаточный прогресс для получения награды", show_alert=True)
            return
        
        # Claim reward
        reward = achievement_data["rewards"][level]
        user["balance"] += reward
        user["total_income"] += reward
        
        # Update achievement
        user.setdefault("achievements", {})[achievement_id] = (current_level, True, progress)
        
        save_db()
        
        await callback.answer(f"🎉 Вы получили {format_number(reward)}₽!", show_alert=True)
        
        # Refresh achievements page
        await achievements_handler(callback)
        
    except Exception as e:
        logging.error(f"Claim achievement error: {e}", exc_info=True)
        await callback.answer("❌ Ошибка при получении награды", show_alert=True)

@dp.callback_query(F.data.startswith(f"{MARKET_PREFIX}view"))
async def market_view_handler(callback: types.CallbackQuery):
    try:
        user_id = callback.from_user.id
        user = users.get(user_id)
        
        if not user:
            await callback.answer("❌ Сначала зарегистрируйтесь!", show_alert=True)
            return
        
        # Filter active listings
        active_listings = []
        for listing in market_listings:
            # Check if listing is still valid
            if listing.get("expires_at") and datetime.fromisoformat(listing["expires_at"]) < datetime.now():
                continue
                
            # Check if slave still exists
            slave_id = listing.get("slave_id")
            if not slave_id or slave_id not in users:
                continue
                
            # Check if slave is still owned by seller
            seller_id = listing.get("seller_id")
            if not seller_id or seller_id not in users:
                continue
                
            if users[slave_id].get("owner") != seller_id:
                continue
                
            active_listings.append(listing)
        
        # Sort by price
        active_listings.sort(key=lambda x: x.get("price", 0))
        
        # Prepare message
        message = ["📊 <b>Рынок рабов</b>\n"]
        
        if not active_listings:
            message.append("На рынке пока нет рабов.\nВы можете выставить своего раба на продажу!")
        else:
            message.append("Доступные рабы:")
            
            for i, listing in enumerate(active_listings[:5], 1):
                slave_id = listing.get("slave_id")
                slave = users.get(slave_id, {})
                price = listing.get("price", 0)
                expires_at = datetime.fromisoformat(listing.get("expires_at", datetime.now().isoformat()))
                time_left = expires_at - datetime.now()
                hours = time_left.seconds // 3600
                minutes = (time_left.seconds % 3600) // 60
                
                message.append(
                    f"{i}. @{slave.get('username', 'unknown')} "
                    f"(Ур. {slave.get('slave_level', 0)})"
                )
                message.append(
                    f"▸ Цена: {format_number(price)}₽"
                )
                message.append(
                    f"▸ Истекает через: {hours}ч {minutes}м"
                )
                message.append("")
        
        # Create buttons
        buttons = []
        
        # Buy buttons
        for i, listing in enumerate(active_listings[:5]):
            slave_id = listing.get("slave_id")
            price = listing.get("price", 0)
            
            buttons.append([
                InlineKeyboardButton(
                    text=f"Купить @{users.get(slave_id, {}).get('username', 'unknown')} за {format_number(price)}₽",
                    callback_data=f"{MARKET_PREFIX}buy_{listing.get('id')}"
                )
            ])
        
        # Sell button
        if user.get("slaves"):
            buttons.append([
                InlineKeyboardButton(
                    text="📈 Выставить раба на продажу",
                    callback_data=f"{MARKET_PREFIX}sell"
                )
            ])
        
        # Back button
        buttons.append([InlineKeyboardButton(text="🔙 Назад", callback_data=BUY_MENU)])
        
        await callback.message.edit_text(
            "\n".join(message),
            reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
            parse_mode=ParseMode.HTML
        )
        await callback.answer()
        
    except Exception as e:
        logging.error(f"Market view error: {e}", exc_info=True)
        await callback.answer("❌ Ошибка при загрузке рынка", show_alert=True)

@dp.callback_query(F.data == f"{MARKET_PREFIX}sell")
async def market_sell_handler(callback: types.CallbackQuery):
    try:
        user_id = callback.from_user.id
        user = users.get(user_id)
        
        if not user:
            await callback.answer("❌ Сначала зарегистрируйтесь!", show_alert=True)
            return
        
        # Check if user has slaves
        if not user.get("slaves"):
            await callback.answer("❌ У вас нет рабов для продажи", show_alert=True)
            return
        
        # Create buttons for each slave
        buttons = []
        for slave_id in user.get("slaves", [])[:5]:  # Maximum 5 first slaves
            slave = users.get(slave_id, {})
            price = slave_price(slave)
            
            buttons.append([
                InlineKeyboardButton(
                    text=f"@{slave.get('username', 'unknown')} (Ур. {slave.get('slave_level', 0)}) - {format_number(price)}₽",
                    callback_data=f"{MARKET_PREFIX}select_{slave_id}"
                )
            ])
        
        # Back button
        buttons.append([InlineKeyboardButton(text="🔙 Назад", callback_data=f"{MARKET_PREFIX}view")])
        
        await callback.message.edit_text(
            "📈 <b>Выставить раба на продажу</b>\n\n"
            "Выберите раба для продажи:",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
            parse_mode=ParseMode.HTML
        )
        await callback.answer()
        
    except Exception as e:
        logging.error(f"Market sell error: {e}", exc_info=True)
        await callback.answer("❌ Ошибка при выборе раба", show_alert=True)

@dp.callback_query(F.data.startswith(f"{MARKET_PREFIX}select_"))
async def market_select_handler(callback: types.CallbackQuery):
    try:
        user_id = callback.from_user.id
        user = users.get(user_id)
        slave_id = int(callback.data.split("_")[2])
        
        if not user:
            await callback.answer("❌ Сначала зарегистрируйтесь!", show_alert=True)
            return
        
        # Check if slave belongs to user
        if slave_id not in user.get("slaves", []):
            await callback.answer("❌ Этот раб вам не принадлежит", show_alert=True)
            return
        
        # Get slave
        slave = users.get(slave_id)
        if not slave:
            await callback.answer("❌ Раб не найден", show_alert=True)
            return
        
        # Calculate base price
        base_price = slave_price(slave)
        
        # Create buttons for different prices
        buttons = []
        prices = [
            int(base_price * 0.8),
            int(base_price),
            int(base_price * 1.2),
            int(base_price * 1.5),
            int(base_price * 2)
        ]
        
        for price in prices:
            buttons.append([
                InlineKeyboardButton(
                    text=f"{format_number(price)}₽",
                    callback_data=f"{MARKET_PREFIX}price_{slave_id}_{price}"
                )
            ])
        
        # Custom price button
        buttons.append([
            InlineKeyboardButton(
                text="🎲 Своя цена",
                callback_data=f"{MARKET_PREFIX}custom_price_{slave_id}"
            )
        ])
        
        # Back button
        buttons.append([InlineKeyboardButton(text="🔙 Назад", callback_data=f"{MARKET_PREFIX}sell")])
        
        await callback.message.edit_text(
            f"📈 <b>Выставить раба на продажу</b>\n\n"
            f"Раб: @{slave.get('username', 'unknown')} (Ур. {slave.get('slave_level', 0)})\n"
            f"Базовая цена: {format_number(base_price)}₽\n\n"
            f"Выберите цену продажи:",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
            parse_mode=ParseMode.HTML
        )
        await callback.answer()
        
    except Exception as e:
        logging.error(f"Market select error: {e}", exc_info=True)
        await callback.answer("❌ Ошибка при выборе цены", show_alert=True)

@dp.callback_query(F.data.startswith(f"{MARKET_PREFIX}price_"))
async def market_price_handler(callback: types.CallbackQuery):
    try:
        user_id = callback.from_user.id
        user = users.get(user_id)
        _, slave_id, price = callback.data.split("_")
        slave_id = int(slave_id)
        price = int(price)
        
        if not user:
            await callback.answer("❌ Сначала зарегистрируйтесь!", show_alert=True)
            return
        
        # Check if slave belongs to user
        if slave_id not in user.get("slaves", []):
            await callback.answer("❌ Этот раб вам не принадлежит", show_alert=True)
            return
        
        # Get slave
        slave = users.get(slave_id)
        if not slave:
            await callback.answer("❌ Раб не найден", show_alert=True)
            return
        
        # Create listing
        listing_id = f"listing_{int(time.time())}_{user_id}_{slave_id}"
        listing = {
            "id": listing_id,
            "seller_id": user_id,
            "slave_id": slave_id,
            "price": price,
            "created_at": datetime.now().isoformat(),
            "expires_at": (datetime.now() + timedelta(hours=24)).isoformat()
        }
        
        # Add to market
        market_listings.append(listing)
        
        save_db()
        
        await callback.message.edit_text(
            f"📈 <b>Раб выставлен на продажу</b>\n\n"
            f"Раб: @{slave.get('username', 'unknown')} (Ур. {slave.get('slave_level', 0)})\n"
            f"Цена: {format_number(price)}₽\n"
            f"Объявление будет активно 24 часа.\n\n"
            f"Когда раба купят, вы получите уведомление и деньги.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🔙 В меню рынка", callback_data=f"{MARKET_PREFIX}view")]
            ]),
            parse_mode=ParseMode.HTML
        )
        await callback.answer()
        
    except Exception as e:
        logging.error(f"Market price error: {e}", exc_info=True)
        await callback.answer("❌ Ошибка при создании объявления", show_alert=True)

@dp.callback_query(F.data.startswith(f"{MARKET_PREFIX}buy_"))
async def market_buy_handler(callback: types.CallbackQuery):
    try:
        user_id = callback.from_user.id
        user = users.get(user_id)
        listing_id = callback.data.split("_")[2]
        
        if not user:
            await callback.answer("❌ Сначала зарегистрируйтесь!", show_alert=True)
            return
        
        # Find listing
        listing = None
        for l in market_listings:
            if l.get("id") == listing_id:
                listing = l
                break
        
        if not listing:
            await callback.answer("❌ Объявление не найдено", show_alert=True)
            return
        
        # Check if listing is still valid
        if listing.get("expires_at") and datetime.fromisoformat(listing["expires_at"]) < datetime.now():
            await callback.answer("❌ Объявление истекло", show_alert=True)
            return
        
        # Get slave and seller
        slave_id = listing.get("slave_id")
        seller_id = listing.get("seller_id")
        
        if not slave_id or slave_id not in users:
            await callback.answer("❌ Раб не найден", show_alert=True)
            return
            
        if not seller_id or seller_id not in users:
            await callback.answer("❌ Продавец не найден", show_alert=True)
            return
        
        slave = users[slave_id]
        seller = users[seller_id]
        
        # Check if slave is still owned by seller
        if slave.get("owner") != seller_id:
            await callback.answer("❌ Раб больше не принадлежит продавцу", show_alert=True)
            return
        
        # Check price
        price = listing.get("price", 0)
        
        if user.get("balance", 0) < price:
            await callback.answer("❌ Недостаточно средств", show_alert=True)
            return
        
        # Check slave limit
        barracks_level = user.get("upgrades", {}).get("barracks", 0)
        slave_limit = 5 + 2 * barracks_level
        if len(user.get("slaves", [])) >= slave_limit:
            await callback.answer(
                f"❌ Лимит рабов ({slave_limit}). Улучшите бараки!",
                show_alert=True
            )
            return
        
        # Process purchase
        user["balance"] -= price
        
        # Apply market bonus to seller
        market_level = seller.get("upgrades", {}).get("market", 0)
        market_bonus = price * (market_level * 0.05)
        seller["balance"] += price + market_bonus
        seller["total_income"] += price + market_bonus
        
        # Update slave ownership
        if slave_id in seller.get("slaves", []):
            seller["slaves"].remove(slave_id)
        
        user.setdefault("slaves", []).append(slave_id)
        
        # Update slave data
        slave["owner"] = user_id
        slave["slave_level"] = min(slave.get("slave_level", 0) + 1, MAX_SLAVE_LEVEL)
        slave["price"] = slave_price(slave)
        slave["enslaved_date"] = datetime.now()
        slave["last_purchased"] = datetime.now()
        
        # Remove listing
        market_listings.remove(listing)
        
        # Check achievement progress
        check_achievement_progress(user_id)
        
        save_db()
        
        # Notify seller
        try:
            await bot.send_message(
                seller_id,
                f"💰 Ваш раб @{slave.get('username', 'unknown')} был продан за {format_number(price)}₽!\n"
                f"Бонус рынка: +{format_number(int(market_bonus))}₽"
            )
        except Exception:
            pass
        
        # Notify slave
        try:
            await bot.send_message(
                slave_id,
                f"⚡ Вы были проданы на рынке!\n"
                f"Новый владелец: @{user.get('username', 'unknown')}\n"
                f"Цена: {format_number(price)}₽\n"
                f"Уровень: {slave['slave_level']}"
            )
        except Exception:
            pass
        
        await callback.message.edit_text(
            f"✅ <b>Покупка успешна!</b>\n\n"
            f"Вы купили @{slave.get('username', 'unknown')} за {format_number(price)}₽\n"
            f"Уровень: {slave['slave_level']}\n"
            f"Доход/час: {format_number(int(100 * (1 + 0.3 * slave['slave_level'])))}₽",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🔙 В меню рынка", callback_data=f"{MARKET_PREFIX}view")]
            ]),
            parse_mode=ParseMode.HTML
        )
        await callback.answer()
        
    except Exception as e:
        logging.error(f"Market buy error: {e}", exc_info=True)
        await callback.answer("❌ Ошибка при покупке", show_alert=True)

async def autosave_task():
    while True:
        await asyncio.sleep(300)  # 5 minutes
        save_db()
        logging.info("Auto-save completed")

async def on_shutdown():
    save_db()
    logging.info("Bot shutdown, database saved")

async def main():
    try:
        # Initialize logging (should be first)
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
            handlers=[
                logging.StreamHandler(),
                logging.FileHandler("bot.log", encoding='utf-8')
            ]
        )
        logger = logging.getLogger(__name__)
        
        logger.info("Starting bot...")
        
        # Load and initialize
        await on_startup()
        
        # Main bot loop
        await dp.start_polling(
            bot, 
            allowed_updates=dp.resolve_used_update_types(),
            skip_updates=True
        )
        
    except (KeyboardInterrupt, SystemExit):
        logger.info("Bot stopped manually")
    except Exception as e:
        logger.critical(f"Critical error: {str(e)}", exc_info=True)
    finally:
        logger.info("Shutting down...")
        await on_shutdown()
        logger.info("Bot successfully stopped")

if __name__ == "__main__":
    try:
        # For Windows, need to set special event loop
        if os.name == 'nt':
            asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
            
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
