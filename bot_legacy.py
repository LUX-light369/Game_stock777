import asyncio
import io
import json
import logging
import os
import random
import re
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Set, Tuple

from aiogram import Bot, Dispatcher, F, types
from aiogram.exceptions import TelegramRetryAfter, TelegramBadRequest
from aiogram.filters import Command, StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    CallbackQuery,
    InlineKeyboardMarkup,
    ReplyKeyboardMarkup,
    Message,
    ChatPermissions,
    BufferedInputFile,
    InlineKeyboardButton,
)
from aiogram.utils.keyboard import InlineKeyboardBuilder, ReplyKeyboardBuilder
from PIL import Image, ImageDraw, ImageFont

# ---------- ПЕРЕМЕННЫЕ ОКРУЖЕНИЯ ----------
BOT_TOKEN = os.getenv("BOT_TOKEN")
MAIN_ADMIN_ID_STR = os.getenv("MAIN_ADMIN_ID")
if not BOT_TOKEN or not MAIN_ADMIN_ID_STR:
    raise ValueError("BOT_TOKEN и MAIN_ADMIN_ID обязательны")
MAIN_ADMIN_ID = int(MAIN_ADMIN_ID_STR)

DATA_FILE = "bot_data.json"

# ---------- ГЛОБАЛЬНЫЕ ШРИФТЫ ----------
try:
    FONT_TITLE = ImageFont.truetype("Arial.ttf", 28)
    FONT_NUMBERS = ImageFont.truetype("Arial.ttf", 24)
    FONT_CELL = ImageFont.truetype("Arial.ttf", 21)   # увеличен с 14 до 21
    FONT_DATE = ImageFont.truetype("Arial.ttf", 16)
except IOError:
    FONT_TITLE = ImageFont.load_default()
    FONT_NUMBERS = ImageFont.load_default()
    FONT_CELL = ImageFont.load_default()
    FONT_DATE = ImageFont.load_default()

# ---------- ОЧЕРЕДЬ СООБЩЕНИЙ ----------
message_queues: Dict[int, asyncio.Queue] = {}
QUEUE_PAUSE = 1.0   # задержка между отправкой сообщений 1 секунда

async def message_worker(chat_id: int):
    q = message_queues[chat_id]
    while True:
        kwargs = await q.get()
        try:
            await bot.send_message(chat_id, **kwargs)
        except TelegramRetryAfter as e:
            await asyncio.sleep(e.retry_after)
            await bot.send_message(chat_id, **kwargs)
        except Exception as e:
            logging.error(f"Ошибка отправки в чат {chat_id}: {e}")
        finally:
            await asyncio.sleep(QUEUE_PAUSE)

def enqueue_message(chat_id: int, text: str, **kwargs):
    if chat_id not in message_queues:
        q = asyncio.Queue()
        message_queues[chat_id] = q
        asyncio.create_task(message_worker(chat_id))
    message_queues[chat_id].put_nowait({"text": text, **kwargs})

# ---------- ОЧЕРЕДЬ ПРОВЕРОК (анти-перемешивание + защита от спама) ----------
check_queues: Dict[int, asyncio.Queue] = {}

async def check_worker(chat_id: int):
    q = check_queues[chat_id]
    while True:
        coro = await q.get()
        try:
            # Ждём, пока очередь сообщений чата опустеет
            if chat_id in message_queues:
                while not message_queues[chat_id].empty():
                    await asyncio.sleep(0.5)
            await coro
        except Exception as e:
            logging.error(f"Ошибка в обработке проверки чата {chat_id}: {e}")
        finally:
            await asyncio.sleep(1)   # защита от спама: пауза 1 сек между проверками

def enqueue_check(chat_id: int, coro):
    if chat_id not in check_queues:
        q = asyncio.Queue()
        check_queues[chat_id] = q
        asyncio.create_task(check_worker(chat_id))
    check_queues[chat_id].put_nowait(coro)

# ---------- МОДЕЛИ ДАННЫХ ----------
class ChatSettings:
    def __init__(self):
        self.items: List[str] = []
        self.index: int = 0
        self.interval: float = 2.0
        self.status: str = "stopped"
        self.dice_enabled: bool = False
        self.slot_enabled: bool = False
        self.loto_enabled: bool = False
        self.sea_battle_enabled: bool = False
        self.loto_triggers: List[str] = ["стоп"]
        self.loto_trigger_enabled: bool = True
        self.sea_battle_triggers: List[str] = ["залп"]
        self.sea_battle_trigger_enabled: bool = True
        self.muted_users: List[int] = []
        self.loto_finalists_count: int = 3
        self.loto_finalists: List[str] = []
        self.loto_finalists_disabled: bool = False
        self.sea_battle_finalists_count: int = 3
        self.sea_battle_finalists: List[str] = []
        self.sea_battle_finalists_disabled: bool = False
        self.dictation_start_message: Optional[str] = None

    def to_dict(self):
        return {
            "items": self.items, "index": self.index, "interval": self.interval,
            "status": self.status, "dice_enabled": self.dice_enabled,
            "slot_enabled": self.slot_enabled,
            "loto_enabled": self.loto_enabled, "sea_battle_enabled": self.sea_battle_enabled,
            "loto_triggers": self.loto_triggers, "loto_trigger_enabled": self.loto_trigger_enabled,
            "sea_battle_triggers": self.sea_battle_triggers, "sea_battle_trigger_enabled": self.sea_battle_trigger_enabled,
            "muted_users": self.muted_users,
            "loto_finalists_count": self.loto_finalists_count,
            "loto_finalists": self.loto_finalists, "loto_finalists_disabled": self.loto_finalists_disabled,
            "sea_battle_finalists_count": self.sea_battle_finalists_count,
            "sea_battle_finalists": self.sea_battle_finalists, "sea_battle_finalists_disabled": self.sea_battle_finalists_disabled,
            "dictation_start_message": self.dictation_start_message,
        }

    @classmethod
    def from_dict(cls, d: dict):
        obj = cls()
        obj.items = d.get("items", [])
        obj.index = d.get("index", 0)
        obj.interval = d.get("interval", 2.0)
        obj.status = d.get("status", "stopped")
        obj.dice_enabled = d.get("dice_enabled", False)
        obj.slot_enabled = d.get("slot_enabled", False)
        obj.loto_enabled = d.get("loto_enabled", False)
        obj.sea_battle_enabled = d.get("sea_battle_enabled", False)
        obj.loto_triggers = d.get("loto_triggers", ["стоп"])
        if isinstance(obj.loto_triggers, str): obj.loto_triggers = [obj.loto_triggers]
        obj.loto_trigger_enabled = d.get("loto_trigger_enabled", True)
        obj.sea_battle_triggers = d.get("sea_battle_triggers", ["залп"])
        if isinstance(obj.sea_battle_triggers, str): obj.sea_battle_triggers = [obj.sea_battle_triggers]
        obj.sea_battle_trigger_enabled = d.get("sea_battle_trigger_enabled", True)
        obj.muted_users = d.get("muted_users", [])
        obj.loto_finalists_count = d.get("loto_finalists_count", 3)
        obj.loto_finalists = d.get("loto_finalists", [])
        obj.loto_finalists_disabled = d.get("loto_finalists_disabled", False)
        obj.sea_battle_finalists_count = d.get("sea_battle_finalists_count", 3)
        obj.sea_battle_finalists = d.get("sea_battle_finalists", [])
        obj.sea_battle_finalists_disabled = d.get("sea_battle_finalists_disabled", False)
        obj.dictation_start_message = d.get("dictation_start_message")
        return obj

class LotoUserData:
    def __init__(self, username: str, numbers: List[str]):
        self.username = username
        self.numbers = numbers
        self.registered_at = datetime.now()
        self.card_message_id: Optional[int] = None
        self.photo_file_id: Optional[str] = None

    def to_dict(self):
        return {
            "username": self.username, "numbers": self.numbers,
            "registered_at": self.registered_at.isoformat(),
            "card_message_id": self.card_message_id, "photo_file_id": self.photo_file_id,
        }

    @classmethod
    def from_dict(cls, d: dict):
        obj = cls.__new__(cls)
        obj.username = d["username"]
        obj.numbers = d["numbers"]
        obj.registered_at = datetime.fromisoformat(d["registered_at"])
        obj.card_message_id = d.get("card_message_id")
        obj.photo_file_id = d.get("photo_file_id")
        return obj

class SeaBattleUserData:
    def __init__(self, username: str, cells: List[str]):
        self.username = username
        self.cells = cells
        self.registered_at = datetime.now()
        self.card_message_id: Optional[int] = None
        self.photo_file_id: Optional[str] = None

    def to_dict(self):
        return {
            "username": self.username, "cells": self.cells,
            "registered_at": self.registered_at.isoformat(),
            "card_message_id": self.card_message_id, "photo_file_id": self.photo_file_id,
        }

    @classmethod
    def from_dict(cls, d: dict):
        obj = cls.__new__(cls)
        obj.username = d["username"]
        obj.cells = d["cells"]
        obj.registered_at = datetime.fromisoformat(d["registered_at"])
        obj.card_message_id = d.get("card_message_id")
        obj.photo_file_id = d.get("photo_file_id")
        return obj

class BotData:
    def __init__(self):
        self.admins: Set[int] = {MAIN_ADMIN_ID}
        self.default_chat_id: Optional[int] = None
        self.default_channel_id: Optional[int] = None
        self.channel_invite_link: Optional[str] = None
        self.check_subscription: bool = True
        self.chats: Dict[int, ChatSettings] = {}
        self.loto_users: Dict[int, LotoUserData] = {}
        self.sea_battle_users: Dict[int, SeaBattleUserData] = {}
        self.banned_users: Dict[str, datetime] = {}
        self.registered_ever: Set[int] = set()
        self.loto_target_chat: Optional[int] = None
        self.loto_deadline: Optional[datetime] = None
        self.loto_start_message: Optional[str] = None
        self.loto_user_buttons: Dict[str, str] = {}
        self.sea_battle_target_chat: Optional[int] = None
        self.sea_battle_deadline: Optional[datetime] = None
        self.sea_battle_start_message: Optional[str] = None
        self.sea_battle_user_buttons: Dict[str, str] = {}

    def save(self):
        data = {
            "admins": list(self.admins),
            "default_chat_id": self.default_chat_id,
            "default_channel_id": self.default_channel_id,
            "channel_invite_link": self.channel_invite_link,
            "check_subscription": self.check_subscription,
            "chats": {str(k): v.to_dict() for k, v in self.chats.items()},
            "loto_users": {str(uid): u.to_dict() for uid, u in self.loto_users.items()},
            "sea_battle_users": {str(uid): u.to_dict() for uid, u in self.sea_battle_users.items()},
            "banned_users": {uname: dt.isoformat() for uname, dt in self.banned_users.items()},
            "registered_ever": list(self.registered_ever),
            "loto_target_chat": self.loto_target_chat,
            "loto_deadline": self.loto_deadline.isoformat() if self.loto_deadline else None,
            "loto_start_message": self.loto_start_message,
            "loto_user_buttons": self.loto_user_buttons,
            "sea_battle_target_chat": self.sea_battle_target_chat,
            "sea_battle_deadline": self.sea_battle_deadline.isoformat() if self.sea_battle_deadline else None,
            "sea_battle_start_message": self.sea_battle_start_message,
            "sea_battle_user_buttons": self.sea_battle_user_buttons,
        }
        with open(DATA_FILE, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

    @classmethod
    def load(cls):
        obj = cls()
        try:
            with open(DATA_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
            obj.admins = set(data.get("admins", [MAIN_ADMIN_ID]))
            obj.admins.add(MAIN_ADMIN_ID)
            obj.default_chat_id = data.get("default_chat_id")
            obj.default_channel_id = data.get("default_channel_id")
            obj.channel_invite_link = data.get("channel_invite_link")
            obj.check_subscription = data.get("check_subscription", True)
            for chat_id_str, chat_data in data.get("chats", {}).items():
                obj.chats[int(chat_id_str)] = ChatSettings.from_dict(chat_data)
            for uid_str, u_data in data.get("loto_users", {}).items():
                obj.loto_users[int(uid_str)] = LotoUserData.from_dict(u_data)
            for uid_str, u_data in data.get("sea_battle_users", {}).items():
                obj.sea_battle_users[int(uid_str)] = SeaBattleUserData.from_dict(u_data)
            obj.banned_users = {uname: datetime.fromisoformat(iso) for uname, iso in data.get("banned_users", {}).items()}
            obj.registered_ever = set(data.get("registered_ever", []))
            obj.loto_target_chat = data.get("loto_target_chat")
            obj.loto_deadline = datetime.fromisoformat(data["loto_deadline"]) if data.get("loto_deadline") else None
            obj.loto_start_message = data.get("loto_start_message")
            obj.loto_user_buttons = data.get("loto_user_buttons", {})
            obj.sea_battle_target_chat = data.get("sea_battle_target_chat")
            obj.sea_battle_deadline = datetime.fromisoformat(data["sea_battle_deadline"]) if data.get("sea_battle_deadline") else None
            obj.sea_battle_start_message = data.get("sea_battle_start_message")
            obj.sea_battle_user_buttons = data.get("sea_battle_user_buttons", {})
        except FileNotFoundError:
            pass
        return obj

bot_data = BotData.load()
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(storage=MemoryStorage())

# ---------- FSM СОСТОЯНИЯ ----------
class UploadList(StatesGroup): waiting_for_list = State()
class SetChatIdState(StatesGroup): waiting_for_chat_id = State()
class SetChannelIdState(StatesGroup): waiting_for_channel_id = State()
class VerifyList(StatesGroup): waiting_for_verify = State()
class AdminAdd(StatesGroup): waiting_for_id = State()
class AdminRemove(StatesGroup): waiting_for_id = State()
class DictationStartMessage(StatesGroup): waiting_for_start_message = State()
class SetIntervalState(StatesGroup): waiting_for_interval = State()
class SetInviteLinkState(StatesGroup): waiting_for_link = State()
class LotoRegistration(StatesGroup): waiting_for_numbers = State()
class SeaBattleRegistration(StatesGroup): waiting_for_cells = State()
class LotoBroadcast(StatesGroup): waiting_for_message = State()
class SeaBattleBroadcast(StatesGroup): waiting_for_message = State()
class LotoSettingsFlow(StatesGroup):
    waiting_for_target_chat = State(); waiting_for_deadline = State()
    waiting_for_start_message = State(); waiting_for_buttons = State()
    waiting_for_bans = State(); waiting_for_finalists = State()
class LotoTriggerInput(StatesGroup): waiting_for_triggers = State()
class SeaBattleSettingsFlow(StatesGroup):
    waiting_for_target_chat = State(); waiting_for_deadline = State()
    waiting_for_start_message = State(); waiting_for_buttons = State()
    waiting_for_triggers = State(); waiting_for_finalists = State()
    waiting_for_bans = State()
class SeaBattleTriggerInput(StatesGroup): waiting_for_triggers = State()

# ---------- ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ----------
def is_admin(user_id: int) -> bool: return user_id in bot_data.admins
def is_main_admin(user_id: int) -> bool: return user_id == MAIN_ADMIN_ID

async def check_chat_access(user_id: int, chat_id: int) -> bool:
    if is_main_admin(user_id): return True
    try:
        member = await bot.get_chat_member(chat_id, user_id)
        return member.status in ("creator", "administrator", "member")
    except: return False

async def is_chat_admin(chat_id: int, user_id: int) -> bool:
    try:
        member = await bot.get_chat_member(chat_id, user_id)
        return member.status in ("creator", "administrator")
    except: return False

def parse_list(text: str) -> List[str]:
    parts = re.split(r'[,\s]+', text.strip())
    return [p.strip() for p in parts if p.strip()]

def now_novosibirsk(): return datetime.now() + timedelta(hours=7)

async def check_subscription(user_id: int, channel_id: Optional[int]) -> bool:
    if not channel_id: return True
    try:
        member = await bot.get_chat_member(channel_id, user_id)
        return member.status in ("member", "administrator", "creator", "restricted")
    except: return False

# ---------- ГЕНЕРАЦИЯ КАРТОЧЕК ----------
def generate_loto_card(username: str, numbers: List[str], registered_at: datetime) -> BufferedInputFile:
    bg_color = (random.randint(180, 240), random.randint(180, 240), random.randint(180, 240))
    img = Image.new("RGB", (450, 280), bg_color)
    draw = ImageDraw.Draw(img)
    text = f"@{username}" if username else "Аноним"
    bbox = draw.textbbox((0, 0), text, font=FONT_TITLE)
    w = bbox[2] - bbox[0]
    draw.text(((450 - w) / 2, 10), text, fill="black", font=FONT_TITLE)
    sorted_nums = sorted(numbers, key=lambda x: int(x) if x.isdigit() else x)
    cell_w, cell_h = 70, 40
    start_x = (450 - cell_w * 5) / 2
    start_y = 60
    for row in range(3):
        for col in range(5):
            num = sorted_nums[row * 5 + col]
            x = start_x + col * cell_w; y = start_y + row * cell_h
            draw.rectangle([x, y, x + cell_w, y + cell_h], outline="black", width=1)
            num_str = str(num)
            bbox = draw.textbbox((0, 0), num_str, font=FONT_NUMBERS)
            tw, th = bbox[2] - bbox[0], bbox[3] - bbox[1]
            draw.text((x + (cell_w - tw) / 2, y + (cell_h - th) / 2), num_str, fill="black", font=FONT_NUMBERS)
    dt_str = registered_at.strftime("%d.%m.%Y %H:%M (Нск)")
    bbox = draw.textbbox((0, 0), dt_str, font=FONT_DATE)
    wdt = bbox[2] - bbox[0]
    draw.text(((450 - wdt) / 2, start_y + 3 * cell_h + 5), dt_str, fill="black", font=FONT_DATE)
    bio = io.BytesIO(); img.save(bio, format="PNG"); bio.seek(0); del img, draw
    return BufferedInputFile(bio.read(), filename="card.png")

async def send_loto_card_and_notify(uid: int, username: str, numbers: List[str], registered_at: datetime):
    if not bot_data.loto_target_chat: return
    photo = generate_loto_card(username, numbers, registered_at)
    sorted_nums = sorted(numbers, key=lambda x: int(x) if x.isdigit() else x)
    caption = ", ".join(sorted_nums) + f" @{username}"
    msg = await bot.send_photo(bot_data.loto_target_chat, photo=photo, caption=caption)
    file_id = msg.photo[-1].file_id
    bot_data.loto_users[uid].photo_file_id = file_id
    bot_data.loto_users[uid].card_message_id = msg.message_id
    bot_data.save()
    try:
        await bot.copy_message(chat_id=uid, from_chat_id=bot_data.loto_target_chat, message_id=msg.message_id)
    except Exception as e: logging.error(f"Не удалось отправить копию пользователю {uid}: {e}")

COLS = "АБВГДЕЖЗИК"

def parse_cells(cells: List[str]) -> Set[Tuple[int, int]]:
    result = set()
    for c in cells:
        c = c.strip().upper()
        if len(c) < 2: continue
        letter, num = c[0], c[1:]
        if letter in COLS and num.isdigit() and 1 <= int(num) <= 10:
            result.add((int(num)-1, COLS.index(letter)))
    return result

def validate_ships(cells_set: Set[Tuple[int, int]]) -> bool:
    if len(cells_set) != 15:
        return False
    visited = set()
    ships = []
    for cell in cells_set:
        if cell in visited:
            continue
        stack = [cell]
        ship = set()
        while stack:
            r, c = stack.pop()
            if (r, c) in visited:
                continue
            visited.add((r, c))
            ship.add((r, c))
            for dr, dc in ((1,0),(-1,0),(0,1),(0,-1)):
                nr, nc = r+dr, c+dc
                if (nr, nc) in cells_set and (nr, nc) not in visited:
                    stack.append((nr, nc))
        rows = {rr for rr,_ in ship}
        cols = {cc for _,cc in ship}
        if len(rows) > 1 and len(cols) > 1:
            return False
        ships.append(ship)
    from collections import Counter
    sz = Counter(len(s) for s in ships)
    if sz.get(1,0) != 4 or sz.get(2,0) != 2 or sz.get(3,0) != 1 or sz.get(4,0) != 1:
        return False
    cell_to_ship = {}
    for idx, ship in enumerate(ships):
        for cell in ship:
            cell_to_ship[cell] = idx
    for idx, ship in enumerate(ships):
        for r, c in ship:
            for dr in (-1,0,1):
                for dc in (-1,0,1):
                    if dr == 0 and dc == 0:
                        continue
                    nr, nc = r+dr, c+dc
                    if (nr, nc) in cell_to_ship:
                        if cell_to_ship[(nr, nc)] != idx:
                            return False
    return True

def generate_random_sea_battle_cells() -> List[str]:
    while True:
        field = [[0]*10 for _ in range(10)]
        try:
            place_ship(field, 4)
            place_ship(field, 3)
            place_ship(field, 2)
            place_ship(field, 2)
            for _ in range(4):
                place_ship(field, 1)
            break
        except Exception:
            continue
    cells = []
    for r in range(10):
        for c in range(10):
            if field[r][c] == 1:
                cells.append(f"{COLS[c]}{r+1}")
    return cells

def place_ship(field: List[List[int]], size: int):
    placed = False
    for _ in range(100):
        orientation = random.choice(['h', 'v'])
        if orientation == 'h':
            r = random.randint(0, 9)
            c = random.randint(0, 10 - size)
            coords = [(r, c+i) for i in range(size)]
        else:
            r = random.randint(0, 10 - size)
            c = random.randint(0, 9)
            coords = [(r+i, c) for i in range(size)]
        if all(field[rr][cc] == 0 for rr, cc in coords) and all(
            not any(field[nr][nc] for nr in range(max(0, rr-1), min(10, rr+2))
                    for nc in range(max(0, cc-1), min(10, cc+2)) if (nr, nc) not in coords)
            for rr, cc in coords
        ):
            for rr, cc in coords:
                field[rr][cc] = 1
            placed = True
            break
    if not placed:
        raise Exception("Не удалось разместить корабль")

def generate_sea_battle_card(username: str, cells: List[str], registered_at: datetime) -> BufferedInputFile:
    bg_color = (random.randint(180, 240), random.randint(180, 240), random.randint(180, 240))
    img = Image.new("RGB", (520, 600), bg_color)
    draw = ImageDraw.Draw(img)
    text = f"@{username}" if username else "Аноним"
    bbox = draw.textbbox((0,0), text, font=FONT_TITLE)
    w = bbox[2] - bbox[0]
    draw.text(((520 - w)//2, 10), text, fill="black", font=FONT_TITLE)
    username_bottom = bbox[3] + 10
    cell_size, start_x = 40, 60
    start_y = max(60, username_bottom + 70)
    for i, ch in enumerate(COLS):
        x = start_x + i*cell_size + cell_size//2 - 5
        draw.text((x, start_y - 30), ch, fill="black", font=FONT_CELL)
    for i in range(10):
        y = start_y + i*cell_size + cell_size//2 - 8
        draw.text((start_x - 25, y), str(i+1), fill="black", font=FONT_CELL)
    cells_set = parse_cells(cells)
    for r in range(10):
        for c in range(10):
            x = start_x + c*cell_size; y = start_y + r*cell_size
            draw.rectangle([x, y, x+cell_size, y+cell_size], outline="black", width=1)
            if (r, c) in cells_set:
                draw.rectangle([x+2, y+2, x+cell_size-2, y+cell_size-2], fill="navy")
    dt_str = registered_at.strftime("%d.%m.%Y %H:%M (Нск)")
    bbox = draw.textbbox((0,0), dt_str, font=FONT_DATE)
    wdt = bbox[2] - bbox[0]
    draw.text(((520 - wdt)//2, start_y + 10*cell_size + 20), dt_str, fill="black", font=FONT_DATE)
    bio = io.BytesIO(); img.save(bio, format="PNG"); bio.seek(0); del img, draw
    return BufferedInputFile(bio.read(), filename="card.png")

async def send_sea_battle_card(uid: int, username: str, cells: List[str], registered_at: datetime):
    if not bot_data.sea_battle_target_chat: return
    photo = generate_sea_battle_card(username, cells, registered_at)
    sorted_cells = sorted(cells, key=lambda x: (x[0], int(x[1:])))
    caption = ", ".join(sorted_cells) + f" @{username}"
    msg = await bot.send_photo(bot_data.sea_battle_target_chat, photo=photo, caption=caption)
    bot_data.sea_battle_users[uid].photo_file_id = msg.photo[-1].file_id
    bot_data.sea_battle_users[uid].card_message_id = msg.message_id
    bot_data.save()
    try:
        await bot.copy_message(chat_id=uid, from_chat_id=bot_data.sea_battle_target_chat, message_id=msg.message_id)
    except Exception as e: logging.error(f"Не удалось отправить копию пользователю {uid}: {e}")

# ---------- КНОПКИ ----------
def main_menu_keyboard() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text="▶️ /play", callback_data="menu_play")
    builder.button(text="⏸ /pause", callback_data="menu_pause")
    builder.button(text="⏹ /stop", callback_data="menu_stop")
    builder.button(text="📂 /upload", callback_data="menu_upload")
    builder.button(text="✅ РУЧНАЯ ПРОВЕРКА ✅", callback_data="menu_verify")
    chat_id = bot_data.default_chat_id
    settings = bot_data.chats.get(chat_id) if chat_id else None
    dice_on = settings.dice_enabled if settings else False
    slot_on = settings.slot_enabled if settings else False
    loto_on = settings.loto_enabled if settings else False
    sb_on = settings.sea_battle_enabled if settings else False
    dice_text = f"🎲 Кубики: {'ВКЛ' if dice_on else 'ВЫКЛ'}"
    slot_text = f"🎰 Слот: {'ВКЛ' if slot_on else 'ВЫКЛ'}"
    loto_text = f"🎫 Лото: {'ВКЛ' if loto_on else 'ВЫКЛ'}"
    sb_text = f"⚓ Морской бой: {'ВКЛ' if sb_on else 'ВЫКЛ'}"
    builder.button(text=dice_text, callback_data="menu_dice_toggle")
    builder.button(text=slot_text, callback_data="menu_slot_toggle")
    builder.button(text=loto_text, callback_data="menu_loto_toggle")
    builder.button(text=sb_text, callback_data="menu_sea_battle_toggle")
    builder.button(text="⚙️ Настройки", callback_data="menu_settings")
    builder.adjust(2, 2, 1, 2, 2, 1)
    return builder.as_markup()

def settings_menu_keyboard() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text="📌 Мой чат", callback_data="settings_my_chat")
    builder.button(text="📢 Мой канал", callback_data="settings_my_channel")
    builder.button(text="⏱ Интервал", callback_data="settings_my_interval")
    builder.button(text="🔗 Ссылка приглашения", callback_data="settings_invite_link")
    builder.button(text="🔔 Проверка подписки", callback_data="settings_sub_check")
    builder.button(text="💬 Стартовое сообщение", callback_data="settings_start_msg")
    builder.button(text="🎫 Настройки ЛОТО", callback_data="settings_loto")
    builder.button(text="⚓ Настройки Морского боя", callback_data="settings_sea_battle")
    builder.button(text="📋 Все чаты", callback_data="settings_chats")
    builder.button(text="📋 Список финалистов Лото", callback_data="settings_finalists_loto")
    builder.button(text="📋 Список финалистов МБ", callback_data="settings_finalists_sea")
    if bot_data.admins: builder.button(text="👑 Администрирование", callback_data="settings_admin")
    builder.button(text="↩️ Назад", callback_data="settings_back")
    builder.adjust(1)
    return builder.as_markup()

def loto_menu_keyboard() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text="💬 Чат для карточек", callback_data="loto_chat")
    builder.button(text="📅 Дедлайн", callback_data="loto_deadline")
    builder.button(text="💬 Стартовое сообщение", callback_data="loto_start_msg")
    builder.button(text="🔘 Кнопки участников", callback_data="loto_buttons")
    builder.button(text="🏆 Ограничения", callback_data="loto_bans")
    builder.button(text="🎫 Кол-во финалистов", callback_data="loto_finalists")
    settings = bot_data.chats.get(bot_data.default_chat_id) if bot_data.default_chat_id else None
    trigger_enabled = settings.loto_trigger_enabled if settings else True
    trigger_text = f"⚡ Триггер: {'ВКЛ' if trigger_enabled else 'ВЫКЛ'}"
    builder.button(text=trigger_text, callback_data="loto_toggle_trigger")
    builder.button(text="⚙️ Слова-триггеры", callback_data="loto_triggers")
    builder.button(text="ℹ️ Текущие настройки", callback_data="loto_current")
    builder.button(text="📊 Статистика", callback_data="loto_stats")
    builder.button(text="📢 Рассылка", callback_data="loto_broadcast")
    builder.button(text="↩️ Назад", callback_data="loto_back")
    builder.adjust(1)
    return builder.as_markup()

def sea_battle_menu_keyboard() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text="💬 Чат для карточек", callback_data="sb_chat")
    builder.button(text="📅 Дедлайн", callback_data="sb_deadline")
    builder.button(text="💬 Стартовое сообщение", callback_data="sb_start_msg")
    builder.button(text="🔘 Кнопки участников", callback_data="sb_buttons")
    builder.button(text="🏆 Ограничения", callback_data="sb_bans")
    builder.button(text="🎫 Кол-во финалистов", callback_data="sb_finalists")
    settings = bot_data.chats.get(bot_data.default_chat_id) if bot_data.default_chat_id else None
    trigger_enabled = settings.sea_battle_trigger_enabled if settings else True
    trigger_text = f"⚡ Триггер: {'ВКЛ' if trigger_enabled else 'ВЫКЛ'}"
    builder.button(text=trigger_text, callback_data="sb_toggle_trigger")
    builder.button(text="⚙️ Слова-триггеры", callback_data="sb_triggers")
    builder.button(text="ℹ️ Текущие настройки", callback_data="sb_current")
    builder.button(text="📊 Статистика", callback_data="sb_stats")
    builder.button(text="📢 Рассылка", callback_data="sb_broadcast")
    builder.button(text="↩️ Назад", callback_data="sb_back")
    builder.adjust(1)
    return builder.as_markup()

def admin_menu_keyboard() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text="➕ Добавить админа", callback_data="admin_add")
    builder.button(text="➖ Удалить админа", callback_data="admin_remove")
    builder.button(text="👥 Список админов", callback_data="admin_list")
    builder.button(text="🏆 Ограничения победителей", callback_data="admin_bans")
    builder.button(text="↩️ Назад", callback_data="admin_back")
    builder.adjust(1)
    return builder.as_markup()

def common_reply_keyboard() -> ReplyKeyboardMarkup:
    builder = ReplyKeyboardBuilder()
    builder.button(text="🟢 ЗАПИСЬ НА ИГРУ 🟢")
    settings = bot_data.chats.get(bot_data.default_chat_id) if bot_data.default_chat_id else None
    if settings:
        if settings.loto_enabled:
            for text in bot_data.loto_user_buttons: builder.button(text=text)
        elif settings.sea_battle_enabled:
            for text in bot_data.sea_battle_user_buttons: builder.button(text=text)
    builder.adjust(1)
    return builder.as_markup(resize_keyboard=True)
    
# ===================== ДИКТОВКА =====================
async def dictation_loop(chat_id: int, settings: ChatSettings):
    while settings.status == "playing" and settings.index < len(settings.items):
        item = settings.items[settings.index]
        enqueue_message(chat_id, item)
        settings.index += 1
        bot_data.save()
        if settings.index >= len(settings.items):
            settings.status = "paused"
            bot_data.save()
            enqueue_message(chat_id, "✅ Весь рандом чисел продиктован!")
            if settings.loto_enabled and settings.loto_finalists:
                if len(settings.loto_finalists) < settings.loto_finalists_count:
                    for admin_id in bot_data.admins:
                        await bot.send_message(admin_id,
                            f"Лото: набрано только {len(settings.loto_finalists)} из {settings.loto_finalists_count}. Выдать список?",
                            reply_markup=InlineKeyboardBuilder().button(text="Да", callback_data=f"give_finalists_loto_{chat_id}").as_markup())
                else:
                    await send_finalists_list(chat_id, settings, "loto")
            if settings.sea_battle_enabled and settings.sea_battle_finalists:
                if len(settings.sea_battle_finalists) < settings.sea_battle_finalists_count:
                    for admin_id in bot_data.admins:
                        await bot.send_message(admin_id,
                            f"МБ: набрано только {len(settings.sea_battle_finalists)} из {settings.sea_battle_finalists_count}. Выдать список?",
                            reply_markup=InlineKeyboardBuilder().button(text="Да", callback_data=f"give_finalists_sea_{chat_id}").as_markup())
                else:
                    await send_finalists_list(chat_id, settings, "sea_battle")
            if not settings.loto_finalists and not settings.sea_battle_finalists:
                for uid in bot_data.admins:
                    await bot.send_message(uid, "✅ Весь рандом выстрелов продиктован!", reply_markup=main_menu_keyboard())
            return
        await asyncio.sleep(settings.interval)

async def send_finalists_list(chat_id: int, settings: ChatSettings, game: str):
    digits = ["1️⃣","2️⃣","3️⃣","4️⃣","5️⃣","6️⃣","7️⃣","8️⃣","9️⃣","🔟"]
    finalists = settings.loto_finalists if game == "loto" else settings.sea_battle_finalists
    lines = []
    for i, username in enumerate(finalists):
        num = digits[i] if i < 10 else f"{i+1}️⃣"
        lines.append(f"{num} @{username}")
    text = "\n".join(lines)
    for admin_id in bot_data.admins:
        await bot.send_message(admin_id, text)

# ===================== КОЛЛБЭКИ ВЫДАЧИ ФИНАЛИСТОВ =====================
@dp.callback_query(F.data.startswith("give_finalists_loto_"))
async def give_finalists_loto_callback(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer("⛔", show_alert=True)
        return
    chat_id = int(callback.data.split("_")[-1])
    settings = bot_data.chats.get(chat_id)
    if settings:
        await send_finalists_list(chat_id, settings, "loto")
    await callback.answer("Список отправлен.")
    try:
        await callback.message.delete_reply_markup()
    except:
        pass

@dp.callback_query(F.data.startswith("give_finalists_sea_"))
async def give_finalists_sea_callback(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer("⛔", show_alert=True)
        return
    chat_id = int(callback.data.split("_")[-1])
    settings = bot_data.chats.get(chat_id)
    if settings:
        await send_finalists_list(chat_id, settings, "sea_battle")
    await callback.answer("Список отправлен.")
    try:
        await callback.message.delete_reply_markup()
    except:
        pass

# ===================== ХЕНДЛЕРЫ КОМАНД =====================
@dp.message(Command("start"), F.chat.type == "private")
async def cmd_start(message: Message, state: FSMContext):
    user = message.from_user
    if is_admin(user.id):
        await message.answer("🤖 Главное меню", reply_markup=main_menu_keyboard())
        return
    chat_id = bot_data.default_chat_id
    if not chat_id:
        await message.answer("⛔ Бот не настроен. Обратитесь к администратору.")
        return
    settings = bot_data.chats.get(chat_id)
    if not settings:
        await message.answer("Чат не настроен.")
        return
    if bot_data.check_subscription and bot_data.default_channel_id:
        if not await check_subscription(user.id, bot_data.default_channel_id):
            kb = InlineKeyboardBuilder()
            invite = bot_data.channel_invite_link or "https://t.me/your_channel"
            kb.button(text="📢 Подписаться", url=invite)
            kb.button(text="✅ Проверить", callback_data="check_sub")
            kb.adjust(1)
            await message.answer("❗ Подпишитесь на канал и нажмите «Проверить».", reply_markup=kb.as_markup())
            return
    if not user.username:
        await message.answer("❌ У вас нет @username. Установите его в настройках Telegram и нажмите /start.")
        return
    if settings.loto_enabled:
        await start_loto_registration(message, state)
    elif settings.sea_battle_enabled:
        await start_sea_battle_registration(message, state)
    else:
        await message.answer("📭 Сегодня игр нет. Играем по средам(ЛОТО) и субботам(Морской бой).")

@dp.message(F.text == "🟢 ЗАПИСЬ НА ИГРУ 🟢", F.chat.type == "private")
async def restart_registration(message: Message, state: FSMContext):
    await cmd_start(message, state)

@dp.callback_query(F.data == "check_sub")
async def check_sub_callback(callback: CallbackQuery, state: FSMContext):
    user = callback.from_user
    if not user.username:
        await callback.answer("Сначала установите @username", show_alert=True)
        return
    if await check_subscription(user.id, bot_data.default_channel_id):
        await callback.answer("✅ Подписка подтверждена!", show_alert=True)
        await callback.message.delete()
        await cmd_start(callback.message, state)
    else:
        await callback.answer("❌ Вы ещё не подписались.", show_alert=True)

# ===================== РЕГИСТРАЦИЯ ЛОТО =====================
async def start_loto_registration(message: Message, state: FSMContext):
    deadline = bot_data.loto_deadline
    if deadline and now_novosibirsk() > deadline:
        await message.answer("⏰ Прием карточек окончен ⛔")
        return
    uname = message.from_user.username
    if uname in bot_data.banned_users:
        until = bot_data.banned_users[uname]
        if now_novosibirsk() < until:
            remaining = (until - now_novosibirsk()).days + 1
            await message.answer(
                f"Вы недавно победили в одной из игр, после выигрыша вы пропускаете "
                f"одно «Лото» и один «Морской бой». Доступ восстановится автоматически через {remaining} дн."
            )
            return
        else:
            del bot_data.banned_users[uname]
            bot_data.save()
    greeting = bot_data.loto_start_message or "🎫 Лото. Отправьте 15 чисел от 1 до 99 или «Да» для генерации случайных."
    reply_markup = common_reply_keyboard()
    await message.answer(greeting, reply_markup=reply_markup)
    await state.set_state(LotoRegistration.waiting_for_numbers)

@dp.message(StateFilter(LotoRegistration.waiting_for_numbers), F.chat.type == "private")
async def loto_numbers(message: Message, state: FSMContext):
    if not message.from_user.username: return
    if message.text:
        if message.text in bot_data.loto_user_buttons:
            await message.answer(bot_data.loto_user_buttons[message.text])
            return
        if message.text == "🟢 ЗАПИСЬ НА ИГРУ 🟢":
            await state.clear()
            await start_loto_registration(message, state)
            return
    if not message.text: return
    if message.text.strip().lower() == "да":
        nums = [str(n) for n in random.sample(range(1, 100), 15)]
    else:
        parts = re.split(r'[,\s]+', message.text.strip())
        try:
            nums = [int(p) for p in parts if p.isdigit()]
        except ValueError:
            await message.answer("Только числа.")
            return
        if len(nums) != 15:
            await message.answer(f"Нужно 15 чисел, у вас {len(nums)}.")
            return
        if any(n < 1 or n > 99 for n in nums):
            await message.answer("Числа от 1 до 99.")
            return
        if len(set(nums)) != 15:
            await message.answer("Повторы запрещены.")
            return
        nums = [str(n) for n in nums]
    user_data = LotoUserData(message.from_user.username, nums)
    user_data.registered_at = now_novosibirsk()
    bot_data.loto_users[message.from_user.id] = user_data
    bot_data.registered_ever.add(message.from_user.id)
    bot_data.save()
    await send_loto_card_and_notify(message.from_user.id, message.from_user.username, nums, user_data.registered_at)
    await message.answer("✅ Карточка принята. Удачи в игре!")
    await state.clear()

# ===================== РЕГИСТРАЦИЯ МОРСКОГО БОЯ =====================
async def start_sea_battle_registration(message: Message, state: FSMContext):
    deadline = bot_data.sea_battle_deadline
    if deadline and now_novosibirsk() > deadline:
        await message.answer("⏰ Прием карточек окончен ⛔")
        return
    uname = message.from_user.username
    if uname in bot_data.banned_users:
        until = bot_data.banned_users[uname]
        if now_novosibirsk() < until:
            remaining = (until - now_novosibirsk()).days + 1
            await message.answer(
                f"Вы недавно победили в одной из игр, после выигрыша вы пропускаете "
                f"одно «Лото» и один «Морской бой». Доступ восстановится автоматически через {remaining} дн."
            )
            return
        else:
            del bot_data.banned_users[uname]
            bot_data.save()
    greeting = bot_data.sea_battle_start_message or "⚓ Морской бой. Отправьте 15 клеток (например, А1 Б4 В3 …) или «Да» для случайной расстановки."
    reply_markup = common_reply_keyboard()
    await message.answer(greeting, reply_markup=reply_markup)
    await state.set_state(SeaBattleRegistration.waiting_for_cells)

@dp.message(StateFilter(SeaBattleRegistration.waiting_for_cells), F.chat.type == "private")
async def sea_battle_cells(message: Message, state: FSMContext):
    if not message.from_user.username: return
    if message.text:
        if message.text in bot_data.sea_battle_user_buttons:
            await message.answer(bot_data.sea_battle_user_buttons[message.text])
            return
        if message.text == "🟢 ЗАПИСЬ НА ИГРУ 🟢":
            await state.clear()
            await start_sea_battle_registration(message, state)
            return
    if not message.text: return
    text = message.text.strip()
    if text.lower() == "да":
        try:
            raw_cells = generate_random_sea_battle_cells()
        except Exception as e:
            await message.answer("❌ Не удалось сгенерировать расстановку, попробуйте ещё раз или введите вручную.")
            return
        cells_set = parse_cells(raw_cells)
        if not validate_ships(cells_set):
            await message.answer("❌ Сгенерированная расстановка не прошла проверку. Попробуйте ещё раз.")
            return
    else:
        raw_cells = parse_list(text)
        if len(raw_cells) != 15:
            await message.answer(f"Нужно ровно 15 клеток. Вы ввели {len(raw_cells)}.")
            return
        cells_set = parse_cells(raw_cells)
        if len(cells_set) != 15:
            await message.answer("Ошибка в обозначениях клеток. Убедитесь, что используете буквы А–К и числа 1–10.")
            return
        if not validate_ships(cells_set):
            await message.answer(
                "❌ Неверная расстановка кораблей. Проверьте правила:\n"
                "• 4×однопалубных, 2×двухпалубных, 1×трёхпалубный, 1×четырёхпалубный\n"
                "• Корабли не касаются друг друга\n"
                "• Все корабли должны быть прямыми линиями.\n"
                "Исправьте ошибки и нажмите /start для повторной регистрации."
            )
            return
        raw_cells = text.split()
    user_data = SeaBattleUserData(message.from_user.username, raw_cells)
    user_data.registered_at = now_novosibirsk()
    bot_data.sea_battle_users[message.from_user.id] = user_data
    bot_data.registered_ever.add(message.from_user.id)
    bot_data.save()
    await send_sea_battle_card(message.from_user.id, message.from_user.username, raw_cells, user_data.registered_at)
    await message.answer("✅ Карточка принята. Удачи в игре!")
    await state.clear()
    
# ===================== ОСНОВНЫЕ КНОПКИ МЕНЮ =====================
@dp.callback_query(F.data.startswith("menu_"))
async def menu_callback(callback: CallbackQuery, state: FSMContext):
    try:
        if not is_admin(callback.from_user.id): await callback.answer("⛔"); return
        cmd = callback.data[5:]
        chat_id = bot_data.default_chat_id
        await callback.answer()
        if cmd == "play":
            if not chat_id: await callback.message.answer("❌ Чат не выбран.")
            elif not await check_chat_access(callback.from_user.id, chat_id): await callback.message.answer("⛔ Нет доступа.")
            else:
                settings = bot_data.chats.get(chat_id)
                if not settings or not settings.items: await callback.message.answer("❌ Список пуст.")
                elif settings.status == "playing": await callback.message.answer("⚠️ Уже идёт.")
                else:
                    was_paused = settings.status == "paused"
                    action = "продолжить" if was_paused else "начать"
                    preview = ", ".join(settings.items[:5])
                    if len(settings.items) > 5: preview += "..."
                    try:
                        chat = await bot.get_chat(chat_id)
                        chat_title = chat.title or "Без названия"
                    except:
                        chat_title = "Неизвестный чат"
                    kb = InlineKeyboardMarkup(inline_keyboard=[
                        [InlineKeyboardButton(text="✅ Да", callback_data=f"confirm_play_{chat_id}"),
                         InlineKeyboardButton(text="❌ Нет", callback_data="cancel")]
                    ])
                    await callback.message.answer(
                        f"▶️ Вы уверены, что хотите {action} диктовку в чате **{chat_title}** (`{chat_id}`)?\n"
                        f"Элементов: {len(settings.items)}. Интервал: {settings.interval} сек.\n"
                        f"Текущий индекс: {settings.index}.\nПревью: {preview}",
                        reply_markup=kb, parse_mode="Markdown"
                    )
        elif cmd == "pause":
            if not chat_id: await callback.message.answer("❌ Чат не выбран.")
            elif not await check_chat_access(callback.from_user.id, chat_id): await callback.message.answer("⛔ Нет доступа.")
            else:
                settings = bot_data.chats.get(chat_id)
                if settings and settings.status == "playing":
                    settings.status = "paused"; bot_data.save(); await callback.message.answer("⏸ Пауза.")
                else: await callback.message.answer("ℹ️ Не идёт.")
        elif cmd == "stop":
            if not chat_id: await callback.message.answer("❌ Чат не выбран.")
            elif not await check_chat_access(callback.from_user.id, chat_id): await callback.message.answer("⛔ Нет доступа.")
            else:
                settings = bot_data.chats.get(chat_id)
                if settings and settings.status in ("playing", "paused"):
                    settings.status = "stopped"; settings.index = 0; bot_data.save()
                    await callback.message.answer("⏹ Остановлена.")
                else: await callback.message.answer("ℹ️ Не идёт.")
        elif cmd == "upload":
            if not chat_id: await callback.message.answer("❌ Чат не выбран.")
            else:
                await callback.message.answer(f"📂 Отправьте список элементов для чата. Элементы можно разделять пробелами, запятыми или с новой строки.")
                await state.set_state(UploadList.waiting_for_list)
        elif cmd == "verify":
            if not chat_id: await callback.message.answer("❌ Чат не выбран.")
            else:
                settings = bot_data.chats.get(chat_id)
                if not settings or settings.status not in ("paused", "playing"): await callback.message.answer("⛔ Игра не активна.")
                else:
                    await callback.message.answer("📝 Отправьте 15 чисел (+ @username).")
                    await state.set_state(VerifyList.waiting_for_verify)
        elif cmd == "dice_toggle":
            if not chat_id: await callback.message.answer("❌ Чат не выбран.")
            else:
                settings = bot_data.chats.setdefault(chat_id, ChatSettings())
                settings.dice_enabled = not settings.dice_enabled; bot_data.save()
                await callback.message.answer(f"🎲 Кубики {'вкл' if settings.dice_enabled else 'выкл'}.", reply_markup=main_menu_keyboard())
        elif cmd == "slot_toggle":
            if not chat_id: await callback.message.answer("❌ Чат не выбран.")
            else:
                settings = bot_data.chats.setdefault(chat_id, ChatSettings())
                settings.slot_enabled = not settings.slot_enabled; bot_data.save()
                await callback.message.answer(f"🎰 Слот {'вкл' if settings.slot_enabled else 'выкл'}.", reply_markup=main_menu_keyboard())
        elif cmd == "loto_toggle":
            if not chat_id: await callback.message.answer("❌ Чат не выбран.")
            else:
                settings = bot_data.chats.setdefault(chat_id, ChatSettings())
                settings.loto_enabled = not settings.loto_enabled; bot_data.save()
                await callback.message.answer(f"🎫 Лото {'вкл' if settings.loto_enabled else 'выкл'}.", reply_markup=main_menu_keyboard())
        elif cmd == "sea_battle_toggle":
            if not chat_id: await callback.message.answer("❌ Чат не выбран.")
            else:
                settings = bot_data.chats.setdefault(chat_id, ChatSettings())
                settings.sea_battle_enabled = not settings.sea_battle_enabled; bot_data.save()
                await callback.message.answer(f"⚓ Морской бой {'вкл' if settings.sea_battle_enabled else 'выкл'}.", reply_markup=main_menu_keyboard())
        elif cmd == "settings":
            await callback.message.answer("⚙️ Настройки", reply_markup=settings_menu_keyboard())
    except TelegramBadRequest as e:
        if "query is too old" in str(e): logging.warning("old callback")
        else: raise

@dp.callback_query(F.data.startswith("confirm_play_"))
async def confirm_play_callback(callback: CallbackQuery):
    if not is_admin(callback.from_user.id): return
    chat_id = int(callback.data.split("_")[2])
    settings = bot_data.chats.get(chat_id)
    if not settings or not settings.items:
        await callback.answer("Список пуст.", show_alert=True); return
    was_paused = settings.status == "paused"
    settings.status = "playing"
    # Автоматически включаем проверку подписки при старте игры
    bot_data.check_subscription = True
    if not was_paused:
        settings.index = 0
        settings.loto_finalists = []; settings.loto_finalists_disabled = False
        settings.sea_battle_finalists = []; settings.sea_battle_finalists_disabled = False
        if settings.dictation_start_message: enqueue_message(chat_id, settings.dictation_start_message)
    bot_data.save()
    await callback.message.delete_reply_markup()
    await callback.message.answer(f"▶️ Диктовка {'продолжена' if was_paused else 'началась'}.")
    asyncio.create_task(dictation_loop(chat_id, settings))
    await callback.answer()

@dp.callback_query(F.data == "cancel")
async def cancel_callback(callback: CallbackQuery):
    await callback.message.delete_reply_markup()
    await callback.message.answer("❌ Действие отменено.", reply_markup=main_menu_keyboard())
    await callback.answer()

# ===================== НАСТРОЙКИ (settings_) =====================
@dp.callback_query(F.data.startswith("settings_"))
async def settings_callback(callback: CallbackQuery, state: FSMContext):
    try:
        if not is_admin(callback.from_user.id): await callback.answer("⛔"); return
        cmd = callback.data[9:]
        await callback.answer()
        if cmd == "my_chat":
            if bot_data.default_chat_id:
                try:
                    chat = await bot.get_chat(bot_data.default_chat_id); title = chat.title or "Без названия"
                except: title = "Неизвестно"
                await callback.message.answer(f"📌 Текущий чат: `{bot_data.default_chat_id}` — {title}\nЧтобы изменить, отправьте новый ID чата или перешлите сообщение из группы.", parse_mode="Markdown")
            else:
                await callback.message.answer("❌ Чат не установлен. Отправьте ID чата или перешлите сообщение из группы.")
            await state.set_state(SetChatIdState.waiting_for_chat_id)
        elif cmd == "my_channel":
            if bot_data.default_channel_id:
                try:
                    chat = await bot.get_chat(bot_data.default_channel_id); name = f"@{chat.username}" if chat.username else chat.title
                except: name = "Неизвестный канал"
                await callback.message.answer(f"📢 Текущий канал: `{bot_data.default_channel_id}` — {name}\nЧтобы изменить, отправьте ID канала или @username.", parse_mode="Markdown")
            else:
                await callback.message.answer("❌ Канал не задан. Отправьте ID канала или @username.")
            await state.set_state(SetChannelIdState.waiting_for_channel_id)
        elif cmd == "my_interval":
            await callback.message.answer("Введите новый интервал в секундах:")
            await state.set_state(SetIntervalState.waiting_for_interval)
        elif cmd == "invite_link":
            await callback.message.answer("Отправьте ссылку приглашения:")
            await state.set_state(SetInviteLinkState.waiting_for_link)
        elif cmd == "sub_check":
            bot_data.check_subscription = not bot_data.check_subscription; bot_data.save()
            status = "включена" if bot_data.check_subscription else "выключена"
            await callback.message.answer(f"🔔 Проверка подписки {status}.")
            await callback.message.answer("⚙️ Настройки", reply_markup=settings_menu_keyboard())
        elif cmd == "start_msg":
            await callback.message.answer("Отправьте текст стартового сообщения диктовки:")
            await state.set_state(DictationStartMessage.waiting_for_start_message)
        elif cmd == "loto":
            await callback.message.answer("🎫 Настройки Лото", reply_markup=loto_menu_keyboard())
        elif cmd == "sea_battle":
            await callback.message.answer("⚓ Настройки Морского боя", reply_markup=sea_battle_menu_keyboard())
        elif cmd == "chats":
            await show_chats_message(callback.message, callback.from_user.id)
        elif cmd == "finalists_loto":
            await show_finalists_list_msg(callback.message, "loto")
        elif cmd == "finalists_sea":
            await show_finalists_list_msg(callback.message, "sea")
        elif cmd == "back":
            await callback.message.answer("Главное меню", reply_markup=main_menu_keyboard())
        elif cmd == "admin":
            if not is_main_admin(callback.from_user.id): await callback.answer("Только главный админ.", show_alert=True)
            else: await callback.message.answer("👑 Администрирование", reply_markup=admin_menu_keyboard())
    except TelegramBadRequest as e:
        if "query is too old" in str(e): logging.warning("old callback")
        else: raise

# ---------- ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ НАСТРОЕК ----------
async def show_chats_message(target: Message, user_id: int):
    builder = InlineKeyboardBuilder()
    txt = ""
    for cid, settings in bot_data.chats.items():
        if not await check_chat_access(user_id, cid): continue
        try:
            chat = await bot.get_chat(cid); title = chat.title or "Без названия"
        except: title = "Неизвестный чат"
        txt += f"• `{cid}` — {title} [{settings.status}]\n"
        builder.button(text=f"✅ {title[:20]}", callback_data=f"selectchat_{cid}")
    if not txt:
        await target.answer("Нет доступных чатов.")
        return
    builder.adjust(1)
    await target.answer("📋 Доступные чаты:\n" + txt, reply_markup=builder.as_markup())

async def show_finalists_list_msg(message: Message, game: str):
    chat_id = bot_data.default_chat_id
    if not chat_id: await message.answer("❌ Чат не выбран."); return
    settings = bot_data.chats.get(chat_id)
    if not settings: await message.answer("Чат не настроен."); return
    finalists = settings.loto_finalists if game == "loto" else settings.sea_battle_finalists
    if not finalists:
        await message.answer("📭 Список финалистов пуст.")
        return
    digits = ["1️⃣","2️⃣","3️⃣","4️⃣","5️⃣","6️⃣","7️⃣","8️⃣","9️⃣","🔟"]
    lines = []
    for i, username in enumerate(finalists):
        num = digits[i] if i < 10 else f"{i+1}️⃣"
        lines.append(f"{num} @{username}")
    await message.answer("📋 Список финалистов:\n" + "\n".join(lines))

# ---------- ЛОТО АДМИНКА ----------
@dp.callback_query(F.data.startswith("loto_"))
async def loto_admin_callback(callback: CallbackQuery, state: FSMContext):
    try:
        if not is_admin(callback.from_user.id): await callback.answer("⛔", show_alert=True); return
        cmd = callback.data[5:]
        await callback.answer()
        if cmd == "chat":
            await callback.message.answer("Отправьте ID чата для карточек Лото:")
            await state.set_state(LotoSettingsFlow.waiting_for_target_chat)
        elif cmd == "deadline":
            await callback.message.answer("Введите дату и время (ДД.ММ.ГГГГ ЧЧ:ММ Нск):")
            await state.set_state(LotoSettingsFlow.waiting_for_deadline)
        elif cmd == "start_msg":
            await callback.message.answer("Отправьте текст приветствия Лото:")
            await state.set_state(LotoSettingsFlow.waiting_for_start_message)
        elif cmd == "buttons":
            await callback.message.answer("Формат: Кнопка1=Ответ1, Кнопка2=Ответ2")
            await state.set_state(LotoSettingsFlow.waiting_for_buttons)
        elif cmd == "bans":
            await callback.message.answer("Формат: @username дни (или remove @username)")
            await state.set_state(LotoSettingsFlow.waiting_for_bans)
        elif cmd == "finalists":
            await callback.message.answer("Введите количество финалистов Лото:")
            await state.set_state(LotoSettingsFlow.waiting_for_finalists)
        elif cmd == "toggle_trigger":
            settings = bot_data.chats.get(bot_data.default_chat_id) if bot_data.default_chat_id else None
            if settings:
                settings.loto_trigger_enabled = not settings.loto_trigger_enabled; bot_data.save()
                status = "включен" if settings.loto_trigger_enabled else "выключен"
                await callback.message.answer(f"⚡ Триггер Лото {status}.", reply_markup=loto_menu_keyboard())
            else: await callback.message.answer("❌ Чат не выбран.")
        elif cmd == "triggers":
            await callback.message.answer("Введите слова-триггеры (каждое слово с новой строки или через запятую):")
            await state.set_state(LotoTriggerInput.waiting_for_triggers)
        elif cmd == "current":
            await show_current_loto_settings(callback.message)
        elif cmd == "stats":
            current = len(bot_data.loto_users); total = len(bot_data.registered_ever)
            await callback.message.answer(f"📊 Лото: текущая игра — {current}, всего участников — {total}")
        elif cmd == "broadcast":
            await callback.message.answer("Отправьте сообщение для рассылки всем участникам.")
            await state.set_state(LotoBroadcast.waiting_for_message)
        elif cmd == "back":
            await callback.message.answer("⚙️ Настройки", reply_markup=settings_menu_keyboard())
    except TelegramBadRequest as e:
        if "query is too old" in str(e): logging.warning("old callback")
        else: raise

async def show_current_loto_settings(message: Message):
    chat_id = bot_data.default_chat_id
    settings = bot_data.chats.get(chat_id) if chat_id else None
    text = "📋 Текущие настройки Лото:\n"
    text += f"Чат для карточек: {bot_data.loto_target_chat or 'не задан'}\n"
    text += f"Дедлайн: {bot_data.loto_deadline.strftime('%d.%m.%Y %H:%M') if bot_data.loto_deadline else 'не задан'}\n"
    text += f"Стартовое сообщение: {bot_data.loto_start_message or 'стандартное'}\n"
    text += f"Кнопки: {', '.join(bot_data.loto_user_buttons.keys()) if bot_data.loto_user_buttons else 'нет'}\n"
    text += f"Количество финалистов: {settings.loto_finalists_count if settings else 'не задано'}\n"
    text += f"Триггер включён: {'да' if (settings and settings.loto_trigger_enabled) else 'нет'}\n"
    text += f"Слова-триггеры: {', '.join(settings.loto_triggers) if settings else 'не заданы'}"
    await message.answer(text, reply_markup=loto_menu_keyboard())

# ---------- МОРСКОЙ БОЙ АДМИНКА ----------
@dp.callback_query(F.data.startswith("sb_"))
async def sea_battle_admin_callback(callback: CallbackQuery, state: FSMContext):
    try:
        if not is_admin(callback.from_user.id): await callback.answer("⛔", show_alert=True); return
        cmd = callback.data[3:]
        await callback.answer()
        if cmd == "chat":
            await callback.message.answer("Отправьте ID чата для карточек Морского боя:")
            await state.set_state(SeaBattleSettingsFlow.waiting_for_target_chat)
        elif cmd == "deadline":
            await callback.message.answer("Введите дату и время (ДД.ММ.ГГГГ ЧЧ:ММ Нск):")
            await state.set_state(SeaBattleSettingsFlow.waiting_for_deadline)
        elif cmd == "start_msg":
            await callback.message.answer("Отправьте текст приветствия Морского боя:")
            await state.set_state(SeaBattleSettingsFlow.waiting_for_start_message)
        elif cmd == "buttons":
            await callback.message.answer("Формат: Кнопка1=Ответ1, Кнопка2=Ответ2")
            await state.set_state(SeaBattleSettingsFlow.waiting_for_buttons)
        elif cmd == "bans":
            await callback.message.answer("Формат: @username дни (или remove @username)")
            await state.set_state(SeaBattleSettingsFlow.waiting_for_bans)
        elif cmd == "finalists":
            await callback.message.answer("Введите количество финалистов МБ:")
            await state.set_state(SeaBattleSettingsFlow.waiting_for_finalists)
        elif cmd == "toggle_trigger":
            settings = bot_data.chats.get(bot_data.default_chat_id) if bot_data.default_chat_id else None
            if settings:
                settings.sea_battle_trigger_enabled = not settings.sea_battle_trigger_enabled; bot_data.save()
                status = "включен" if settings.sea_battle_trigger_enabled else "выключен"
                await callback.message.answer(f"⚡ Триггер МБ {status}.", reply_markup=sea_battle_menu_keyboard())
            else: await callback.message.answer("❌ Чат не выбран.")
        elif cmd == "triggers":
            await callback.message.answer("Введите слова-триггеры (каждое слово с новой строки или через запятую):")
            await state.set_state(SeaBattleTriggerInput.waiting_for_triggers)
        elif cmd == "current":
            await show_current_sea_battle_settings(callback.message)
        elif cmd == "stats":
            current = len(bot_data.sea_battle_users); total = len(bot_data.registered_ever)
            await callback.message.answer(f"📊 Морской бой: текущая игра — {current}, всего участников — {total}")
        elif cmd == "broadcast":
            await callback.message.answer("Отправьте сообщение для рассылки всем участникам.")
            await state.set_state(SeaBattleBroadcast.waiting_for_message)
        elif cmd == "back":
            await callback.message.answer("⚙️ Настройки", reply_markup=settings_menu_keyboard())
    except TelegramBadRequest as e:
        if "query is too old" in str(e): logging.warning("old callback")
        else: raise

async def show_current_sea_battle_settings(message: Message):
    chat_id = bot_data.default_chat_id
    settings = bot_data.chats.get(chat_id) if chat_id else None
    text = "📋 Текущие настройки Морского боя:\n"
    text += f"Чат для карточек: {bot_data.sea_battle_target_chat or 'не задан'}\n"
    text += f"Дедлайн: {bot_data.sea_battle_deadline.strftime('%d.%m.%Y %H:%M') if bot_data.sea_battle_deadline else 'не задан'}\n"
    text += f"Стартовое сообщение: {bot_data.sea_battle_start_message or 'стандартное'}\n"
    text += f"Кнопки: {', '.join(bot_data.sea_battle_user_buttons.keys()) if bot_data.sea_battle_user_buttons else 'нет'}\n"
    text += f"Количество финалистов: {settings.sea_battle_finalists_count if settings else 'не задано'}\n"
    text += f"Триггер включён: {'да' if (settings and settings.sea_battle_trigger_enabled) else 'нет'}\n"
    text += f"Слова-триггеры: {', '.join(settings.sea_battle_triggers) if settings else 'не заданы'}"
    await message.answer(text, reply_markup=sea_battle_menu_keyboard())

# ---------- FSM ДЛЯ НАСТРОЕК ----------
@dp.message(StateFilter(SetChatIdState.waiting_for_chat_id), F.chat.type == "private")
async def set_chat_id(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id): await state.clear(); return
    if message.forward_from_chat and message.forward_from_chat.type in ("group", "supergroup"):
        chat_id = message.forward_from_chat.id
    else:
        try: chat_id = int(message.text.strip())
        except ValueError:
            await message.answer("❌ Отправьте ID чата или перешлите сообщение из группы.")
            return
    if not await check_chat_access(message.from_user.id, chat_id):
        await message.answer("⛔ Нет доступа к этому чату.")
        await state.clear()
        return
    bot_data.default_chat_id = chat_id
    bot_data.chats.setdefault(chat_id, ChatSettings())
    bot_data.save()
    await message.answer(f"✅ Чат установлен: {chat_id}", reply_markup=main_menu_keyboard())
    await state.clear()

@dp.message(StateFilter(SetChannelIdState.waiting_for_channel_id), F.chat.type == "private")
async def set_channel_id(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id): await state.clear(); return
    try:
        if message.text.startswith('@'):
            chat = await bot.get_chat(message.text.strip())
            channel_id = chat.id
        else:
            channel_id = int(message.text.strip())
        bot_data.default_channel_id = channel_id; bot_data.save()
        await message.answer(f"✅ Канал установлен: {channel_id}", reply_markup=main_menu_keyboard())
    except Exception as e: await message.answer(f"❌ Ошибка: {e}")
    await state.clear()

@dp.message(StateFilter(SetIntervalState.waiting_for_interval), F.chat.type == "private")
async def set_interval_input(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id): await state.clear(); return
    try:
        interval = float(message.text.strip())
        if interval <= 0: raise ValueError
    except: await message.answer("❌ Введите положительное число."); return
    settings = bot_data.chats.get(bot_data.default_chat_id) if bot_data.default_chat_id else None
    if settings: settings.interval = interval; bot_data.save()
    await message.answer(f"✅ Интервал установлен: {interval} сек.", reply_markup=main_menu_keyboard())
    await state.clear()

@dp.message(StateFilter(DictationStartMessage.waiting_for_start_message), F.chat.type == "private")
async def set_start_msg_input(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id): await state.clear(); return
    settings = bot_data.chats.get(bot_data.default_chat_id) if bot_data.default_chat_id else None
    if settings: settings.dictation_start_message = message.text.strip() or None; bot_data.save()
    await message.answer("✅ Стартовое сообщение сохранено.", reply_markup=main_menu_keyboard())
    await state.clear()

@dp.message(StateFilter(SetInviteLinkState.waiting_for_link), F.chat.type == "private")
async def set_invite_link_input(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id): await state.clear(); return
    bot_data.channel_invite_link = message.text.strip(); bot_data.save()
    await message.answer("✅ Ссылка приглашения сохранена.", reply_markup=main_menu_keyboard())
    await state.clear()

# FSM для настроек Лото
@dp.message(StateFilter(LotoSettingsFlow.waiting_for_target_chat), F.chat.type == "private")
async def set_loto_chat(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id): await state.clear(); return
    try:
        cid = int(message.text.strip())
        bot_data.loto_target_chat = cid; bot_data.save()
        await message.answer(f"✅ Чат для карточек Лото: {cid}", reply_markup=loto_menu_keyboard())
    except: await message.answer("Ошибка.")
    await state.clear()

@dp.message(StateFilter(LotoSettingsFlow.waiting_for_deadline), F.chat.type == "private")
async def set_loto_deadline(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id): await state.clear(); return
    try:
        dt = datetime.strptime(message.text.strip(), "%d.%m.%Y %H:%M")
        bot_data.loto_deadline = dt; bot_data.save()
        await message.answer(f"✅ Дедлайн Лото: {dt.strftime('%d.%m.%Y %H:%M')} Нск", reply_markup=loto_menu_keyboard())
    except: await message.answer("Неверный формат.")
    await state.clear()

@dp.message(StateFilter(LotoSettingsFlow.waiting_for_start_message), F.chat.type == "private")
async def set_loto_start_msg(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id): await state.clear(); return
    bot_data.loto_start_message = message.text.strip() or None; bot_data.save()
    await message.answer("✅ Стартовое сообщение Лото обновлено.", reply_markup=loto_menu_keyboard())
    await state.clear()

@dp.message(StateFilter(LotoSettingsFlow.waiting_for_buttons), F.chat.type == "private")
async def set_loto_buttons(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id): await state.clear(); return
    raw = message.text.strip()
    new_buttons = {}
    if raw:
        for pair in raw.split(','):
            if '=' in pair: k, v = pair.split('=', 1); new_buttons[k.strip()] = v.strip()
            else: new_buttons[pair.strip()] = pair.strip()
    bot_data.loto_user_buttons = new_buttons; bot_data.save()
    await message.answer("✅ Кнопки Лото сохранены.", reply_markup=loto_menu_keyboard())
    await state.clear()

@dp.message(StateFilter(LotoSettingsFlow.waiting_for_bans), F.chat.type == "private")
async def set_loto_bans(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id): await state.clear(); return
    lines = message.text.strip().splitlines()
    updated = 0
    for line in lines:
        if not line: continue
        if line.startswith("remove "):
            uname = line.split(maxsplit=1)[1].strip().lstrip('@')
            if uname in bot_data.banned_users: del bot_data.banned_users[uname]; updated += 1
            continue
        match = re.match(r'@?(\w+)\s+(\d+)', line)
        if match:
            uname = match.group(1); days = int(match.group(2))
            bot_data.banned_users[uname] = now_novosibirsk() + timedelta(days=days); updated += 1
    bot_data.save()
    await message.answer(f"✅ Обработано записей: {updated}", reply_markup=loto_menu_keyboard())
    await state.clear()

@dp.message(StateFilter(LotoSettingsFlow.waiting_for_finalists), F.chat.type == "private")
async def set_loto_finalists(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id): await state.clear(); return
    try:
        count = int(message.text.strip())
        if count < 1: raise ValueError
        chat_id = bot_data.default_chat_id
        if not chat_id: await message.answer("❌ Сначала выберите чат в настройках."); return
        settings = bot_data.chats[chat_id]
        settings.loto_finalists_count = count; bot_data.save()
        await message.answer(f"✅ Количество финалистов Лото: {count}", reply_markup=loto_menu_keyboard())
    except: await message.answer("❌ Введите целое число больше 0.")
    await state.clear()

@dp.message(StateFilter(LotoTriggerInput.waiting_for_triggers), F.chat.type == "private")
async def set_loto_triggers(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id): await state.clear(); return
    raw = message.text.strip()
    triggers = [t.strip().lower() for t in raw.replace(',', '\n').splitlines() if t.strip()]
    if not triggers: await message.answer("❌ Введите хотя бы один триггер."); return
    chat_id = bot_data.default_chat_id
    if not chat_id: await message.answer("❌ Сначала выберите чат в настройках."); return
    settings = bot_data.chats[chat_id]
    settings.loto_triggers = triggers; bot_data.save()
    await message.answer(f"✅ Триггеры Лото сохранены: {', '.join(triggers)}", reply_markup=loto_menu_keyboard())
    await state.clear()

# FSM для настроек Морского боя
@dp.message(StateFilter(SeaBattleSettingsFlow.waiting_for_target_chat), F.chat.type == "private")
async def set_sb_chat(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id): await state.clear(); return
    try:
        cid = int(message.text.strip())
        bot_data.sea_battle_target_chat = cid; bot_data.save()
        await message.answer(f"✅ Чат для карточек МБ: {cid}", reply_markup=sea_battle_menu_keyboard())
    except: await message.answer("Ошибка.")
    await state.clear()

@dp.message(StateFilter(SeaBattleSettingsFlow.waiting_for_deadline), F.chat.type == "private")
async def set_sb_deadline(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id): await state.clear(); return
    try:
        dt = datetime.strptime(message.text.strip(), "%d.%m.%Y %H:%M")
        bot_data.sea_battle_deadline = dt; bot_data.save()
        await message.answer(f"✅ Дедлайн МБ: {dt.strftime('%d.%m.%Y %H:%M')} Нск", reply_markup=sea_battle_menu_keyboard())
    except: await message.answer("Неверный формат.")
    await state.clear()

@dp.message(StateFilter(SeaBattleSettingsFlow.waiting_for_start_message), F.chat.type == "private")
async def set_sb_start_msg(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id): await state.clear(); return
    bot_data.sea_battle_start_message = message.text.strip() or None; bot_data.save()
    await message.answer("✅ Стартовое сообщение МБ обновлено.", reply_markup=sea_battle_menu_keyboard())
    await state.clear()

@dp.message(StateFilter(SeaBattleSettingsFlow.waiting_for_buttons), F.chat.type == "private")
async def set_sb_buttons(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id): await state.clear(); return
    raw = message.text.strip()
    new_buttons = {}
    if raw:
        for pair in raw.split(','):
            if '=' in pair: k, v = pair.split('=', 1); new_buttons[k.strip()] = v.strip()
            else: new_buttons[pair.strip()] = pair.strip()
    bot_data.sea_battle_user_buttons = new_buttons; bot_data.save()
    await message.answer("✅ Кнопки МБ сохранены.", reply_markup=sea_battle_menu_keyboard())
    await state.clear()

@dp.message(StateFilter(SeaBattleSettingsFlow.waiting_for_bans), F.chat.type == "private")
async def set_sb_bans(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id): await state.clear(); return
    lines = message.text.strip().splitlines()
    updated = 0
    for line in lines:
        if not line: continue
        if line.startswith("remove "):
            uname = line.split(maxsplit=1)[1].strip().lstrip('@')
            if uname in bot_data.banned_users: del bot_data.banned_users[uname]; updated += 1
            continue
        match = re.match(r'@?(\w+)\s+(\d+)', line)
        if match:
            uname = match.group(1); days = int(match.group(2))
            bot_data.banned_users[uname] = now_novosibirsk() + timedelta(days=days); updated += 1
    bot_data.save()
    await message.answer(f"✅ Обработано записей: {updated}", reply_markup=sea_battle_menu_keyboard())
    await state.clear()

@dp.message(StateFilter(SeaBattleSettingsFlow.waiting_for_finalists), F.chat.type == "private")
async def set_sb_finalists(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id): await state.clear(); return
    try:
        count = int(message.text.strip())
        if count < 1: raise ValueError
        chat_id = bot_data.default_chat_id
        if not chat_id: await message.answer("❌ Сначала выберите чат в настройках."); return
        settings = bot_data.chats[chat_id]
        settings.sea_battle_finalists_count = count; bot_data.save()
        await message.answer(f"✅ Количество финалистов МБ: {count}", reply_markup=sea_battle_menu_keyboard())
    except: await message.answer("❌ Введите целое число больше 0.")
    await state.clear()

@dp.message(StateFilter(SeaBattleTriggerInput.waiting_for_triggers), F.chat.type == "private")
async def set_sb_triggers(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id): await state.clear(); return
    raw = message.text.strip()
    triggers = [t.strip().lower() for t in raw.replace(',', '\n').splitlines() if t.strip()]
    if not triggers: await message.answer("❌ Введите хотя бы один триггер."); return
    chat_id = bot_data.default_chat_id
    if not chat_id: await message.answer("❌ Сначала выберите чат в настройках."); return
    settings = bot_data.chats[chat_id]
    settings.sea_battle_triggers = triggers; bot_data.save()
    await message.answer(f"✅ Триггеры МБ сохранены: {', '.join(triggers)}", reply_markup=sea_battle_menu_keyboard())
    await state.clear()

# ---------- РАССЫЛКИ ----------
@dp.message(StateFilter(LotoBroadcast.waiting_for_message), F.chat.type == "private")
async def loto_broadcast_send(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id): return
    users = list(bot_data.registered_ever)
    if not users: await message.answer("Нет пользователей."); await state.clear(); return
    succ, fail = 0, 0
    for uid in users:
        try:
            await bot.copy_message(uid, message.chat.id, message.message_id)
            succ += 1
        except Exception as e: logging.error(f"Ошибка отправки {uid}: {e}"); fail += 1
        await asyncio.sleep(0.5)
    await message.answer(f"✅ Рассылка завершена. Успешно: {succ}, ошибок: {fail}", reply_markup=loto_menu_keyboard())
    await state.clear()

@dp.message(StateFilter(SeaBattleBroadcast.waiting_for_message), F.chat.type == "private")
async def sea_battle_broadcast_send(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id): return
    users = list(bot_data.registered_ever)
    if not users: await message.answer("Нет пользователей."); await state.clear(); return
    succ, fail = 0, 0
    for uid in users:
        try:
            await bot.copy_message(uid, message.chat.id, message.message_id)
            succ += 1
        except Exception as e: logging.error(f"Ошибка отправки {uid}: {e}"); fail += 1
        await asyncio.sleep(0.5)
    await message.answer(f"✅ Рассылка завершена. Успешно: {succ}, ошибок: {fail}", reply_markup=sea_battle_menu_keyboard())
    await state.clear()

# ---------- АДМИНИСТРИРОВАНИЕ ----------
@dp.callback_query(F.data.startswith("admin_"))
async def admin_callback(callback: CallbackQuery, state: FSMContext):
    try:
        if not is_main_admin(callback.from_user.id): await callback.answer("⛔", show_alert=True); return
        action = callback.data[6:]
        await callback.answer()
        if action == "back":
            await callback.message.answer("⚙️ Настройки", reply_markup=settings_menu_keyboard())
        elif action == "add":
            await callback.message.answer("Введите user_id нового администратора:")
            await state.set_state(AdminAdd.waiting_for_id)
        elif action == "remove":
            await callback.message.answer("Введите user_id для удаления:")
            await state.set_state(AdminRemove.waiting_for_id)
        elif action == "list":
            admins = sorted(bot_data.admins)
            text = "👥 Список администраторов:\n" + "\n".join(f"• `{uid}`" for uid in admins)
            await callback.message.answer(text, parse_mode="Markdown")
        elif action == "bans":
            await callback.message.answer("Формат: @username дни (или remove @username)")
            await state.set_state(LotoSettingsFlow.waiting_for_bans)
    except TelegramBadRequest as e:
        if "query is too old" in str(e): logging.warning("old callback")
        else: raise

@dp.message(StateFilter(AdminAdd.waiting_for_id), F.chat.type == "private")
async def add_admin_input(message: Message, state: FSMContext):
    try: new_id = int(message.text.strip())
    except: await message.answer("❌ Неверный формат."); return
    if new_id in bot_data.admins: await message.answer("ℹ️ Уже администратор.")
    else: bot_data.admins.add(new_id); bot_data.save(); await message.answer(f"✅ Добавлен {new_id}.", reply_markup=main_menu_keyboard())
    await state.clear()

@dp.message(StateFilter(AdminRemove.waiting_for_id), F.chat.type == "private")
async def remove_admin_input(message: Message, state: FSMContext):
    try: remove_id = int(message.text.strip())
    except: await message.answer("❌ Неверный формат."); return
    if remove_id == MAIN_ADMIN_ID: await message.answer("❌ Нельзя удалить главного."); await state.clear(); return
    if remove_id not in bot_data.admins: await message.answer("ℹ️ Не является админом.")
    else: bot_data.admins.remove(remove_id); bot_data.save(); await message.answer(f"✅ Удалён {remove_id}.", reply_markup=main_menu_keyboard())
    await state.clear()

# ===================== ЗАГРУЗКА СПИСКА ДИКТОВКИ =====================
@dp.message(StateFilter(UploadList.waiting_for_list), F.chat.type == "private")
async def process_upload(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        await state.clear()
        return
    chat_id = bot_data.default_chat_id
    if not chat_id:
        await message.answer("❌ Чат не выбран.")
        await state.clear()
        return
    items = parse_list(message.text)
    if not items:
        await message.answer("❌ Не удалось распарсить список. Попробуйте ещё раз.")
        return
    if len(items) != len(set(items)):
        await message.answer("❌ Список содержит повторяющиеся элементы. Повторы запрещены.")
        return
    settings = bot_data.chats.setdefault(chat_id, ChatSettings())
    settings.items = items
    settings.index = 0
    bot_data.save()
    await message.answer(f"✅ Список загружен: {len(items)} элементов.", reply_markup=main_menu_keyboard())
    await state.clear()

# ===================== РУЧНАЯ ПРОВЕРКА (РЕГИСТРОНЕЗАВИСИМАЯ) =====================
@dp.message(StateFilter(VerifyList.waiting_for_verify), F.chat.type == "private")
async def process_verify(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        await state.clear()
        return
    if not message.text:
        await message.answer("Пожалуйста, отправьте текстовое сообщение.")
        return
    chat_id = bot_data.default_chat_id
    settings = bot_data.chats.get(chat_id)
    if not settings or settings.status != "paused":
        await message.answer("⛔ Проверка возможна только на паузе диктовки.")
        await state.clear()
        return
    text = message.text.strip()
    parts = parse_list(text)
    numbers = []
    username = None
    for p in parts:
        if p.startswith('@'):
            username = p.lstrip('@')
        else:
            numbers.append(p)
    if len(numbers) != 15:
        await message.answer(f"❌ Отправлено {len(numbers)} элементов, а нужно ровно 15. Повторите ввод.")
        return
    dictated_upper = [item.upper() for item in settings.items[:settings.index]]
    missing = [n for n in numbers if str(n).upper() not in dictated_upper]
    if not missing:
        res = "✅ Карточка верна"
        if username:
            if settings.loto_enabled and not settings.loto_finalists_disabled:
                if username not in settings.loto_finalists:
                    settings.loto_finalists.append(username)
                    bot_data.save()
                    place = len(settings.loto_finalists)
                    res += f" | ✅ @{username} добавлен в финалисты Лото (№{place})"
                    if place >= settings.loto_finalists_count:
                        settings.loto_finalists_disabled = True
                        bot_data.save()
                        await send_finalists_list(chat_id, settings, "loto")
                        res += "\n🏁 Набрано нужное количество финалистов Лото."
            if settings.sea_battle_enabled and not settings.sea_battle_finalists_disabled:
                if username not in settings.sea_battle_finalists:
                    settings.sea_battle_finalists.append(username)
                    bot_data.save()
                    place = len(settings.sea_battle_finalists)
                    res += f" | ✅ @{username} добавлен в финалисты МБ (№{place})"
                    if place >= settings.sea_battle_finalists_count:
                        settings.sea_battle_finalists_disabled = True
                        bot_data.save()
                        await send_finalists_list(chat_id, settings, "sea_battle")
                        res += "\n🏁 Набрано нужное количество финалистов МБ."
    else:
        res = f"❌ Карточка неверна. Не было: {', '.join(missing)}"
    if username:
        res += f" (участник @{username})"
    await message.answer(res, reply_markup=main_menu_keyboard())
    await state.clear()

# ===================== ГРУППОВЫЕ ФУНКЦИИ =====================
async def mute_user(chat_id, user_id, duration, chat_settings):
    try:
        await bot.restrict_chat_member(
            chat_id, user_id,
            ChatPermissions(can_send_messages=False),
            until_date=datetime.now() + duration
        )
        chat_settings.muted_users.append(user_id)
        bot_data.save()
    except Exception as e:
        logging.error(f"Mute error: {e}")

async def unmute_all(chat_id, chat_settings):
    for uid in chat_settings.muted_users[:]:
        try:
            await bot.restrict_chat_member(
                chat_id, uid,
                ChatPermissions(
                    can_send_messages=True,
                    can_send_media_messages=True,
                    can_send_other_messages=True,
                    can_add_web_page_previews=True
                )
            )
            chat_settings.muted_users.remove(uid)
        except Exception as e:
            logging.error(f"Unmute error: {e}")
    bot_data.save()

async def close_chat_for_members(chat_id: int):
    try:
        await bot.set_chat_permissions(chat_id, ChatPermissions(can_send_messages=False))
        enqueue_message(chat_id, "🔒 Чат закрыт до начала финала.")
    except Exception as e:
        logging.error(f"Не удалось закрыть чат: {e}")

async def open_chat_for_all(chat_id: int):
    try:
        await bot.set_chat_permissions(
            chat_id,
            ChatPermissions(
                can_send_messages=True,
                can_send_media_messages=True,
                can_send_other_messages=True,
                can_add_web_page_previews=True
            )
        )
        enqueue_message(chat_id, "🔓 Чат открыт! Начинаем финал! Ожидаем команду админа!")
    except Exception as e:
        logging.error(f"Не удалось открыть чат: {e}")

# ---------- ОЧЕРЕДЬ ПРОВЕРОК (триггеры и репосты) с регистронезависимостью ----------
async def handle_loto_trigger(chat_id: int, user: types.User, settings: ChatSettings):
    if not user.username: return
    uid = user.id
    if uid not in bot_data.loto_users: return
    if settings.loto_finalists_disabled: return
    user_data = bot_data.loto_users[uid]
    file_id = user_data.photo_file_id
    caption = ", ".join(map(str, sorted(user_data.numbers))) + f" @{user.username}"
    if file_id:
        await bot.send_photo(chat_id, photo=file_id, caption=caption)
    else:
        enqueue_message(chat_id, caption)
    enqueue_message(chat_id, "⚠ ПОЛНАЯ ТИШИНА ⚠\nКарточка проверяется...")
    numbers = user_data.numbers
    dictated_upper = [item.upper() for item in settings.items[:settings.index]]
    missing = [n for n in numbers if str(n).upper() not in dictated_upper]
    channel_id = bot_data.default_channel_id
    sub_ok = await check_subscription(user.id, channel_id) if channel_id else True

    flight = False
    if missing or not sub_ok:
        flight = True
        try:
            await mute_user(chat_id, uid, timedelta(hours=1), settings)
        except Exception as e:
            logging.error(f"Mute error: {e}")

    if not missing:
        res = "✅ Карточка верна"
    else:
        res = f"❌ Карточка неверна. Не было: {', '.join(map(str, missing))}"

    if not sub_ok:
        res += " | 🚫 Не подписан(-а) на канал"
    else:
        res += " | 🟢 Подписан(-а) на канал"

    if flight:
        res += " | Вылет из игры ⛔"
    enqueue_message(chat_id, res)

    if not missing and sub_ok and not settings.loto_finalists_disabled:
        if user.username not in settings.loto_finalists:
            settings.loto_finalists.append(user.username)
            bot_data.save()
            place = len(settings.loto_finalists)
            enqueue_message(chat_id, f"✅ @{user.username} стал(-а) финалистом Лото №{place}!")
            if place >= settings.loto_finalists_count:
                settings.loto_finalists_disabled = True
                bot_data.save()
                await send_finalists_list(chat_id, settings, "loto")
                await close_chat_for_members(chat_id)
                enqueue_message(chat_id, "🏁 Набрано нужное количество финалистов Лото. Чат закрывается.")

def enqueue_loto_trigger(chat_id: int, user: types.User, settings: ChatSettings):
    enqueue_check(chat_id, handle_loto_trigger(chat_id, user, settings))

async def handle_sea_battle_trigger(chat_id: int, user: types.User, settings: ChatSettings):
    if not user.username: return
    uid = user.id
    if uid not in bot_data.sea_battle_users: return
    if settings.sea_battle_finalists_disabled: return
    user_data = bot_data.sea_battle_users[uid]
    file_id = user_data.photo_file_id
    sorted_cells = sorted(user_data.cells, key=lambda x: (x[0], int(x[1:])))
    caption = ", ".join(sorted_cells) + f" @{user.username}"
    if file_id:
        await bot.send_photo(chat_id, photo=file_id, caption=caption)
    else:
        enqueue_message(chat_id, caption)
    enqueue_message(chat_id, "⚠ ПОЛНАЯ ТИШИНА ⚠\nКарточка проверяется...")
    cells = user_data.cells
    dictated_upper = [item.upper() for item in settings.items[:settings.index]]
    missing = [c for c in cells if c.upper() not in dictated_upper]
    channel_id = bot_data.default_channel_id
    sub_ok = await check_subscription(user.id, channel_id) if channel_id else True

    flight = False
    if missing or not sub_ok:
        flight = True
        try:
            await mute_user(chat_id, uid, timedelta(hours=1), settings)
        except Exception as e:
            logging.error(f"Mute error: {e}")

    if not missing:
        res = "✅ Карточка верна"
    else:
        res = f"❌ Карточка неверна. Не было: {', '.join(missing)}"

    if not sub_ok:
        res += " | 🚫 Не подписан(-а) на канал"
    else:
        res += " | 🟢 Подписан(-а) на канал"

    if flight:
        res += " | Вылет из игры ⛔"
    enqueue_message(chat_id, res)

    if not missing and sub_ok and not settings.sea_battle_finalists_disabled:
        if user.username not in settings.sea_battle_finalists:
            settings.sea_battle_finalists.append(user.username)
            bot_data.save()
            place = len(settings.sea_battle_finalists)
            enqueue_message(chat_id, f"✅ @{user.username} стал(-а) финалистом МБ №{place}!")
            if place >= settings.sea_battle_finalists_count:
                settings.sea_battle_finalists_disabled = True
                bot_data.save()
                await send_finalists_list(chat_id, settings, "sea_battle")
                await close_chat_for_members(chat_id)
                enqueue_message(chat_id, "🏁 Набрано нужное количество финалистов МБ. Чат закрывается.")

def enqueue_sea_battle_trigger(chat_id: int, user: types.User, settings: ChatSettings):
    enqueue_check(chat_id, handle_sea_battle_trigger(chat_id, user, settings))

async def handle_loto_forward(chat_id: int, message: Message, settings: ChatSettings):
    caption = message.caption or message.text or ""
    match = re.search(r'@(\w+)', caption)
    if not match: return
    username = match.group(1)
    uid = None
    for u_id, data in bot_data.loto_users.items():
        if data.username == username:
            uid = u_id
            break
    if not uid: return
    if settings.loto_finalists_disabled: return
    user_data = bot_data.loto_users[uid]
    caption_card = ", ".join(map(str, sorted(user_data.numbers))) + f" @{username}"
    if user_data.photo_file_id:
        await bot.send_photo(chat_id, photo=user_data.photo_file_id, caption=caption_card)
    else:
        enqueue_message(chat_id, caption_card)
    enqueue_message(chat_id, "⚠ ПОЛНАЯ ТИШИНА ⚠\nКарточка проверяется...")
    numbers = user_data.numbers
    dictated_upper = [item.upper() for item in settings.items[:settings.index]]
    missing = [n for n in numbers if str(n).upper() not in dictated_upper]
    channel_id = bot_data.default_channel_id
    sub_ok = await check_subscription(uid, channel_id) if channel_id else True

    flight = False
    if missing or not sub_ok:
        flight = True
        try:
            await mute_user(chat_id, uid, timedelta(hours=1), settings)
        except Exception as e:
            logging.error(f"Mute error: {e}")

    if not missing:
        res = "✅ Карточка верна"
    else:
        res = f"❌ Карточка неверна. Не было: {', '.join(map(str, missing))}"

    if not sub_ok:
        res += " | 🚫 Не подписан(-а) на канал"
    else:
        res += " | 🟢 Подписан(-а) на канал"

    if flight:
        res += " | Вылет из игры ⛔"
    enqueue_message(chat_id, res)

    if not missing and sub_ok and not settings.loto_finalists_disabled:
        if username not in settings.loto_finalists:
            settings.loto_finalists.append(username)
            bot_data.save()
            place = len(settings.loto_finalists)
            enqueue_message(chat_id, f"✅ @{username} стал(-а) финалистом Лото №{place}!")
            if place >= settings.loto_finalists_count:
                settings.loto_finalists_disabled = True
                bot_data.save()
                await send_finalists_list(chat_id, settings, "loto")
                await close_chat_for_members(chat_id)
                enqueue_message(chat_id, "🏁 Набрано нужное количество финалистов Лото. Чат закрывается.")

def enqueue_loto_forward(chat_id: int, message: Message, settings: ChatSettings):
    enqueue_check(chat_id, handle_loto_forward(chat_id, message, settings))

async def handle_sea_battle_forward(chat_id: int, message: Message, settings: ChatSettings):
    caption = message.caption or message.text or ""
    match = re.search(r'@(\w+)', caption)
    if not match: return
    username = match.group(1)
    uid = None
    for u_id, data in bot_data.sea_battle_users.items():
        if data.username == username:
            uid = u_id
            break
    if not uid: return
    if settings.sea_battle_finalists_disabled: return
    user_data = bot_data.sea_battle_users[uid]
    sorted_cells = sorted(user_data.cells, key=lambda x: (x[0], int(x[1:])))
    caption_card = ", ".join(sorted_cells) + f" @{username}"
    if user_data.photo_file_id:
        await bot.send_photo(chat_id, photo=user_data.photo_file_id, caption=caption_card)
    else:
        enqueue_message(chat_id, caption_card)
    enqueue_message(chat_id, "⚠ ПОЛНАЯ ТИШИНА ⚠\nКарточка проверяется...")
    cells = user_data.cells
    dictated_upper = [item.upper() for item in settings.items[:settings.index]]
    missing = [c for c in cells if c.upper() not in dictated_upper]
    channel_id = bot_data.default_channel_id
    sub_ok = await check_subscription(uid, channel_id) if channel_id else True

    flight = False
    if missing or not sub_ok:
        flight = True
        try:
            await mute_user(chat_id, uid, timedelta(hours=1), settings)
        except Exception as e:
            logging.error(f"Mute error: {e}")

    if not missing:
        res = "✅ Карточка верна"
    else:
        res = f"❌ Карточка неверна. Не было: {', '.join(missing)}"

    if not sub_ok:
        res += " | 🚫 Не подписан(-а) на канал"
    else:
        res += " | 🟢 Подписан(-а) на канал"

    if flight:
        res += " | Вылет из игры ⛔"
    enqueue_message(chat_id, res)

    if not missing and sub_ok and not settings.sea_battle_finalists_disabled:
        if username not in settings.sea_battle_finalists:
            settings.sea_battle_finalists.append(username)
            bot_data.save()
            place = len(settings.sea_battle_finalists)
            enqueue_message(chat_id, f"✅ @{username} стал(-а) финалистом МБ №{place}!")
            if place >= settings.sea_battle_finalists_count:
                settings.sea_battle_finalists_disabled = True
                bot_data.save()
                await send_finalists_list(chat_id, settings, "sea_battle")
                await close_chat_for_members(chat_id)
                enqueue_message(chat_id, "🏁 Набрано нужное количество финалистов МБ. Чат закрывается.")

def enqueue_sea_battle_forward(chat_id: int, message: Message, settings: ChatSettings):
    enqueue_check(chat_id, handle_sea_battle_forward(chat_id, message, settings))

# ===================== ОСНОВНОЙ ГРУППОВОЙ ХЕНДЛЕР =====================
@dp.message(F.chat.type.in_({"group", "supergroup"}), ~F.from_user.is_bot)
async def group_handler(message: Message):
    chat_id = message.chat.id
    settings = bot_data.chats.get(chat_id)
    if not settings:
        return
    sender_admin = await is_chat_admin(chat_id, message.from_user.id)

    # Вот кубик / Вот слот – отключаем проверку подписки
    if sender_admin and message.text and message.text.strip().lower() in ("вот кубик", "вот слот"):
        bot_data.check_subscription = False
        bot_data.save()
        await open_chat_for_all(chat_id)
        return

    if sender_admin and message.text and message.text.strip().lower() == "стоп игра":
        await unmute_all(chat_id, settings)
        enqueue_message(chat_id, "✅ Все ограничения сняты.")
        return

    if sender_admin and message.forward_from_chat and message.photo:
        if settings.status in ("playing", "paused"):
            if settings.status == "playing":
                settings.status = "paused"
                bot_data.save()
            caption = message.caption or message.text or ""
            if '@' in caption:
                uname = re.search(r'@(\w+)', caption).group(1)
                if any(d.username == uname for d in bot_data.loto_users.values()):
                    enqueue_loto_forward(chat_id, message, settings)
                elif any(d.username == uname for d in bot_data.sea_battle_users.values()):
                    enqueue_sea_battle_forward(chat_id, message, settings)
        return

    if message.text and not sender_admin:
        text = message.text.lower()
        if settings.loto_enabled and settings.loto_trigger_enabled and not settings.loto_finalists_disabled:
            for trigger in settings.loto_triggers:
                if trigger.lower() in text:
                    if settings.status in ("playing", "paused"):
                        if settings.status == "playing":
                            settings.status = "paused"
                            bot_data.save()
                        enqueue_loto_trigger(chat_id, message.from_user, settings)
                        return
                    break
        if settings.sea_battle_enabled and settings.sea_battle_trigger_enabled and not settings.sea_battle_finalists_disabled:
            for trigger in settings.sea_battle_triggers:
                if trigger.lower() in text:
                    if settings.status in ("playing", "paused"):
                        if settings.status == "playing":
                            settings.status = "paused"
                            bot_data.save()
                        enqueue_sea_battle_trigger(chat_id, message.from_user, settings)
                        return
                    break

    if settings.status in ("playing", "paused"):
        if settings.status == "playing":
            settings.status = "paused"
            bot_data.save()

        if sender_admin and message.text and message.text.strip().lower() == "играем дальше":
            settings.status = "playing"
            bot_data.save()
            asyncio.create_task(dictation_loop(chat_id, settings))
            return

        if not sender_admin:
            user = message.from_user
            user_name = user.full_name
            if user.username:
                user_name += f" (@{user.username})"
            channel_id = bot_data.default_channel_id
            if bot_data.check_subscription and channel_id:
                sub_ok = await check_subscription(user.id, channel_id)
                if not sub_ok:
                    await mute_user(chat_id, user.id, timedelta(hours=1), settings)
                    notify = f"👤 {user_name} — ❌ нет подписки на канал, Вылет из игры ⛔\n🔇 Мут на 1 час"
                else:
                    notify = f"👤 {user_name} — ✅ подписан(-а)"
                for uid in bot_data.admins:
                    if await check_chat_access(uid, chat_id):
                        try:
                            await bot.send_message(uid, notify, reply_markup=main_menu_keyboard())
                        except Exception as e:
                            logging.error(f"Не удалось отправить уведомление админу {uid}: {e}")

    if not sender_admin and message.dice:
        if message.dice.emoji == "🎲" and settings.dice_enabled:
            total = await process_dice_sequence(chat_id, message.from_user.id, message.dice.value)
            if total is not None:
                username = f"@{message.from_user.username}" if message.from_user.username else message.from_user.full_name
                await asyncio.sleep(3)
                await message.reply(f"{username} {total}")
        elif message.dice.emoji == "🎰" and settings.slot_enabled:
            total = calc_slot_sum(message.dice.value)
            username = f"@{message.from_user.username}" if message.from_user.username else message.from_user.full_name
            await asyncio.sleep(3)
            await message.reply(f"{username} {total}")

# ---------- ИГРОВЫЕ ФУНКЦИИ ----------
dice_sequences: Dict[int, Dict[int, List[int]]] = {}
async def process_dice_sequence(chat_id, user_id, value):
    if chat_id not in dice_sequences:
        dice_sequences[chat_id] = {}
    seq = dice_sequences[chat_id].setdefault(user_id, [])
    seq.append(value)
    if len(seq) == 3:
        total = sum(seq)
        if seq[0] == seq[1] == seq[2]:
            total *= 2
        del dice_sequences[chat_id][user_id]
        return total
    return None

SLOT_VALUES = [5,3,4,7]
def calc_slot_sum(value):
    v = value - 1
    i = v // 16
    j = (v // 4) % 4
    k = v % 4
    total = SLOT_VALUES[i] + SLOT_VALUES[j] + SLOT_VALUES[k]
    if i == j == k:
        total *= 2
    return total

# ===================== ЗАПУСК =====================
async def main():
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())