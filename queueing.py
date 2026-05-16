import asyncio
import inspect
import logging
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable, Dict, Optional, Tuple

from aiogram.exceptions import TelegramRetryAfter


@dataclass
class _QueuedCall:
    factory: Callable[[], Awaitable[Any]]
    future: asyncio.Future[Any]


class ChatSendQueue:
    def __init__(self, bot, delay: float = 0.35):
        self.bot = bot
        self.delay = delay
        self._queues: Dict[int, asyncio.Queue[_QueuedCall]] = defaultdict(asyncio.Queue)
        self._workers: Dict[int, asyncio.Task] = {}
        self._lock = asyncio.Lock()

    def _ensure_worker(self, chat_id: int) -> None:
        if chat_id in self._workers and not self._workers[chat_id].done():
            return
        self._workers[chat_id] = asyncio.create_task(self._worker(chat_id))

    async def _worker(self, chat_id: int) -> None:
        q = self._queues[chat_id]
        while True:
            item = await q.get()
            try:
                result = await item.factory()
                if not item.future.done():
                    item.future.set_result(result)
            except TelegramRetryAfter as e:
                try:
                    await asyncio.sleep(e.retry_after)
                    result = await item.factory()
                    if not item.future.done():
                        item.future.set_result(result)
                except Exception as exc:
                    if not item.future.done():
                        item.future.set_exception(exc)
                    logging.exception("Queued send retry failed for chat %s", chat_id)
            except Exception as exc:
                if not item.future.done():
                    item.future.set_exception(exc)
                logging.exception("Queued send failed for chat %s", chat_id)
            finally:
                await asyncio.sleep(self.delay)

    async def enqueue(self, chat_id: int, factory: Callable[[], Awaitable[Any]]) -> Any:
        async with self._lock:
            self._ensure_worker(chat_id)
            loop = asyncio.get_running_loop()
            fut: asyncio.Future[Any] = loop.create_future()
            await self._queues[chat_id].put(_QueuedCall(factory=factory, future=fut))
        return await fut


def _extract_chat_id(*args, **kwargs) -> int:
    if "chat_id" in kwargs and kwargs["chat_id"] is not None:
        return int(kwargs["chat_id"])
    if args:
        return int(args[0])
    raise TypeError("chat_id is required")


def install(bot, delay: float = 0.35):
    """
    Serialize outbound messages per target chat.
    This covers message.answer()/reply() too because they call Bot.send_message internally.
    """
    manager = ChatSendQueue(bot, delay=delay)

    original_send_message = bot.send_message
    original_send_photo = bot.send_photo
    original_copy_message = bot.copy_message

    async def send_message(*args, **kwargs):
        chat_id = _extract_chat_id(*args, **kwargs)
        return await manager.enqueue(chat_id, lambda: original_send_message(*args, **kwargs))

    async def send_photo(*args, **kwargs):
        chat_id = _extract_chat_id(*args, **kwargs)
        return await manager.enqueue(chat_id, lambda: original_send_photo(*args, **kwargs))

    async def copy_message(*args, **kwargs):
        chat_id = _extract_chat_id(*args, **kwargs)
        return await manager.enqueue(chat_id, lambda: original_copy_message(*args, **kwargs))

    bot.send_message = send_message
    bot.send_photo = send_photo
    bot.copy_message = copy_message

    return manager
