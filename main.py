import asyncio

import bot_legacy as app
from queueing import install as install_send_queue

# Install outbound message serialization before polling starts.
install_send_queue(app.bot, delay=0.35)

# Register missing/extra fixes and UI tweaks.
import fixes  # noqa: F401


async def main():
    await bot.delete_webhook(drop_pending_updates=True)
    await app.dp.start_polling(app.bot)


if __name__ == "__main__":
    asyncio.run(main())
