from aiogram import F
from aiogram.types import CallbackQuery

import bot_legacy as app


@app.dp.callback_query(F.data.startswith("selectchat_"))
async def selectchat_callback(callback: CallbackQuery, state):
    if not app.is_admin(callback.from_user.id):
        await callback.answer("⛔", show_alert=True)
        return

    try:
        chat_id = int(callback.data.split("_", 1)[1])
    except Exception:
        await callback.answer("❌ Неверный ID чата", show_alert=True)
        return

    if not await app.check_chat_access(callback.from_user.id, chat_id):
        await callback.answer("⛔ Нет доступа к чату", show_alert=True)
        return

    app.bot_data.default_chat_id = chat_id
    app.bot_data.chats.setdefault(chat_id, app.ChatSettings())
    app.bot_data.save()

    try:
        chat = await app.bot.get_chat(chat_id)
        title = chat.title or "Без названия"
    except Exception:
        title = "Неизвестный чат"

    await callback.answer("✅ Чат выбран")
    await callback.message.answer(
        f"✅ Активный чат: `{chat_id}` — {title}",
        reply_markup=app.main_menu_keyboard(),
        parse_mode="Markdown",
    )
