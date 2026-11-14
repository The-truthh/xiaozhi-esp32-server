import json
import uuid
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from core.connection import ConnectionHandler
TAG = __name__


async def handleAbortMessage(conn: "ConnectionHandler"):
    conn.logger.bind(tag=TAG).info("Abort message received")
    # 设置成打断状态，会自动打断llm、tts任务
    conn.close_after_chat = False
    conn.client_abort = True
    if hasattr(conn, "information_session"):
        conn.information_session.invalidate()
    cancel_audio = None
    if getattr(getattr(conn, "intent", None), "read_only", False):
        conn.sentence_id = 'cancelled-' + uuid.uuid4().hex
        cancel_audio = getattr(conn.tts, "cancel_pending_audio", None)
    conn.clear_queues()
    # 打断客户端说话状态
    await conn.websocket.send(
        json.dumps({"type": "tts", "state": "stop", "session_id": conn.session_id})
    )
    conn.clearSpeakStatus()
    if cancel_audio:
        await cancel_audio()
    conn.logger.bind(tag=TAG).info("Abort message received-end")
