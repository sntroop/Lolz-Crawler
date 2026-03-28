import re
import json
from typing import Tuple, List
from datetime import datetime

_html_tag = re.compile(r"<[^>]+>")
_ws = re.compile(r"\s{2,}")
_html_ent = [("&nbsp;"," "),("&amp;","&"),("&lt;","<"),("&gt;",">"),("&quot;",'"'),("&#39;","'")]
_unwrap = re.compile(
    r'\[(?:spoiler|quote|b|i|u|s|color|size|center|left|right|indent'
    r'|highlight|font|code|php|html|ispoiler)(?:=[^\]]+)?\](.*?)'
    r'\[/(?:spoiler|quote|b|i|u|s|color|size|center|left|right|indent'
    r'|highlight|font|code|php|html|ispoiler)\]',
    re.IGNORECASE | re.DOTALL,
)
_img = re.compile(r'\[IMG(?:=[^\]]+)?\](https?://[^\[\]\s]+?)\[/IMG\]', re.IGNORECASE)
_bbcode = re.compile(r'\[[^\]]*\]', re.IGNORECASE)

def clean_bbcode(raw: str) -> Tuple[str, List[str]]:
    images = _img.findall(raw)
    text = raw
    prev = None
    while prev != text:
        prev = text
        text = _unwrap.sub(r'\1', text)
    text = re.sub(r'\[tooltip=\d+\](.*?)\[/tooltip\]', r'\1', text, flags=re.IGNORECASE | re.DOTALL)
    text = re.sub(r'\[HIDE(?:=[^\]]+)?\].*?\[/HIDE\]', '', text, flags=re.IGNORECASE | re.DOTALL)
    text = text.replace("[Скрытый контент]", "")
    text = _bbcode.sub(' ', text)
    text = _html_tag.sub(' ', text)
    for esc, r in _html_ent:
        text = text.replace(esc, r)
    return _ws.sub(' ', text).strip(), images

def fmt_post(row: Tuple, idx: int, total: int, keyword: str = "") -> str:
    post_id, thread_id, thread_title, forum_name, author, post_date, content, images_json = row
    lines = content.split("\n")
    body = "\n".join(lines[2:]).strip() if len(lines) > 2 else content
    if keyword:
        body = re.sub(f"({re.escape(keyword)})", r"<b>\1</b>", body, flags=re.IGNORECASE)
    date_str = post_date.strftime("%d.%m.%Y %H:%M") if hasattr(post_date, "strftime") else str(post_date)[:16]
    images = json.loads(images_json) if images_json else []
    img_line = f"  🖼<b>{len(images)}</b>" if images else ""
    return (
        f"<b>{idx}/{total}</b>  ·  "
        f"<a href='https://lolz.live/posts/{post_id}/'>🔗 пост #{post_id}</a>\n"
        f"{'─'*28}\n"
        f"📂 <b>{forum_name}</b>\n"
        f"🗒 <i>{thread_title[:55]}</i>\n"
        f"👤 <b>{author}</b>  🕒 {date_str}{img_line}\n"
        f"{'─'*28}\n"
        f"{body}"
    )

def fmt_status(stats: dict, running: bool, throttle: str) -> str:
    elapsed = ""
    if stats["started_at"]:
        sec = int((datetime.now() - stats["started_at"]).total_seconds())
        elapsed = f"{sec//3600:02d}:{(sec%3600)//60:02d}:{sec%60:02d}"
    icon = "🟢 Работает" if running else "🔴 Остановлен"
    speed = ""
    if stats["started_at"] and stats["posts_saved"] > 0:
        sec = max(1, int((datetime.now() - stats["started_at"]).total_seconds()))
        speed = f"  ⚡<b>{stats['posts_saved'] // sec}/с</b>"
    return (
        f"🕷 <b>Lolz Краулер — {icon}</b>\n{'─'*32}\n"
        f"⏱ <b>{elapsed}</b>{speed}\n"
        f"🖥 {throttle}\n"
        f"{'─'*32}\n"
        f"📁 Форумы: <b>{stats['forums_done']}/{stats['forums_total']}</b>\n"
        f"🗂 Треды: <b>{stats['threads_done']:,}</b>\n"
        f"{'─'*32}\n"
        f"💬 Постов: <b>{stats['posts_saved']:,}</b>\n"
        f"💭 Комментов: <b>{stats['comments_saved']:,}</b>\n"
        f"👤 Профилей: <b>{stats['profiles_saved']:,}</b>\n"
        f"📝 Постов стены: <b>{stats['profile_posts_saved']:,}</b>\n"
        f"📡 Чатбокс: <b>{stats['chatbox_saved']:,}</b>\n"
        f"🔁 Пропущено: <b>{stats['posts_skipped']:,}</b>\n"
        f"❌ Ошибок: <b>{stats['errors']:,}</b>\n"
        f"{'─'*32}\n"
        f"📂 <i>{stats['current_forum'][:45]}</i>\n"
        f"🗒 <i>{stats['current_thread'][:55]}</i>"
    )