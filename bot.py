import asyncio
import json
import logging
from typing import Optional

from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery, Message

from config import BOT_TOKEN, ADMIN_ID, SEARCH_PAGE_SIZE
from database import db
from crawler import crawler
from utils import fmt_post, fmt_status


class States(StatesGroup):
    waiting_search = State()


class BotApp:
    def __init__(self):
        self.bot = Bot(token=BOT_TOKEN)
        self.dp = Dispatcher(storage=MemoryStorage())
        self.report_chat = None
        self.report_msg = None
        self._setup()
    
    def _is_admin(self, uid: int) -> bool:
        return uid == ADMIN_ID
    
    def _kb_main(self) -> InlineKeyboardMarkup:
        running = crawler.running
        return InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📊 Статистика БД", callback_data="stats")],
            [InlineKeyboardButton(text="⏹ Стоп" if running else "🕷 Запустить краулер", callback_data="crawler_stop" if running else "crawler_start")],
            [InlineKeyboardButton(text="📈 Статус краулера", callback_data="crawler_status")],
            [InlineKeyboardButton(text="🔍 Поиск по БД", callback_data="search_start")],
            [InlineKeyboardButton(text="🗑 Очистить БД", callback_data="confirm_clear")],
        ])
    
    def _kb_search(self, kw: str, off: int, total: int, pid: int, imgs: list) -> InlineKeyboardMarkup:
        rows = []
        nav = []
        if off > 0:
            nav.append(InlineKeyboardButton(text="◀️", callback_data=f"sr:{kw[:28]}:{off - SEARCH_PAGE_SIZE}"))
        cur = off // SEARCH_PAGE_SIZE + 1
        tot = (total + SEARCH_PAGE_SIZE - 1) // SEARCH_PAGE_SIZE
        nav.append(InlineKeyboardButton(text=f"📄 {cur}/{tot}", callback_data="noop"))
        if off + SEARCH_PAGE_SIZE < total:
            nav.append(InlineKeyboardButton(text="▶️", callback_data=f"sr:{kw[:28]}:{off + SEARCH_PAGE_SIZE}"))
        rows.append(nav)
        if imgs:
            btns = [InlineKeyboardButton(text=f"🖼{i+1}", callback_data=f"photo:{pid}:{i}") for i in range(min(len(imgs), 9))]
            for i in range(0, len(btns), 4):
                rows.append(btns[i:i+4])
        rows.append([
            InlineKeyboardButton(text="🔍 Новый поиск", callback_data="search_start"),
            InlineKeyboardButton(text="🏠 Меню", callback_data="main_back"),
        ])
        return InlineKeyboardMarkup(inline_keyboard=rows)
    
    async def _send_search(self, chat_id: int, kw: str, off: int, edit_id: int = None):
        try:
            rows, total = db.search(kw, off)
        except Exception as e:
            await self.bot.send_message(chat_id, f"❌ {e}")
            return
        if not rows:
            kb = InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(text="🔍 Новый поиск", callback_data="search_start"),
                InlineKeyboardButton(text="🏠 Меню", callback_data="main_back"),
            ]])
            text = f"😶 По запросу «<b>{kw}</b>» ничего не найдено."
            if edit_id:
                try:
                    await self.bot.edit_message_text(text, chat_id=chat_id, message_id=edit_id, parse_mode="HTML", reply_markup=kb)
                    return
                except:
                    pass
            await self.bot.send_message(chat_id, text, parse_mode="HTML", reply_markup=kb)
            return
        row = rows[0]
        idx = off + 1
        imgs = json.loads(row[7]) if row[7] else []
        text = fmt_post(row, idx, total, kw)
        kb = self._kb_search(kw, off, total, row[0], imgs)
        try:
            if edit_id:
                await self.bot.edit_message_text(text, chat_id=chat_id, message_id=edit_id, parse_mode="HTML", reply_markup=kb, disable_web_page_preview=True)
            else:
                await self.bot.send_message(chat_id, text, parse_mode="HTML", reply_markup=kb, disable_web_page_preview=True)
        except Exception as e:
            logging.warning(f"Search send: {e}")
    
    async def _status_updater(self):
        while crawler.running:
            await asyncio.sleep(15)
            if not crawler.running or not self.report_chat or not self.report_msg:
                continue
            try:
                await self.bot.edit_message_text(
                    fmt_status(crawler.stats, crawler.running, crawler.throttle.status),
                    chat_id=self.report_chat, message_id=self.report_msg,
                    parse_mode="HTML",
                    reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⏹ Остановить", callback_data="crawler_stop")]]),
                )
            except:
                pass
    
    def _setup(self):
        @self.dp.message(Command("start"))
        async def start(m: Message, state: FSMContext):
            if not self._is_admin(m.from_user.id):
                await m.answer("❌")
                return
            await state.clear()
            await m.answer("🛡 <b>Lolz Crawler</b>\n\nВыберите действие:", parse_mode="HTML", reply_markup=self._kb_main())
        
        @self.dp.callback_query(F.data == "stats")
        async def stats(c: CallbackQuery):
            if not self._is_admin(c.from_user.id):
                await c.answer("❌", show_alert=True)
                return
            await c.answer()
            try:
                st = db.stats()
                size, ratio = db.size_stats()
                top = db.top_forums(5)
                forum_txt = "".join(f"  • <code>{r[0][:35]}</code>: <b>{r[1]:,}</b>\n" for r in top)
                text = f"📊 <b>Статистика БД</b>\n{'─'*32}\n💬 Постов: <b>{st['posts']:,}</b>\n💭 Комментов: <b>{st['comments']:,}</b>\n👤 Профилей: <b>{st['profiles']:,}</b>\n📝 Постов стены: <b>{st['profile_posts']:,}</b>\n📡 Чатбокс: <b>{st['chatbox']:,}</b>\n{'─'*32}\n💾 Размер: <b>{size}</b> (×{ratio})\n\n🏆 <b>Топ форумов:</b>\n{forum_txt}"
                try:
                    await c.message.edit_text(text, parse_mode="HTML", reply_markup=self._kb_main())
                except:
                    await c.message.answer(text, parse_mode="HTML", reply_markup=self._kb_main())
            except Exception as e:
                await c.answer(f"Ошибка: {str(e)[:80]}", show_alert=True)
        
        @self.dp.callback_query(F.data == "crawler_start")
        async def start_crawl(c: CallbackQuery):
            if not self._is_admin(c.from_user.id):
                await c.answer("❌", show_alert=True)
                return
            if crawler.running:
                await c.answer("⚠️ Уже запущен", show_alert=True)
                return
            self.report_chat = c.message.chat.id
            msg = await c.message.answer(fmt_status(crawler.stats, crawler.running, crawler.throttle.status), parse_mode="HTML", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⏹ Остановить", callback_data="crawler_stop")]]))
            self.report_msg = msg.message_id
            asyncio.create_task(crawler.run())
            asyncio.create_task(self._status_updater())
            await c.answer("🕷 Запущен")
            try:
                await c.message.edit_reply_markup(reply_markup=self._kb_main())
            except:
                pass
        
        @self.dp.callback_query(F.data == "crawler_stop")
        async def stop_crawl(c: CallbackQuery):
            if not self._is_admin(c.from_user.id):
                await c.answer("❌", show_alert=True)
                return
            if not crawler.running:
                await c.answer("ℹ️ Не запущен", show_alert=True)
                return
            crawler.stop()
            await c.answer("⏹ Остановлен")
            await c.message.answer(f"⏹ <b>Краулер остановлен</b>\n\n{fmt_status(crawler.stats, crawler.running, crawler.throttle.status)}", parse_mode="HTML", reply_markup=self._kb_main())
            try:
                await c.message.edit_reply_markup(reply_markup=self._kb_main())
            except:
                pass
        
        @self.dp.callback_query(F.data == "crawler_status")
        async def status_crawl(c: CallbackQuery):
            if not self._is_admin(c.from_user.id):
                await c.answer("❌", show_alert=True)
                return
            await c.answer()
            try:
                await c.message.edit_text(fmt_status(crawler.stats, crawler.running, crawler.throttle.status), parse_mode="HTML", reply_markup=self._kb_main())
            except:
                await c.message.answer(fmt_status(crawler.stats, crawler.running, crawler.throttle.status), parse_mode="HTML", reply_markup=self._kb_main())
        
        @self.dp.callback_query(F.data == "search_start")
        async def search_start(c: CallbackQuery, state: FSMContext):
            if not self._is_admin(c.from_user.id):
                await c.answer("❌", show_alert=True)
                return
            await c.answer()
            await state.set_state(States.waiting_search)
            await c.message.answer("🔍 <b>Поиск по БД</b>\n\nВведите ключевое слово:", parse_mode="HTML")
        
        @self.dp.message(States.waiting_search)
        async def search_do(m: Message, state: FSMContext):
            if not self._is_admin(m.from_user.id):
                return
            kw = (m.text or "").strip()
            if not kw:
                await m.answer("Введи запрос")
                return
            await state.clear()
            await m.answer("🔄 Ищу...")
            await self._send_search(m.chat.id, kw, 0)
        
        @self.dp.callback_query(F.data.startswith("sr:"))
        async def search_page(c: CallbackQuery):
            if not self._is_admin(c.from_user.id):
                await c.answer("❌", show_alert=True)
                return
            await c.answer()
            _, kw, off = c.data.split(":", 2)
            await self._send_search(c.message.chat.id, kw, int(off), edit_id=c.message.message_id)
        
        @self.dp.callback_query(F.data.startswith("photo:"))
        async def show_photo(c: CallbackQuery):
            if not self._is_admin(c.from_user.id):
                await c.answer("❌", show_alert=True)
                return
            _, pid_s, idx_s = c.data.split(":", 2)
            try:
                row = db.get_post(int(pid_s))
                if not row:
                    await c.answer("Пост не найден", show_alert=True)
                    return
                imgs = json.loads(row[7]) if row[7] else []
                idx = int(idx_s)
                if idx >= len(imgs):
                    await c.answer("Фото не найдено", show_alert=True)
                    return
                await c.answer()
                await self.bot.send_photo(c.message.chat.id, photo=imgs[idx], caption=f"🖼 {idx+1}/{len(imgs)} · <a href='https://lolz.live/posts/{pid_s}/'>#{pid_s}</a>", parse_mode="HTML")
            except Exception as e:
                await c.answer(str(e)[:60], show_alert=True)
        
        @self.dp.callback_query(F.data == "noop")
        async def noop(c: CallbackQuery):
            await c.answer()
        
        @self.dp.callback_query(F.data == "confirm_clear")
        async def confirm_clear(c: CallbackQuery):
            if not self._is_admin(c.from_user.id):
                await c.answer("❌", show_alert=True)
                return
            await c.message.edit_text("⚠️ <b>ВНИМАНИЕ!</b>\nОчистить ВСЕ таблицы?\nНеобратимо!", parse_mode="HTML", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔥 ДА", callback_data="do_clear"), InlineKeyboardButton(text="🔙 Отмена", callback_data="main_back")]]))
            await c.answer()
        
        @self.dp.callback_query(F.data == "do_clear")
        async def do_clear(c: CallbackQuery):
            if not self._is_admin(c.from_user.id):
                await c.answer("❌", show_alert=True)
                return
            db.clear_all()
            await c.answer("✅ Очищено", show_alert=True)
            await c.message.edit_text("✅ <b>Все таблицы очищены.</b>", parse_mode="HTML", reply_markup=self._kb_main())
        
        @self.dp.callback_query(F.data == "main_back")
        async def main_back(c: CallbackQuery, state: FSMContext):
            await state.clear()
            await c.answer()
            try:
                await c.message.edit_text("🛡 <b>Lolz Crawler</b>\n\nВыберите действие:", parse_mode="HTML", reply_markup=self._kb_main())
            except:
                await c.message.answer("🛡 <b>Lolz Crawler</b>\n\nВыберите действие:", parse_mode="HTML", reply_markup=self._kb_main())
        
        @self.dp.message()
        async def reject(m: Message):
            if not self._is_admin(m.from_user.id):
                await m.answer("❌")
    
    async def run(self):
        await self.dp.start_polling(self.bot, skip_updates=True)
    
    async def stop(self):
        await self.bot.session.close()