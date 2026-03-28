import asyncio
import json
import logging
import hashlib
from datetime import datetime
from typing import Dict, Any, Optional, Set, Tuple

import aiohttp
import psutil

from config import *
from database import db
from utils import clean_bbcode


class RateLimiter:
    def __init__(self, max_concurrency: int):
        self._delay = RATE_MIN_DELAY
        self._lock = asyncio.Lock()
        self.semaphore = asyncio.Semaphore(max_concurrency)
        self._sem_val = max_concurrency
    
    async def success(self):
        async with self._lock:
            self._delay = max(RATE_MIN_DELAY, self._delay * RATE_RECOVERY_STEP)
    
    async def ratelimit(self):
        async with self._lock:
            self._delay = min(RATE_MAX_DELAY, self._delay * RATE_BACKOFF_FACTOR)
            new_val = max(1, self._sem_val // 2)
            for _ in range(self._sem_val - new_val):
                try:
                    self.semaphore._value = max(0, self.semaphore._value - 1)
                except:
                    pass
            self._sem_val = new_val
            logging.warning(f"429 delay={self._delay:.1f}s sem={self._sem_val}")
        await asyncio.sleep(self._delay)
    
    @property
    def delay(self) -> float:
        return self._delay


class Throttle:
    def __init__(self):
        self._delay = 0.0
        self._paused = False
        self._lock = asyncio.Lock()
    
    async def check(self):
        cpu = psutil.cpu_percent(interval=1)
        ram = psutil.virtual_memory().percent
        async with self._lock:
            if cpu >= CPU_CRITICAL_THRESHOLD or ram >= 95:
                if not self._paused:
                    logging.warning(f"PAUSE CPU={cpu}% RAM={ram}%")
                self._paused = True
                self._delay = 5.0
            elif cpu >= CPU_THROTTLE_THRESHOLD or ram >= RAM_THROTTLE_THRESHOLD:
                self._paused = False
                over = max(max(0, cpu - CPU_THROTTLE_THRESHOLD), max(0, ram - RAM_THROTTLE_THRESHOLD))
                self._delay = (over / 5) * THROTTLE_SLEEP_STEP
            else:
                self._paused = False
                self._delay = 0.0
    
    async def wait(self):
        while True:
            async with self._lock:
                paused = self._paused
                delay = self._delay
            if not paused:
                if delay > 0:
                    await asyncio.sleep(delay)
                return
            await asyncio.sleep(2)
    
    @property
    def status(self):
        cpu = psutil.cpu_percent()
        ram = psutil.virtual_memory().percent
        if self._paused:
            return f"🔴 Пауза CPU={cpu}% RAM={ram}%"
        elif self._delay > 0.05:
            return f"🟡 Throttle CPU={cpu}% RAM={ram}% +{self._delay:.1f}s"
        return f"🟢 OK CPU={cpu}% RAM={ram}%"


class Crawler:
    def __init__(self):
        self.running = False
        self.session = None
        self.throttle = Throttle()
        self.stats = {
            "forums_total": 0, "forums_done": 0, "threads_done": 0,
            "posts_saved": 0, "comments_saved": 0, "profiles_saved": 0,
            "profile_posts_saved": 0, "chatbox_saved": 0,
            "posts_skipped": 0, "errors": 0,
            "current_forum": "—", "current_thread": "—", "started_at": None,
        }
    
    async def _session(self):
        if self.session is None or self.session.closed:
            self.session = aiohttp.ClientSession(
                connector=aiohttp.TCPConnector(
                    limit=CRAWLER_CONCURRENCY * 3, ttl_dns_cache=600,
                    enable_cleanup_closed=True, keepalive_timeout=30,
                ),
                headers={"Authorization": f"Bearer {LOLZ_TOKEN}", "Accept": "application/json"},
                timeout=aiohttp.ClientTimeout(total=30, connect=5),
            )
        return self.session
    
    async def _get(self, path: str, rl: RateLimiter) -> Optional[Dict]:
        url = LOLZ_API + path
        sess = await self._session()
        async with rl.semaphore:
            for attempt in range(5):
                await self.throttle.wait()
                try:
                    async with sess.get(url) as resp:
                        if resp.status == 429:
                            await rl.ratelimit()
                            continue
                        if resp.status in (403, 404):
                            return {"_http_error": resp.status}
                        if resp.status != 200:
                            self.stats["errors"] += 1
                            return {"_http_error": resp.status}
                        data = await resp.json(content_type=None)
                        await rl.success()
                        await asyncio.sleep(rl.delay)
                        return data
                except asyncio.TimeoutError:
                    await asyncio.sleep(2 ** attempt)
                except aiohttp.ClientError as e:
                    logging.warning(f"ClientError {attempt+1} {path}: {e}")
                    await asyncio.sleep(2 ** attempt)
                except Exception as e:
                    logging.error(f"API error {path}: {e}")
                    return None
        return None
    
    async def _crawl_thread(self, forum_id: int, forum_name: str, thread: Dict,
                            rl: RateLimiter, rl_cmt: RateLimiter, queue: asyncio.Queue):
        thread_id = thread.get("thread_id")
        thread_title = thread.get("thread_title", f"thread_{thread_id}")
        if not thread_id:
            return
        self.stats["current_thread"] = thread_title
        self.stats["threads_done"] += 1
        page = 1
        while self.running:
            data = await self._get(f"/posts?thread_id={thread_id}&page={page}&limit={POSTS_PER_PAGE}", rl)
            if not data or "_http_error" in data:
                if data and data.get("_http_error") not in (403, 404):
                    self.stats["errors"] += 1
                break
            posts = data.get("posts", [])
            if not posts:
                break
            for post in posts:
                body, images = clean_bbcode(post.get("post_body") or post.get("post_body_plain_text") or "")
                if not body or len(body) < 10:
                    self.stats["posts_skipped"] += 1
                    continue
                post_id = post.get("post_id", 0)
                author = post.get("poster_username", "unknown")
                ts = post.get("post_create_date", 0)
                dt = datetime.utcfromtimestamp(ts) if ts else datetime(2000, 1, 1)
                content = f"[Lolz] {forum_name} | {thread_title}\nАвтор: {author} | {dt.strftime('%d.%m.%Y %H:%M')} | thread={thread_id} post={post_id}\n\n{body}"
                row = {
                    "id": hashlib.md5(content.encode()).hexdigest(),
                    "forum_id": forum_id, "forum_name": forum_name,
                    "thread_id": thread_id, "thread_title": thread_title,
                    "post_id": post_id, "author": author, "post_date": dt,
                    "content": content, "images": json.dumps(images, ensure_ascii=False),
                }
                await queue.put(("post", row))
                if post_id:
                    asyncio.create_task(self._crawl_comments(post_id, thread_id, rl_cmt, queue))
            if not data.get("links", {}).get("next"):
                break
            page += 1
    
    async def _crawl_comments(self, post_id: int, thread_id: int, rl: RateLimiter, queue: asyncio.Queue):
        data = await self._get(f"/posts/comments?post_id={post_id}", rl)
        if not data or "_http_error" in data:
            return
        for cmt in data.get("post_comments", []):
            raw = cmt.get("post_comment_body") or cmt.get("post_comment_body_plain_text") or ""
            body, _ = clean_bbcode(raw)
            if not body or len(body) < 2:
                continue
            author = cmt.get("poster_username", "unknown")
            ts = cmt.get("post_comment_create_date", 0)
            dt = datetime.utcfromtimestamp(ts) if ts else datetime(2000, 1, 1)
            uid = str(cmt.get("post_comment_id", 0))
            row = {
                "id": hashlib.md5(f"{post_id}{uid}".encode()).hexdigest(),
                "post_id": post_id, "thread_id": thread_id,
                "author": author, "comment_date": dt, "content": body,
            }
            await queue.put(("comment", row))
    
    async def _crawl_users(self, rl: RateLimiter, queue: asyncio.Queue):
        page = db.load_ckpt_int("users_page", 1)
        logging.info(f"Users crawl page={page}")
        while self.running:
            data = await self._get(f"/users?page={page}&limit={USERS_PER_PAGE}", rl)
            if not data or "_http_error" in data:
                break
            users = data.get("users", [])
            if not users:
                break
            for u in users:
                uid = u.get("user_id")
                if not uid:
                    continue
                reg = u.get("user_register_date", 0)
                seen = u.get("user_last_seen_date", 0)
                fol = u.get("user_followers", {})
                fing = u.get("user_following", {})
                row = {
                    "user_id": uid, "username": u.get("username", ""),
                    "group_id": u.get("user_group_id", 0),
                    "message_count": u.get("user_message_count", 0),
                    "like_count": u.get("user_like_count", 0),
                    "register_date": datetime.utcfromtimestamp(reg) if reg else datetime(2000,1,1),
                    "last_seen": datetime.utcfromtimestamp(seen) if seen else datetime(2000,1,1),
                    "balance": str(u.get("balance", "")), "hold": str(u.get("hold", "")),
                    "currency": str(u.get("currency", "")),
                    "is_banned": int(u.get("is_banned", 0)),
                    "custom_title": str(u.get("custom_title", "")),
                    "trophy_count": u.get("trophy_count", 0),
                    "followers": fol.get("total", 0) if isinstance(fol, dict) else 0,
                    "followings": fing.get("total", 0) if isinstance(fing, dict) else 0,
                }
                await queue.put(("profile", row))
                await self._crawl_profile_posts(uid, rl, queue)
            db.save_ckpt("users_page", str(page))
            if not data.get("links", {}).get("next"):
                db.save_ckpt("users_page", "1")
                break
            page += 1
    
    async def _crawl_profile_posts(self, user_id: int, rl: RateLimiter, queue: asyncio.Queue):
        data = await self._get(f"/users/{user_id}/profile-posts?limit=20", rl)
        if not data or "_http_error" in data:
            return
        for pp in data.get("profile_posts", []):
            raw = pp.get("post_body") or pp.get("post_body_plain_text") or ""
            body, images = clean_bbcode(raw)
            if not body or len(body) < 2:
                continue
            ts = pp.get("post_create_date", 0)
            dt = datetime.utcfromtimestamp(ts) if ts else datetime(2000, 1, 1)
            pid = pp.get("profile_post_id", 0)
            row = {
                "id": hashlib.md5(f"{pid}{body}".encode()).hexdigest(),
                "profile_post_id": pid,
                "timeline_user_id": pp.get("timeline_user_id", 0),
                "timeline_username": pp.get("timeline_username", ""),
                "author": pp.get("poster_username", "unknown"),
                "post_date": dt, "content": body,
                "images": json.dumps(images, ensure_ascii=False),
                "like_count": pp.get("post_like_count", 0),
                "comment_count": pp.get("post_comment_count", 0),
            }
            await queue.put(("profile_post", row))
    
    async def _crawl_chatbox(self, rl: RateLimiter, queue: asyncio.Queue):
        data = await self._get("/chatbox/messages", rl)
        if not data or "_http_error" in data:
            return
        for msg in data.get("messages", []):
            raw = msg.get("messageRaw") or msg.get("message") or ""
            body, _ = clean_bbcode(raw)
            if not body:
                continue
            mid = msg.get("message_id", 0)
            ts = msg.get("date", 0)
            dt = datetime.utcfromtimestamp(ts) if ts else datetime(2000, 1, 1)
            user = msg.get("user", {})
            room = msg.get("room", {})
            row = {
                "message_id": mid,
                "author": user.get("username", "unknown") if isinstance(user, dict) else "unknown",
                "message_date": dt, "content": body,
                "room_id": room.get("roomId", 0) if isinstance(room, dict) else 0,
            }
            await queue.put(("chatbox", row))
    
    async def _flusher(self, queue: asyncio.Queue):
        bufs = {"post": [], "comment": [], "profile": [], "profile_post": [], "chatbox": []}
        cols = {
            "post": ["id","forum_id","forum_name","thread_id","thread_title","post_id","author","post_date","content","images"],
            "comment": ["id","post_id","thread_id","author","comment_date","content"],
            "profile": ["user_id","username","group_id","message_count","like_count","register_date","last_seen","balance","hold","currency","is_banned","custom_title","trophy_count","followers","followings"],
            "profile_post": ["id","profile_post_id","timeline_user_id","timeline_username","author","post_date","content","images","like_count","comment_count"],
            "chatbox": ["message_id","author","message_date","content","room_id"],
        }
        tables = {"post": T_POSTS, "comment": T_COMMENTS, "profile": T_PROFILES, "profile_post": T_PROFPOST, "chatbox": T_CHATBOX}
        async def flush():
            for kind, rows in bufs.items():
                if not rows:
                    continue
                batch, bufs[kind] = rows[:], []
                try:
                    db.insert(tables[kind], cols[kind], batch)
                    self.stats[f"{kind}s_saved"] += len(batch)
                    logging.info(f"Flushed {len(batch)} {kind}s")
                except Exception as e:
                    logging.error(f"Insert {kind}: {e}")
                    self.stats["errors"] += 1
        while True:
            try:
                item = await asyncio.wait_for(queue.get(), timeout=3.0)
                if item is None:
                    await flush()
                    break
                kind, row = item
                bufs[kind].append(row)
                queue.task_done()
                if sum(len(v) for v in bufs.values()) >= CRAWLER_BATCH_SIZE:
                    await flush()
            except asyncio.TimeoutError:
                if any(bufs.values()):
                    await flush()
            except asyncio.CancelledError:
                await flush()
                break
    
    async def _watcher(self):
        while self.running:
            await self.throttle.check()
            await asyncio.sleep(THROTTLE_CHECK_INTERVAL)
    
    async def run(self):
        self.running = True
        self.stats["started_at"] = datetime.now()
        rl = RateLimiter(CRAWLER_CONCURRENCY)
        rl_cmt = RateLimiter(COMMENT_CONCURRENCY)
        queue = asyncio.Queue(maxsize=CRAWLER_BATCH_SIZE * 4)
        flusher = asyncio.create_task(self._flusher(queue))
        watcher = asyncio.create_task(self._watcher())
        try:
            data = await self._get("/forums", rl)
            if not data or "forums" not in data:
                logging.error("No forums")
                return
            forums = [(f["forum_id"], f.get("forum_title", f"forum_{f['forum_id']}")) for f in data["forums"] if f.get("forum_id")]
            self.stats["forums_total"] = len(forums)
            start_id = db.load_ckpt_int("last_forum_id", 0)
            skip = start_id > 0
            logging.info(f"{len(forums)} forums, resume={start_id}")
            for forum_id, forum_name in forums:
                if not self.running:
                    break
                if skip:
                    if forum_id == start_id:
                        skip = False
                    else:
                        self.stats["forums_done"] += 1
                        continue
                self.stats["current_forum"] = forum_name
                self.stats["forums_done"] += 1
                db.save_ckpt("last_forum_id", str(forum_id))
                logging.info(f"Forum: {forum_name} ({forum_id})")
                page = db.load_ckpt_int(f"forum_{forum_id}_page", 1)
                pending = set()
                while self.running:
                    tdata = await self._get(f"/threads?forum_id={forum_id}&page={page}&limit={THREADS_PER_PAGE}&order=thread_create_date_reverse", rl)
                    if not tdata or "_http_error" in tdata:
                        self.stats["errors"] += 1
                        break
                    threads = tdata.get("threads", [])
                    if not threads:
                        break
                    db.save_ckpt(f"forum_{forum_id}_page", str(page))
                    for t in threads:
                        if not self.running:
                            break
                        task = asyncio.create_task(self._crawl_thread(forum_id, forum_name, t, rl, rl_cmt, queue))
                        pending.add(task)
                        task.add_done_callback(pending.discard)
                        while len(pending) >= CRAWLER_CONCURRENCY * 4:
                            done, _ = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
                            for d in done:
                                if d.exception():
                                    self.stats["errors"] += 1
                    if not tdata.get("links", {}).get("next"):
                        db.save_ckpt(f"forum_{forum_id}_page", "1")
                        break
                    page += 1
                if pending:
                    await asyncio.gather(*pending, return_exceptions=True)
            if self.running:
                logging.info("Crawling users...")
                await self._crawl_users(rl, queue)
            if self.running:
                await self._crawl_chatbox(rl, queue)
            db.save_ckpt("last_forum_id", "0")
        except asyncio.CancelledError:
            logging.info("Cancelled")
        except Exception as e:
            logging.error(f"Fatal: {e}", exc_info=True)
            self.stats["errors"] += 1
        finally:
            await queue.put(None)
            try:
                await asyncio.wait_for(flusher, timeout=60)
            except asyncio.TimeoutError:
                flusher.cancel()
            watcher.cancel()
            self.running = False
    
    def stop(self):
        self.running = False
        if self.session and not self.session.closed:
            asyncio.create_task(self.session.close())

crawler = Crawler()