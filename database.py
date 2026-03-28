import logging
from typing import List, Dict, Any, Optional, Tuple
import clickhouse_connect
from config import *

class Database:
    def __init__(self):
        self.client = clickhouse_connect.get_client(
            host=CH_HOST, username=CH_USER, password=CH_PASS,
            database=CH_DB, compress=True,
        )
        self.ckpt_client = clickhouse_connect.get_client(
            host=CH_HOST, username=CH_USER, password=CH_PASS,
            database=CH_DB, compress=False,
        )
        self._init_tables()
    
    def _init_tables(self):
        queries = [
            f"""CREATE TABLE IF NOT EXISTS {T_POSTS} (
                id String, forum_id UInt32, forum_name String,
                thread_id UInt32, thread_title String, post_id UInt32,
                author String, post_date DateTime, content String,
                images String, created_at DateTime DEFAULT now()
            ) ENGINE = ReplacingMergeTree() ORDER BY (forum_id, thread_id, post_id)""",
            
            f"""CREATE TABLE IF NOT EXISTS {T_COMMENTS} (
                id String, post_id UInt32, thread_id UInt32,
                author String, comment_date DateTime, content String,
                created_at DateTime DEFAULT now()
            ) ENGINE = ReplacingMergeTree() ORDER BY (post_id, id)""",
            
            f"""CREATE TABLE IF NOT EXISTS {T_PROFILES} (
                user_id UInt32, username String, group_id UInt32,
                message_count UInt32, like_count UInt32,
                register_date DateTime, last_seen DateTime,
                balance String, hold String, currency String,
                is_banned UInt8, custom_title String,
                trophy_count UInt32, followers UInt32, followings UInt32,
                scraped_at DateTime DEFAULT now()
            ) ENGINE = ReplacingMergeTree() ORDER BY (user_id)""",
            
            f"""CREATE TABLE IF NOT EXISTS {T_PROFPOST} (
                id String, profile_post_id UInt32,
                timeline_user_id UInt32, timeline_username String,
                author String, post_date DateTime,
                content String, images String,
                like_count UInt32, comment_count UInt32,
                created_at DateTime DEFAULT now()
            ) ENGINE = ReplacingMergeTree() ORDER BY (profile_post_id)""",
            
            f"""CREATE TABLE IF NOT EXISTS {T_CHATBOX} (
                message_id UInt32, author String,
                message_date DateTime, content String,
                room_id UInt32, created_at DateTime DEFAULT now()
            ) ENGINE = ReplacingMergeTree() ORDER BY (message_id)""",
            
            f"""CREATE TABLE IF NOT EXISTS {T_CKPT} (
                key String, value String,
                updated_at DateTime DEFAULT now()
            ) ENGINE = ReplacingMergeTree() ORDER BY (key)""",
        ]
        for q in queries:
            try:
                self.ckpt_client.command(q)
            except Exception as e:
                logging.error(f"Table init error: {e}")
    
    def insert(self, table: str, columns: List[str], rows: List[Dict]):
        if not rows:
            return
        data = [[r[c] for r in rows] for c in columns]
        self.client.insert(table, data, column_names=columns, column_oriented=True)
    
    def save_ckpt(self, key: str, value: str):
        safe = str(value).replace("'", "")
        self.ckpt_client.command(
            f"INSERT INTO {T_CKPT} (key, value, updated_at) "
            f"VALUES ('{key}', '{safe}', now())"
        )
    
    def load_ckpt(self, key: str) -> Optional[str]:
        try:
            rows = self.ckpt_client.query(
                f"SELECT value FROM {T_CKPT} WHERE key='{key}' "
                f"ORDER BY updated_at DESC LIMIT 1"
            ).result_rows
            return rows[0][0] if rows else None
        except:
            return None
    
    def load_ckpt_int(self, key: str, default: int = 0) -> int:
        v = self.load_ckpt(key)
        try:
            return int(v) if v is not None else default
        except:
            return default
    
    def search(self, keyword: str, offset: int = 0, limit: int = None) -> Tuple[List, int]:
        if limit is None:
            limit = SEARCH_PAGE_SIZE
        safe = keyword.replace("'", "\\'")
        total = self.client.query(
            f"SELECT count() FROM {T_POSTS} "
            f"WHERE positionCaseInsensitive(content, '{safe}') > 0"
        ).result_rows[0][0]
        rows = self.client.query(
            f"SELECT post_id, thread_id, thread_title, forum_name, author, "
            f"post_date, content, images FROM {T_POSTS} "
            f"WHERE positionCaseInsensitive(content, '{safe}') > 0 "
            f"ORDER BY post_date DESC LIMIT {limit} OFFSET {offset}"
        ).result_rows
        return rows, total
    
    def get_post(self, post_id: int) -> Optional[Tuple]:
        rows = self.client.query(
            f"SELECT post_id, thread_id, thread_title, forum_name, author, "
            f"post_date, content, images FROM {T_POSTS} "
            f"WHERE post_id={post_id} LIMIT 1"
        ).result_rows
        return rows[0] if rows else None
    
    def get_comments(self, post_id: int, limit: int = 10) -> List[Tuple]:
        return self.client.query(
            f"SELECT author, comment_date, content FROM {T_COMMENTS} "
            f"WHERE post_id={post_id} ORDER BY comment_date ASC LIMIT {limit}"
        ).result_rows
    
    def stats(self) -> dict:
        def cnt(t):
            try:
                return self.client.query(f"SELECT count() FROM {t}").result_rows[0][0]
            except:
                return 0
        return {
            "posts": cnt(T_POSTS),
            "comments": cnt(T_COMMENTS),
            "profiles": cnt(T_PROFILES),
            "profile_posts": cnt(T_PROFPOST),
            "chatbox": cnt(T_CHATBOX),
        }
    
    def size_stats(self) -> Tuple[str, float]:
        sz = self.client.query(
            "SELECT formatReadableSize(sum(data_compressed_bytes)),"
            "round(sum(data_uncompressed_bytes)/sum(data_compressed_bytes),2) "
            "FROM system.parts WHERE active"
        ).result_rows
        size = sz[0][0] if sz and sz[0][0] else "0 B"
        ratio = sz[0][1] if sz and len(sz[0]) > 1 else 1.0
        return size, ratio
    
    def top_forums(self, limit: int = 5) -> List[Tuple]:
        return self.client.query(
            f"SELECT forum_name, count() cnt FROM {T_POSTS} "
            f"GROUP BY forum_name ORDER BY cnt DESC LIMIT {limit}"
        ).result_rows
    
    def clear_all(self):
        for t in [T_POSTS, T_COMMENTS, T_PROFILES, T_PROFPOST, T_CHATBOX]:
            try:
                self.client.command(f"TRUNCATE TABLE {t}")
            except Exception as e:
                logging.error(f"Truncate error {t}: {e}")

db = Database()