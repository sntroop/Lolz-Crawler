from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional, Dict, Any

@dataclass
class Post:
    id: str
    forum_id: int
    forum_name: str
    thread_id: int
    thread_title: str
    post_id: int
    author: str
    post_date: datetime
    content: str
    images: List[str]
    
    @classmethod
    def from_api(cls, forum_id: int, forum_name: str, thread_id: int, 
                 thread_title: str, post: Dict[str, Any]) -> Optional['Post']:
        from utils import clean_bbcode
        body, images = clean_bbcode(
            post.get("post_body") or post.get("post_body_plain_text") or ""
        )
        if not body or len(body) < 10:
            return None
            
        post_id = post.get("post_id", 0)
        author = post.get("poster_username", "unknown")
        ts = post.get("post_create_date", 0)
        dt = datetime.utcfromtimestamp(ts) if ts else datetime(2000, 1, 1)
        
        content = (
            f"[Lolz] {forum_name} | {thread_title}\n"
            f"Автор: {author} | {dt.strftime('%d.%m.%Y %H:%M')} | "
            f"thread={thread_id} post={post_id}\n\n{body}"
        )
        
        return cls(
            id=cls._generate_id(content),
            forum_id=forum_id,
            forum_name=forum_name,
            thread_id=thread_id,
            thread_title=thread_title,
            post_id=post_id,
            author=author,
            post_date=dt,
            content=content,
            images=images
        )
    
    @staticmethod
    def _generate_id(content: str) -> str:
        import hashlib
        return hashlib.md5(content.encode()).hexdigest()
    
    def to_dict(self) -> Dict[str, Any]:
        import json
        return {
            "id": self.id,
            "forum_id": self.forum_id,
            "forum_name": self.forum_name,
            "thread_id": self.thread_id,
            "thread_title": self.thread_title,
            "post_id": self.post_id,
            "author": self.author,
            "post_date": self.post_date,
            "content": self.content,
            "images": json.dumps(self.images, ensure_ascii=False)
        }

@dataclass
class Comment:
    id: str
    post_id: int
    thread_id: int
    author: str
    comment_date: datetime
    content: str
    
    @classmethod
    def from_api(cls, post_id: int, thread_id: int, cmt: Dict[str, Any]) -> Optional['Comment']:
        from utils import clean_bbcode
        raw = cmt.get("post_comment_body") or cmt.get("post_comment_body_plain_text") or ""
        body, _ = clean_bbcode(raw)
        if not body or len(body) < 2:
            return None
            
        author = cmt.get("poster_username", "unknown")
        ts = cmt.get("post_comment_create_date", 0)
        dt = datetime.utcfromtimestamp(ts) if ts else datetime(2000, 1, 1)
        uid = str(cmt.get("post_comment_id", 0))
        
        return cls(
            id=hashlib.md5(f"{post_id}{uid}".encode()).hexdigest(),
            post_id=post_id,
            thread_id=thread_id,
            author=author,
            comment_date=dt,
            content=body
        )
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "post_id": self.post_id,
            "thread_id": self.thread_id,
            "author": self.author,
            "comment_date": self.comment_date,
            "content": self.content
        }

@dataclass
class Profile:
    user_id: int
    username: str
    group_id: int
    message_count: int
    like_count: int
    register_date: datetime
    last_seen: datetime
    balance: str
    hold: str
    currency: str
    is_banned: bool
    custom_title: str
    trophy_count: int
    followers: int
    followings: int
    
    @classmethod
    def from_api(cls, u: Dict[str, Any]) -> Optional['Profile']:
        uid = u.get("user_id")
        if not uid:
            return None
            
        reg = u.get("user_register_date", 0)
        seen = u.get("user_last_seen_date", 0)
        fol = u.get("user_followers", {})
        fing = u.get("user_following", {})
        
        return cls(
            user_id=uid,
            username=u.get("username", ""),
            group_id=u.get("user_group_id", 0),
            message_count=u.get("user_message_count", 0),
            like_count=u.get("user_like_count", 0),
            register_date=datetime.utcfromtimestamp(reg) if reg else datetime(2000, 1, 1),
            last_seen=datetime.utcfromtimestamp(seen) if seen else datetime(2000, 1, 1),
            balance=str(u.get("balance", "")),
            hold=str(u.get("hold", "")),
            currency=str(u.get("currency", "")),
            is_banned=bool(u.get("is_banned", 0)),
            custom_title=str(u.get("custom_title", "")),
            trophy_count=u.get("trophy_count", 0),
            followers=fol.get("total", 0) if isinstance(fol, dict) else 0,
            followings=fing.get("total", 0) if isinstance(fing, dict) else 0
        )
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "user_id": self.user_id,
            "username": self.username,
            "group_id": self.group_id,
            "message_count": self.message_count,
            "like_count": self.like_count,
            "register_date": self.register_date,
            "last_seen": self.last_seen,
            "balance": self.balance,
            "hold": self.hold,
            "currency": self.currency,
            "is_banned": int(self.is_banned),
            "custom_title": self.custom_title,
            "trophy_count": self.trophy_count,
            "followers": self.followers,
            "followings": self.followings
        }