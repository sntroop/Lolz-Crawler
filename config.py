import os
from dotenv import load_dotenv

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")
ADMIN_ID = int(os.getenv("ADMIN_ID", "0"))
LOLZ_TOKEN = os.getenv("LOLZ_TOKEN")
LOLZ_API = "https://prod-api.lolz.live"

CH_HOST = os.getenv("CH_HOST", "localhost")
CH_USER = os.getenv("CH_USER", "default")
CH_PASS = os.getenv("CH_PASSWORD", "")
CH_DB = os.getenv("CH_DATABASE", "default")

T_POSTS = "lolz_posts"
T_COMMENTS = "lolz_comments"
T_PROFILES = "lolz_profiles"
T_PROFPOST = "lolz_profile_posts"
T_CHATBOX = "lolz_chatbox"
T_CKPT = "lolz_crawler_checkpoint"

CRAWLER_CONCURRENCY = int(os.getenv("CRAWLER_CONCURRENCY", "64"))
CRAWLER_BATCH_SIZE = int(os.getenv("CRAWLER_BATCH_SIZE", "10000"))
CRAWLER_UPDATE_INTERVAL = int(os.getenv("CRAWLER_UPDATE_INTERVAL", "15"))
THREADS_PER_PAGE = int(os.getenv("THREADS_PER_PAGE", "50"))
POSTS_PER_PAGE = int(os.getenv("POSTS_PER_PAGE", "100"))
USERS_PER_PAGE = int(os.getenv("USERS_PER_PAGE", "50"))
CHATBOX_POLL_INTERVAL = int(os.getenv("CHATBOX_POLL_INTERVAL", "30"))
COMMENT_CONCURRENCY = int(os.getenv("COMMENT_CONCURRENCY", "8"))

RATE_MIN_DELAY = float(os.getenv("RATE_MIN_DELAY", "0.05"))
RATE_MAX_DELAY = float(os.getenv("RATE_MAX_DELAY", "60.0"))
RATE_BACKOFF_FACTOR = float(os.getenv("RATE_BACKOFF_FACTOR", "2.0"))
RATE_RECOVERY_STEP = float(os.getenv("RATE_RECOVERY_STEP", "0.8"))

CPU_THROTTLE_THRESHOLD = int(os.getenv("CPU_THROTTLE_THRESHOLD", "80"))
CPU_CRITICAL_THRESHOLD = int(os.getenv("CPU_CRITICAL_THRESHOLD", "95"))
RAM_THROTTLE_THRESHOLD = int(os.getenv("RAM_THROTTLE_THRESHOLD", "80"))
THROTTLE_CHECK_INTERVAL = int(os.getenv("THROTTLE_CHECK_INTERVAL", "5"))
THROTTLE_SLEEP_STEP = float(os.getenv("THROTTLE_SLEEP_STEP", "0.5"))

SEARCH_PAGE_SIZE = int(os.getenv("SEARCH_PAGE_SIZE", "5"))

if not BOT_TOKEN:
    raise ValueError("BOT_TOKEN not set")
if not ADMIN_ID:
    raise ValueError("ADMIN_ID not set")
if not LOLZ_TOKEN:
    raise ValueError("LOLZ_TOKEN not set")