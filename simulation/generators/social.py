import uuid
import random
from datetime import datetime, timezone
from faker import Faker

fake = Faker()

PLATFORMS = ["twitter", "instagram", "facebook", "tiktok", "linkedin", "reddit"]
CONTENT_TYPES = ["post", "reel", "story", "comment", "share", "review"]
TOPICS = ["tech", "food", "travel", "fashion", "sports", "politics", "entertainment", "finance"]
SENTIMENTS = ["positive", "neutral", "negative"]


def generate_post() -> dict:
    platform = random.choice(PLATFORMS)
    likes = int(random.exponential(50))
    shares = int(random.exponential(10))
    comments = int(random.exponential(20))
    return {
        "post_id": str(uuid.uuid4()),
        "user_id": str(uuid.uuid4())[:8],
        "platform": platform,
        "content_type": random.choice(CONTENT_TYPES),
        "topic": random.choice(TOPICS),
        "text_length": random.randint(10, 500),
        "likes": likes,
        "shares": shares,
        "comments": comments,
        "engagement_score": round((likes + shares * 3 + comments * 2) / max(1, random.randint(100, 10000)), 4),
        "sentiment": random.choice(SENTIMENTS),
        "is_sponsored": random.random() < 0.05,
        "hashtag_count": random.randint(0, 15),
        "follower_count": int(random.exponential(500)),
        "event_ts": datetime.now(timezone.utc).isoformat(),
        "event_type": "social_post",
        "source": "social",
    }


def generate_event() -> dict:
    return generate_post()
