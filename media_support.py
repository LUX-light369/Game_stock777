
from datetime import timedelta

CARD_LIFETIME_DAYS = 2

def is_recent_card(now_func, registered_at):
    return (now_func() - registered_at) <= timedelta(days=CARD_LIFETIME_DAYS)
