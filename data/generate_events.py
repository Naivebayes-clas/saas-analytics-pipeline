import pandas as pd
import numpy as np
import json
from datetime import datetime, timedelta
import os

np.random.seed(42)
n = 10_000

events = pd.DataFrame({
    "event_id":     [f"evt_{i:06d}" for i in range(n)],
    "user_id":      np.random.choice([f"usr_{i:04d}" for i in range(500)], n),
    "event_type":   np.random.choice(["page_view", "feature_click", "export", "share", "signup"], n, p=[0.4, 0.25, 0.15, 0.1, 0.1]),
    "feature_used": np.random.choice(["dashboard", "query_builder", "alert", "report", None], n),
    "plan_tier":    np.random.choice(["free", "pro", "enterprise"], n, p=[0.3, 0.5, 0.2]),
    "session_id":   [f"sess_{np.random.randint(1000,9999)}" for _ in range(n)],
    "page_url":     np.random.choice(["/home", "/dashboard", "/reports", "/settings", "/pricing"], n),
    "device_type":  np.random.choice(["desktop", "mobile", "tablet"], n, p=[0.7, 0.2, 0.1]),
    "country":      np.random.choice(["US", "IN", "GB", "DE", "CA", "AU"], n),
    "event_ts":     [datetime.now() - timedelta(seconds=np.random.randint(0, 90*86400)) for _ in range(n)],
})

users = pd.DataFrame({
    "user_id":      [f"usr_{i:04d}" for i in range(500)],
    "email":        [f"user{i}@example.com" for i in range(500)],
    "plan_tier":    np.random.choice(["free", "pro", "enterprise"], 500, p=[0.3, 0.5, 0.2]),
    "company_size": np.random.choice(["1-10", "11-50", "51-200", "200+"], 500),
    "signup_ts":    [datetime.now() - timedelta(days=np.random.randint(1, 365)) for _ in range(500)],
})

os.makedirs("data", exist_ok=True)

# Write events as JSON (one file per "day")
for day in range(30):
    mask = (events["event_ts"] >= datetime.now() - timedelta(days=day+1)) & \
           (events["event_ts"] < datetime.now() - timedelta(days=day))
    chunk = events[mask]
    if not chunk.empty:
        chunk.to_json(f"data/events_{day:02d}.json", orient="records", lines=True)

# Write users as JSON
users.to_json("data/users.json", orient="records", lines=True)

print(f"Generated {n} events, 500 users, 30 daily JSON files")   
