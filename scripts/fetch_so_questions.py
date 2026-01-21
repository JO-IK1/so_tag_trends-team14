import time
import datetime as dt
from typing import Dict, List, Any

import requests
import pandas as pd

BASE_URL = "https://api.stackexchange.com/2.3/questions"
SITE = "stackoverflow"

TAGS = ["android", "ios", "flutter"]

# Новый период: 01.11.2025–31.12.2025
DATE_FROM = dt.datetime(2025, 11, 1)
DATE_TO   = dt.datetime(2025, 12, 31, 23, 59, 59)  # включительно

# Если потом захочешь повысить лимиты — можно добавить key (опционально)
API_KEY = None  # например: "YOUR_KEY_HERE"


def to_unix_seconds(d: dt.datetime) -> int:
    """Datetime -> unix timestamp (seconds)."""
    return int(d.replace(tzinfo=dt.timezone.utc).timestamp())


def fetch_questions_for_tag(tag: str, from_ts: int, to_ts: int) -> List[Dict[str, Any]]:
    """
    Скачивает ВСЕ вопросы с одним тегом за период [from_ts, to_ts] (unix seconds),
    обрабатывая пагинацию и backoff.
    """
    all_items: List[Dict[str, Any]] = []
    page = 1

    while True:
        params = {
            "site": SITE,
            "tagged": tag,          # Важно: один тег за раз
            "fromdate": from_ts,
            "todate": to_ts,
            "pagesize": 100,        # максимум
            "page": page,
            "order": "asc",
            "sort": "creation",
            # "filter": "default"   # нам достаточно дефолта
        }
        if API_KEY:
            params["key"] = API_KEY

        r = requests.get(BASE_URL, params=params, timeout=60)
        r.raise_for_status()
        data = r.json()

        # StackExchange API может сказать "подождать N секунд"
        backoff = data.get("backoff")
        if backoff:
            time.sleep(int(backoff))

        items = data.get("items", [])
        all_items.extend(items)

        has_more = data.get("has_more", False)
        quota_remaining = data.get("quota_remaining", None)

        print(f"[{tag}] page={page} items={len(items)} has_more={has_more} quota_remaining={quota_remaining}")

        if not has_more or len(items) == 0:
            break

        page += 1
        time.sleep(0.2)  # маленькая пауза, чтобы быть вежливыми к API

    return all_items


def normalize_questions(items: List[Dict[str, Any]], main_tag: str) -> pd.DataFrame:
    """
    Превращает items в таблицу.
    main_tag = тот тег, по которому мы делали запрос (android/ios/flutter),
    чтобы потом считать статистику по нему даже если вопрос имеет несколько тегов.
    """
    rows = []
    for it in items:
        creation_ts = it.get("creation_date")
        if not creation_ts:
            continue

        creation_dt = dt.datetime.fromtimestamp(creation_ts, tz=dt.timezone.utc)

        rows.append({
            "question_id": it.get("question_id"),
            "main_tag": main_tag,
            "creation_datetime_utc": creation_dt.isoformat(),
            "creation_date_utc": creation_dt.date().isoformat(),
            "weekday_utc": creation_dt.weekday(),  # 0=Mon ... 6=Sun
            "score": it.get("score"),
            "answer_count": it.get("answer_count"),
            "is_answered": it.get("is_answered"),
            "view_count": it.get("view_count"),
            "title": it.get("title"),
            "link": it.get("link"),
            "tags": ";".join(it.get("tags", [])),  # все теги вопроса
        })

    return pd.DataFrame(rows)


def main():
    from_ts = to_unix_seconds(DATE_FROM)
    to_ts = to_unix_seconds(DATE_TO)

    all_df = []
    for tag in TAGS:
        items = fetch_questions_for_tag(tag, from_ts, to_ts)
        df = normalize_questions(items, main_tag=tag)
        all_df.append(df)

    raw = pd.concat(all_df, ignore_index=True) if all_df else pd.DataFrame()
    print("TOTAL RAW ROWS:", len(raw))

    # На всякий случай удалим возможные дубликаты (иногда бывают повторы при пагинации/граничных датах)
    raw = raw.drop_duplicates(subset=["question_id", "main_tag"]).reset_index(drop=True)

    # --- Сохранение сырых данных ---
    import os
    os.makedirs("data", exist_ok=True)
    raw.to_csv("data/questions_raw.csv", index=False)
    print("Saved: data/questions_raw.csv")

    # --- Агрегация по дням (как требует ТЗ) ---
    by_day = (raw
              .groupby(["creation_date_utc", "main_tag"], as_index=False)
              .agg(questions_count=("question_id", "count"))
              .rename(columns={"creation_date_utc": "date", "main_tag": "tag"}))

    by_day.to_csv("data/questions_by_day.csv", index=False)
    print("Saved: data/questions_by_day.csv")

    # --- Агрегация по дням недели (сезонность) ---
    # weekday_utc: 0=Mon ... 6=Sun
    by_weekday = (raw
                  .groupby(["weekday_utc", "main_tag"], as_index=False)
                  .agg(questions_count=("question_id", "count"))
                  .rename(columns={"weekday_utc": "weekday", "main_tag": "tag"}))

    # Для удобства добавим название дня
    weekday_names = {0: "Mon", 1: "Tue", 2: "Wed", 3: "Thu", 4: "Fri", 5: "Sat", 6: "Sun"}
    by_weekday["weekday_name"] = by_weekday["weekday"].map(weekday_names)

    by_weekday.to_csv("data/questions_by_weekday.csv", index=False)
    print("Saved: data/questions_by_weekday.csv")

    print("\nDone. Next: visualize in DataLens and compute moving averages.")


if __name__ == "__main__":
    main()
