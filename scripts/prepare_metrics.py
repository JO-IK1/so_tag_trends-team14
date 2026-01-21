import pandas as pd
import datetime as dt
import os

# Период (как договорились)
DATE_FROM = dt.date(2025, 11, 1)
DATE_TO   = dt.date(2025, 12, 31)

MA_WINDOW = 7  # moving average 7 days

INPUT_PATH = "data/questions_by_day.csv"
OUT_FILLED = "data/questions_by_day_filled.csv"
OUT_MA     = "data/questions_by_day_ma7.csv"


def main():
    df = pd.read_csv(INPUT_PATH)

    # Приводим дату к datetime
    df["date"] = pd.to_datetime(df["date"])
    df["tag"] = df["tag"].astype(str)

    # Список тегов
    tags = sorted(df["tag"].unique().tolist())

    # Полный диапазон дат
    full_dates = pd.date_range(start=pd.to_datetime(DATE_FROM), end=pd.to_datetime(DATE_TO), freq="D")

    # Дозаполняем: для каждого тега должны быть все даты (если где-то нет — ставим 0)
    full_index = pd.MultiIndex.from_product([full_dates, tags], names=["date", "tag"])
    filled = (
        df.set_index(["date", "tag"])
          .reindex(full_index)
          .fillna({"questions_count": 0})
          .reset_index()
    )

    # На всякий случай типы
    filled["questions_count"] = filled["questions_count"].astype(int)

    # Считаем MA(7) отдельно по тегам (rolling по времени)
    filled = filled.sort_values(["tag", "date"]).reset_index(drop=True)
    filled["ma7"] = (
        filled.groupby("tag")["questions_count"]
              .rolling(window=MA_WINDOW, min_periods=1)
              .mean()
              .reset_index(level=0, drop=True)
    )

    os.makedirs("data", exist_ok=True)

    # 1) Файл с дозаполненными датами
    filled[["date", "tag", "questions_count"]].to_csv(OUT_FILLED, index=False)

    # 2) Файл с MA7 (для графиков)
    filled[["date", "tag", "questions_count", "ma7"]].to_csv(OUT_MA, index=False)

    print("Saved:", OUT_FILLED)
    print("Saved:", OUT_MA)

    # Быстрый контроль
    print("\nRows:", len(filled))
    print("Tags:", tags)
    print("Date range:", filled["date"].min().date(), "->", filled["date"].max().date())
    print("\nSample:")
    print(filled.head(10))


if __name__ == "__main__":
    main()
