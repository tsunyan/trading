"""Official GMO FX special closures, each reviewed against the broker's own notice.

A window lists the hourly bars wholly inside the announced closure, as a half-open JST
interval. Only add a window after reading the official notice; never infer one from a
missing bar, which may equally be an API outage.
"""

from datetime import datetime, timedelta

import pandas as pd

FX_SPECIAL_CLOSURES = (
    {
        "first_missing_bar_jst": "2023-12-25T07:00:00+09:00",
        "resume_jst": "2023-12-26T07:00:00+09:00",
        "hours": 24,
        "source": "https://coin.z.com/jp/news/2023/12/12212/",
        "published": "2023-12-19",
    },
    {
        "first_missing_bar_jst": "2024-01-01T07:00:00+09:00",
        "resume_jst": "2024-01-02T07:00:00+09:00",
        "hours": 24,
        "source": "https://coin.z.com/jp/news/2023/12/12212/",
        "published": "2023-12-19",
    },
    {
        "first_missing_bar_jst": "2024-12-25T06:00:00+09:00",
        "resume_jst": "2024-12-26T07:00:00+09:00",
        "hours": 25,
        "source": "https://coin.z.com/jp/news/2024/12/13869/",
        "published": "2024-12-23",
    },
    {
        "first_missing_bar_jst": "2025-01-01T06:00:00+09:00",
        "resume_jst": "2025-01-02T07:00:00+09:00",
        "hours": 25,
        "source": "https://coin.z.com/jp/news/2024/12/13869/",
        "published": "2024-12-23",
    },
    {
        "first_missing_bar_jst": "2025-12-25T06:00:00+09:00",
        "resume_jst": "2025-12-26T07:00:00+09:00",
        "hours": 25,
        "source": "https://coin.z.com/jp/news/2025/12/15340/",
        "published": "2025-12-19",
    },
    {
        "first_missing_bar_jst": "2026-01-01T06:00:00+09:00",
        "resume_jst": "2026-01-02T07:00:00+09:00",
        "hours": 25,
        "source": "https://coin.z.com/jp/news/2025/12/15340/",
        "published": "2025-12-19",
    },
)


def _windows(windows):
    parsed = []
    for window in windows:
        start = datetime.fromisoformat(window["first_missing_bar_jst"])
        end = datetime.fromisoformat(window["resume_jst"])
        if (
            start.utcoffset() != timedelta(hours=9)
            or end.utcoffset() != timedelta(hours=9)
            or start.minute
            or start.second
            or end.minute
            or end.second
            or end - start != timedelta(hours=window["hours"])
            or not window["source"].startswith("https://coin.z.com/jp/")
            or datetime.fromisoformat(window["published"]).date() > start.date()
        ):
            raise ValueError("invalid_special_closure_window")
        parsed.append((pd.Timestamp(start).tz_convert("UTC"), pd.Timestamp(end), window))
    parsed.sort(key=lambda item: item[0])
    if any(a[1] > b[0] for a, b in zip(parsed, parsed[1:], strict=False)):
        raise ValueError("overlapping_special_closure_windows")
    return tuple((start, end.tz_convert("UTC"), window) for start, end, window in parsed)


_FX_WINDOWS = _windows(FX_SPECIAL_CLOSURES)


def special_fx_closure(timestamp: pd.Timestamp) -> dict | None:
    """The reviewed window whose closure wholly contains this hourly bar, if any."""
    for start, end, window in _FX_WINDOWS:
        if start <= timestamp < end:
            return window
    return None
