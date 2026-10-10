"""Pure unit tests for the business rules (no MongoDB needed):  pytest tests/"""
from datetime import datetime, timedelta, timezone, date

from app.main import (IST, UTC, attendance_date, compute_half_day, compute_late_minutes, compute_overtime,
                      compute_work_hours, count_working_days, derive_fields, dt_to_ms, ms_to_ts, month_bounds,
                      round_half_up, shift_window)

DAY = date(2026, 7, 6)  # a Monday


def ist(h, m, s=0, d=DAY):
    return datetime(d.year, d.month, d.day, h, m, s, tzinfo=IST)


def late(h, m, s):
    start, _ = shift_window(DAY, "09:30", "18:30")
    return compute_late_minutes(ist(h, m, s), start)


def test_r2_examples():
    assert late(9, 40, 0) == 0
    assert late(9, 40, 1) == 10
    assert late(10, 15, 59) == 45
    assert late(9, 0, 0) == 0  # early


def test_r3_overtime():
    _, end = shift_window(DAY, "09:30", "18:30")
    assert compute_overtime(ist(19, 0), end) == 30
    assert compute_overtime(ist(18, 59, 59), end) == 0  # 29 min 59 s
    assert compute_overtime(ist(19, 10), end) == 40
    assert compute_overtime(ist(18, 0), end) == 0


def test_r4_half_up():
    a = ist(9, 0)
    assert compute_work_hours(a, a + timedelta(seconds=4 * 3600 + 1800 + 18)) == 4.51  # 4.505 -> half-up
    assert compute_work_hours(a, a + timedelta(hours=9, minutes=7)) == 9.12
    assert round_half_up(0.03125, 4) == round_half_up("0.0313", 4)
    assert float(round_half_up(2.675, 2)) == 2.68  # float round() would give 2.67


def test_r5_half_day():
    assert compute_half_day(4.49) is True
    assert compute_half_day(4.5) is False
    assert compute_half_day(None) is False


def test_r1_overnight_date_and_window():
    # shift 22:00-06:00; punch-in 02:00 IST on the 7th belongs to the shift that started on the 6th
    ts = ist(2, 0, d=date(2026, 7, 7)).astimezone(UTC)
    assert attendance_date(ts, "22:00", "06:00") == date(2026, 7, 6)
    assert attendance_date(ist(21, 55).astimezone(UTC), "22:00", "06:00") == DAY
    assert attendance_date(ist(6, 0, d=date(2026, 7, 7)).astimezone(UTC), "22:00", "06:00") == date(2026, 7, 7)
    start, end = shift_window(DAY, "22:00", "06:00")
    assert end - start == timedelta(hours=8)
    # a day shift is unaffected, and 00:30 IST is still its own calendar day
    assert attendance_date(ist(0, 30).astimezone(UTC), "09:30", "18:30") == DAY


def test_ist_date_differs_from_utc_date():
    ts = ist(1, 0).astimezone(UTC)  # 01:00 IST = previous day in UTC
    assert ts.date() == date(2026, 7, 5)
    assert attendance_date(ts, "09:30", "18:30") == DAY


def test_sample_overnight_record():
    emp = {"shift_start": "22:00", "shift_end": "06:00"}
    pin = datetime(2026, 7, 6, 16, 25, tzinfo=UTC)
    pout = datetime(2026, 7, 7, 1, 10, tzinfo=UTC)
    d = derive_fields(emp, "2026-07-06", "PRESENT", pin, pout)
    assert d == {"work_hours": 8.75, "late_minutes": 0, "overtime_minutes": 40, "half_day": False}


def test_ms_conversion_truncates_seconds():
    assert ms_to_ts(1783312500900) == ms_to_ts(1783312500000)
    assert dt_to_ms(ms_to_ts(1783312500900)) == 1783312500000


def test_working_days():
    first, last = month_bounds("2026-07")
    assert count_working_days(first, last) == 23
    assert count_working_days(date(2026, 7, 20), last) == 10
    assert count_working_days(date(2026, 7, 25), date(2026, 7, 26)) == 0
