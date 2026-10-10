"""Optional dev helper (not part of the graded app): fills MongoDB with ~100k awkward attendance records.

    python tools/gen_data.py            # DROPS employees + attendance_logs in $MONGO_DB, then reloads

Contains on purpose: employees with no logs, a mid-month joiner, an overnight-shift team, weekend records,
missing weekdays, ABSENT/LEAVE/ON_DUTY/WFH records, half days, legacy docs (no history/half_day), logs of an
unknown employee, and many ties in late minutes.
"""
import os
import random
import sys
from datetime import date, datetime, timedelta, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from app.main import IST, UTC, attendance_date, derive_fields, shift_window  # noqa: E402
from dotenv import load_dotenv  # noqa: E402
from pymongo import MongoClient  # noqa: E402

load_dotenv()
random.seed(7)
db = MongoClient(os.getenv("MONGO_URI", "mongodb://localhost:27017"), tz_aware=True)[os.getenv("MONGO_DB", "attendance_db")]
db.employees.drop()
db.attendance_logs.drop()

DEPTS = ["Engineering", "Sales", "Support", "HR", "Ops"]
emps = []
for i in range(1, 241):
    shift = ("22:00", "06:00") if i % 17 == 0 else ("09:30", "18:30")
    joined = "2025-01-01"
    if i % 29 == 0:
        joined = "2026-07-15"  # mid-month joiner
    if i % 41 == 0:
        joined = "2026-12-01"  # has not joined yet
    emps.append({"emp_code": f"EMP{i:04d}", "name": f"Employee {i}", "email": f"e{i}@example.com",
                 "department": DEPTS[i % 5], "shift_start": shift[0], "shift_end": shift[1],
                 "joined_on": joined, "created_at": datetime(2025, 1, 1, tzinfo=UTC)})
db.employees.insert_many(emps)
NO_LOGS = {"EMP0007", "EMP0014"}

logs = []
day, last = date(2025, 1, 1), date(2026, 10, 9)
while day <= last:
    weekend = day.weekday() >= 5
    for e in emps:
        if e["emp_code"] in NO_LOGS or e["joined_on"] > day.isoformat():
            continue
        if weekend and random.random() > 0.01:
            continue
        if not weekend and random.random() < 0.08:
            continue  # day simply not recorded
        r = random.random()
        base = {"emp_code": e["emp_code"], "date": day.isoformat(), "history": []}
        if r < 0.04:
            logs.append({**base, "status": "ABSENT", "punch_in": None, "punch_out": None, "work_hours": None,
                         "late_minutes": 0, "overtime_minutes": 0, "half_day": False})
            continue
        if r < 0.08:
            logs.append({**base, "status": "LEAVE", "punch_in": None, "punch_out": None, "work_hours": None,
                         "late_minutes": 0, "overtime_minutes": 0, "half_day": False})
            continue
        status = random.choices(["PRESENT", "WFH", "ON_DUTY"], [80, 15, 5])[0]
        start, end = shift_window(day, e["shift_start"], e["shift_end"])
        pin = start + timedelta(minutes=random.choice([-20, -5, 0, 5, 10, 11, 15, 30, 45, 45, 60]), seconds=random.choice([0, 0, 1, 30]))
        if random.random() < 0.05:
            pout = pin + timedelta(hours=random.choice([2, 3, 4]), minutes=random.choice([0, 30]))
        else:
            pout = end + timedelta(minutes=random.choice([-30, 0, 10, 35, 60, 90]))
        pin, pout = pin.astimezone(UTC), pout.astimezone(UTC)
        assert attendance_date(pin, e["shift_start"], e["shift_end"]) == day or e["shift_start"] > e["shift_end"]
        doc = {**base, "status": status, "punch_in": pin, "punch_out": pout,
               **derive_fields(e, day.isoformat(), status, pin, pout)}
        if random.random() < 0.01:
            del doc["history"], doc["half_day"]  # legacy shape
        logs.append(doc)
    day += timedelta(days=1)
logs.append({"emp_code": "EMP9999", "date": "2026-07-06", "status": "PRESENT", "punch_in": datetime(2026, 7, 6, 4, tzinfo=UTC),
             "punch_out": None, "work_hours": None, "late_minutes": 120, "overtime_minutes": 0, "half_day": False,
             "history": []})  # log of an employee that does not exist: must be ignored by the leaderboard
for i in range(0, len(logs), 5000):
    db.attendance_logs.insert_many(logs[i:i + 5000])
print(f"loaded {len(emps)} employees and {len(logs)} attendance records into '{db.name}'")
print("now start the app (it creates the indexes) and call /admin/explain/<endpoint>")
