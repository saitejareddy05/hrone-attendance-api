"""
Employee Attendance & Analytics API  (FastAPI + MongoDB)

Run:  uvicorn app.main:app --port 8000
Env:  MONGO_URI, MONGO_DB  (a local .env is read for convenience; real environment variables win)

Layout of this file
  1. configuration, database handles, indexes
  2. time helpers and business rules R1-R5 (pure functions, no database)
  3. request models and serializers
  4. employees / attendance endpoints
  5. aggregation pipeline builders and analytics endpoints
  6. explain endpoint
"""
import calendar
import json
import logging
import os
import re
import threading
from contextlib import asynccontextmanager
from datetime import date as Date
from datetime import datetime, time as dtime, timedelta, timezone
from decimal import ROUND_HALF_UP, Decimal
from typing import Annotated, Any, Literal, Optional

from bson import json_util
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Path, Query, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field, field_validator, model_validator
from pymongo import ASCENDING, DESCENDING, MongoClient, ReturnDocument
from pymongo.errors import DuplicateKeyError, PyMongoError, ServerSelectionTimeoutError

load_dotenv(override=False)  # never let a .env file override real environment variables

log = logging.getLogger("attendance")

# --------------------------------------------------------------------------- #
# 1. Configuration, database handles, indexes
# --------------------------------------------------------------------------- #
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB = os.getenv("MONGO_DB", "attendance_db")

# tz_aware=True: PyMongo returns timezone-aware (UTC) datetimes instead of naive ones.
client = MongoClient(
    MONGO_URI,
    tz_aware=True,
    tzinfo=timezone.utc,
    serverSelectionTimeoutMS=5000,
    maxPoolSize=100,
)
db = client[MONGO_DB]
EMPLOYEES = db["employees"]
LOGS = db["attendance_logs"]

INDEXES = {
    "employees": [
        # uniqueness of emp_code is enforced by the database, not by check-then-insert
        ([("emp_code", ASCENDING)], {"unique": True, "name": "emp_code_uq"}),
        # GET /employees?department=..  (filter + sort by emp_code)
        ([("department", ASCENDING), ("emp_code", ASCENDING)], {"name": "emp_dept_code"}),
        # headcount: joined_on <= end of period
        ([("joined_on", ASCENDING)], {"name": "emp_joined_on"}),
    ],
    "attendance_logs": [
        # one record per (emp_code, date): punch-in race guard, PATCH lookup, monthly summary,
        # $lookup from the department summary, emp_code filter of the list endpoint
        ([("emp_code", ASCENDING), ("date", ASCENDING)], {"unique": True, "name": "log_emp_date_uq"}),
        # GET /attendance without emp_code: sort date desc, emp_code asc, date range filter
        ([("date", DESCENDING), ("emp_code", ASCENDING)], {"name": "log_date_emp"}),
        # GET /attendance?status=..
        ([("status", ASCENDING), ("date", DESCENDING), ("emp_code", ASCENDING)], {"name": "log_status_date_emp"}),
        # late leaderboard: month range + late_minutes > 0, and it covers the $group (emp_code)
        ([("date", ASCENDING), ("late_minutes", ASCENDING), ("emp_code", ASCENDING)], {"name": "log_late_cover"}),
        # punch-out: latest record of an employee with punch_in <= punched_at
        ([("emp_code", ASCENDING), ("punch_in", DESCENDING)], {"name": "log_emp_punchin"}),
    ],
}

_index_lock = threading.Lock()
_indexes_ready = False


def ensure_indexes() -> bool:
    """Create every index (idempotent). Never raises; returns True when all exist."""
    global _indexes_ready
    with _index_lock:
        if _indexes_ready:
            return True
        ok = True
        for coll, specs in INDEXES.items():
            for keys, opts in specs:
                try:
                    db[coll].create_index(keys, **opts)
                except ServerSelectionTimeoutError as exc:  # MongoDB unreachable: stop waiting, /health retries
                    log.warning("MongoDB unreachable, indexes deferred: %s", exc)
                    return False
                except PyMongoError as exc:  # keep the service up; /health retries later
                    ok = False
                    log.warning("index %s.%s not created: %s", coll, opts.get("name"), exc)
        _indexes_ready = ok
        return ok


@asynccontextmanager
async def lifespan(_: FastAPI):
    ensure_indexes()
    yield


app = FastAPI(title="Employee Attendance & Analytics API", version="2.0.0", lifespan=lifespan)


@app.exception_handler(PyMongoError)
async def mongo_error_handler(_: Request, exc: PyMongoError):
    return JSONResponse(status_code=503, content={"detail": "database error"})


# --------------------------------------------------------------------------- #
# 2. Time helpers and business rules
# --------------------------------------------------------------------------- #
UTC = timezone.utc
IST = timezone(timedelta(hours=5, minutes=30))
EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
MS_MIN, MS_MAX = 100_000_000_000, 4_102_444_800_000
PRESENCE = ("PRESENT", "WFH", "ON_DUTY")
NO_PUNCH = ("ABSENT", "LEAVE")
GRACE_SECONDS = 10 * 60
MIN_OVERTIME_MINUTES = 30
HALF_DAY_LIMIT = Decimal("4.50")

DATE_RE = r"^[0-9]{4}-[0-9]{2}-[0-9]{2}$"
MONTH_RE = r"^[0-9]{4}-(0[1-9]|1[0-2])$"
TIME_RE = r"^([01][0-9]|2[0-3]):[0-5][0-9]$"


def unprocessable(msg: str) -> HTTPException:
    return HTTPException(status_code=422, detail=msg)


def aware(dt: datetime) -> datetime:
    return dt if dt.tzinfo is not None else dt.replace(tzinfo=UTC)


def ms_to_ts(ms: int) -> datetime:
    """Epoch ms -> aware UTC datetime truncated to whole seconds (R1)."""
    return (EPOCH + timedelta(milliseconds=ms)).replace(microsecond=0)


def now_ts() -> datetime:
    return datetime.now(UTC).replace(microsecond=0)


def dt_to_ms(dt: datetime) -> int:
    return (aware(dt) - EPOCH) // timedelta(milliseconds=1)


def opt_ms(v: Any) -> Optional[int]:
    return dt_to_ms(v) if isinstance(v, datetime) else None


def seconds_between(later: datetime, earlier: datetime) -> int:
    d = later - earlier
    return d.days * 86400 + d.seconds


def parse_hhmm(s: str) -> dtime:
    h, m = s.split(":")
    return dtime(int(h), int(m))


def is_overnight(shift_start: str, shift_end: str) -> bool:
    return parse_hhmm(shift_end) <= parse_hhmm(shift_start)


def round_half_up(value: Any, places: int) -> Decimal:
    """Half-up rounding on the decimal representation (not banker's, not binary-float rounding)."""
    q = Decimal(1).scaleb(-places)
    return Decimal(str(value)).quantize(q, rounding=ROUND_HALF_UP)


def attendance_date(ts: datetime, shift_start: str, shift_end: str) -> Date:
    """R1: IST calendar date of the punch-in; overnight shifts before shift_end belong to the previous day."""
    ist = ts.astimezone(IST)
    d = ist.date()
    if is_overnight(shift_start, shift_end) and ist.time() < parse_hhmm(shift_end):
        d -= timedelta(days=1)
    return d


def shift_window(d: Date, shift_start: str, shift_end: str) -> tuple[datetime, datetime]:
    """Aware IST datetimes of shift start and end for the shift that started on date d."""
    start = datetime.combine(d, parse_hhmm(shift_start), tzinfo=IST)
    end = datetime.combine(d, parse_hhmm(shift_end), tzinfo=IST)
    if is_overnight(shift_start, shift_end):
        end += timedelta(days=1)
    return start, end


def compute_late_minutes(punch_in: datetime, shift_start_dt: datetime) -> int:
    """R2: late only if strictly more than 10:00 after shift start; minutes counted from shift start."""
    secs = seconds_between(punch_in, shift_start_dt)
    return secs // 60 if secs > GRACE_SECONDS else 0


def compute_work_hours(punch_in: datetime, punch_out: datetime) -> float:
    """R4: seconds / 3600, 2 decimals half-up."""
    secs = seconds_between(punch_out, punch_in)
    return float(round_half_up(Decimal(secs) / Decimal(3600), 2))


def compute_overtime(punch_out: datetime, shift_end_dt: datetime) -> int:
    """R3: whole minutes after shift end, only when at least 30."""
    secs = seconds_between(punch_out, shift_end_dt)
    mins = secs // 60 if secs > 0 else 0
    return mins if mins >= MIN_OVERTIME_MINUTES else 0


def compute_half_day(work_hours: Optional[float]) -> bool:
    """R5: the rounded work_hours is below 4.50."""
    return work_hours is not None and Decimal(str(work_hours)) < HALF_DAY_LIMIT


def derive_fields(emp: dict, date_str: str, status: str, punch_in: Optional[datetime],
                  punch_out: Optional[datetime]) -> dict:
    """All four derived fields for one record, from final status and punch times (R2-R5)."""
    out = {"work_hours": None, "late_minutes": 0, "overtime_minutes": 0, "half_day": False}
    if status not in PRESENCE or punch_in is None:
        return out
    start, end = shift_window(Date.fromisoformat(date_str), emp["shift_start"], emp["shift_end"])
    out["late_minutes"] = compute_late_minutes(punch_in, start)
    if punch_out is not None:
        wh = compute_work_hours(punch_in, punch_out)
        out["work_hours"] = wh
        out["overtime_minutes"] = compute_overtime(punch_out, end)
        out["half_day"] = compute_half_day(wh)
    return out


def parse_date(s: str) -> Date:
    if not re.fullmatch(DATE_RE, s):
        raise unprocessable(f"invalid date '{s}', expected YYYY-MM-DD")
    try:
        return Date.fromisoformat(s)
    except ValueError:
        raise unprocessable(f"invalid date '{s}'")


def month_bounds(month: str) -> tuple[Date, Date]:
    if not re.fullmatch(MONTH_RE, month):
        raise unprocessable("month must be YYYY-MM")
    y, m = int(month[:4]), int(month[5:])
    try:
        return Date(y, m, 1), Date(y, m, calendar.monthrange(y, m)[1])
    except ValueError:
        raise unprocessable("invalid month")


def count_working_days(start: Date, end: Date) -> int:
    """R7: Monday-Friday between start and end inclusive, no holiday calendar."""
    n, d = 0, start
    while d <= end:
        if d.weekday() < 5:
            n += 1
        d += timedelta(days=1)
    return n


# --------------------------------------------------------------------------- #
# 3. Models and serializers
# --------------------------------------------------------------------------- #
EpochMs = Annotated[int, Field(strict=True, ge=MS_MIN, le=MS_MAX)]
Status = Literal["PRESENT", "ABSENT", "LEAVE", "WFH", "ON_DUTY"]
PresenceStatus = Literal["PRESENT", "WFH", "ON_DUTY"]


class EmployeeIn(BaseModel):
    emp_code: str = Field(pattern=r"^EMP[0-9]{4,6}$")
    name: str = Field(min_length=1, max_length=100)
    email: str = Field(max_length=120, pattern=r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
    department: str = Field(min_length=1, max_length=50)
    shift_start: str = Field(default="09:30", pattern=TIME_RE)
    shift_end: str = Field(default="18:30", pattern=TIME_RE)
    joined_on: str

    @field_validator("joined_on")
    @classmethod
    def _joined_on_is_date(cls, v: str) -> str:
        if not re.fullmatch(DATE_RE, v):
            raise ValueError("joined_on must be YYYY-MM-DD")
        Date.fromisoformat(v)  # ValueError -> 422
        return v

    @model_validator(mode="after")
    def _shift_differs(self):
        if self.shift_start == self.shift_end:
            raise ValueError("shift_start must differ from shift_end")
        return self


class PunchInIn(BaseModel):
    emp_code: str = Field(min_length=1)
    punched_at: Optional[EpochMs] = None
    status: PresenceStatus = "PRESENT"


class PunchOutIn(BaseModel):
    emp_code: str = Field(min_length=1)
    punched_at: Optional[EpochMs] = None


class RegularizeIn(BaseModel):
    status: Optional[Status] = None
    punch_in: Optional[EpochMs] = None
    punch_out: Optional[EpochMs] = None
    reason: str = Field(min_length=5, max_length=200)
    regularized_by: str = Field(min_length=1, max_length=50)

    @model_validator(mode="after")
    def _no_explicit_null(self):
        for f in ("status", "punch_in", "punch_out"):
            if f in self.model_fields_set and getattr(self, f) is None:
                raise ValueError(f"{f} must not be null")
        return self


def employee_out(doc: dict) -> dict:
    return {
        "emp_code": doc["emp_code"],
        "name": doc.get("name"),
        "email": doc.get("email"),
        "department": doc.get("department"),
        "shift_start": doc.get("shift_start"),
        "shift_end": doc.get("shift_end"),
        "joined_on": doc.get("joined_on"),
        "created_at": opt_ms(doc.get("created_at")),
    }


def history_out(history: Any) -> list:
    out = []
    for h in history or []:
        changes = {}
        for field, ch in (h.get("changes") or {}).items():
            if field in ("punch_in", "punch_out"):
                changes[field] = {"from": opt_ms(ch.get("from")), "to": opt_ms(ch.get("to"))}
            else:
                changes[field] = {"from": ch.get("from"), "to": ch.get("to")}
        out.append({"at": opt_ms(h.get("at")), "by": h.get("by"), "reason": h.get("reason"), "changes": changes})
    return out


def record_out(doc: dict) -> dict:
    """Public shape of an attendance record. Missing legacy fields get their documented defaults."""
    wh = doc.get("work_hours")
    return {
        "emp_code": doc["emp_code"],
        "date": doc["date"],
        "status": doc["status"],
        "punch_in": opt_ms(doc.get("punch_in")),
        "punch_out": opt_ms(doc.get("punch_out")),
        "work_hours": float(wh) if wh is not None else None,
        "late_minutes": int(doc.get("late_minutes") or 0),
        "overtime_minutes": int(doc.get("overtime_minutes") or 0),
        "half_day": bool(doc.get("half_day") or False),
        "history": history_out(doc.get("history")),
    }


def get_employee(emp_code: str) -> dict:
    emp = EMPLOYEES.find_one({"emp_code": emp_code})
    if emp is None:
        raise HTTPException(404, "employee not found")
    return emp


# --------------------------------------------------------------------------- #
# 4. Employees and attendance endpoints
# --------------------------------------------------------------------------- #
@app.get("/health")
def health():
    try:
        client.admin.command("ping")
    except PyMongoError:
        raise HTTPException(503, "database unavailable")
    ensure_indexes()  # no-op once created; retries if MongoDB was down at startup
    return {"status": "ok"}


@app.post("/employees", status_code=201)
def create_employee(body: EmployeeIn):
    doc = body.model_dump()
    now = datetime.now(UTC)
    doc["created_at"] = now.replace(microsecond=(now.microsecond // 1000) * 1000)
    try:
        EMPLOYEES.insert_one(doc)  # the unique index decides; no check-then-insert race
    except DuplicateKeyError:
        raise HTTPException(409, "emp_code already exists")
    return employee_out(doc)


@app.get("/employees")
def list_employees(
    department: Optional[str] = None,
    page: int = Query(1, ge=1, le=10_000_000),
    page_size: int = Query(20, ge=1, le=100),
):
    q: dict = {}
    if department is not None:
        q["department"] = department
    total = EMPLOYEES.count_documents(q)  # total reflects the filter (R10)
    cur = EMPLOYEES.find(q, {"_id": 0}).sort("emp_code", ASCENDING).skip((page - 1) * page_size).limit(page_size)
    return {"items": [employee_out(d) for d in cur], "total": total, "page": page, "page_size": page_size}


@app.post("/attendance/punch-in", status_code=201)
def punch_in(body: PunchInIn):
    emp = get_employee(body.emp_code)
    ts = ms_to_ts(body.punched_at) if body.punched_at is not None else now_ts()
    date_str = attendance_date(ts, emp["shift_start"], emp["shift_end"]).isoformat()
    doc = {
        "emp_code": body.emp_code,
        "date": date_str,
        "status": body.status,
        "punch_in": ts,
        "punch_out": None,
        **derive_fields(emp, date_str, body.status, ts, None),
        "history": [],
    }
    try:
        LOGS.insert_one(doc)  # unique (emp_code, date) index: exactly one concurrent request wins
    except DuplicateKeyError:
        raise HTTPException(409, "already punched in for this date")
    return record_out(doc)


@app.post("/attendance/punch-out")
def punch_out(body: PunchOutIn):
    emp = get_employee(body.emp_code)
    ts = ms_to_ts(body.punched_at) if body.punched_at is not None else now_ts()
    rec = LOGS.find_one(
        {"emp_code": body.emp_code, "punch_in": {"$lte": ts}},
        sort=[("punch_in", DESCENDING)],
    )
    if rec is None:
        raise HTTPException(404, "no punch-in found")
    if rec.get("punch_out") is not None:
        raise HTTPException(409, "already punched out")
    pin = aware(rec["punch_in"])
    if ts <= pin:
        raise unprocessable("punched_at must be after punch_in")
    if ts > pin + timedelta(hours=24):
        raise unprocessable("punched_at is more than 24 hours after punch_in")
    derived = derive_fields(emp, rec["date"], rec["status"], pin, ts)
    # compare-and-set: only the request that still sees punch_out == null can win
    updated = LOGS.find_one_and_update(
        {"_id": rec["_id"], "punch_out": None, "punch_in": rec["punch_in"]},
        {"$set": {"punch_out": ts, **derived}},
        return_document=ReturnDocument.AFTER,
    )
    if updated is None:
        raise HTTPException(409, "already punched out")
    return record_out(updated)


ATT_SORT = [("date", DESCENDING), ("emp_code", ASCENDING)]


def attendance_query(emp_code: Optional[str], date_from: Optional[str], date_to: Optional[str],
                     status: Optional[str]) -> dict:
    q: dict = {}
    if emp_code is not None:
        q["emp_code"] = emp_code
    df = parse_date(date_from) if date_from is not None else None
    dt = parse_date(date_to) if date_to is not None else None
    if df and dt and df > dt:
        raise unprocessable("date_from must not be after date_to")
    if df or dt:
        q["date"] = {}
        if df:
            q["date"]["$gte"] = df.isoformat()
        if dt:
            q["date"]["$lte"] = dt.isoformat()
    if status is not None:
        q["status"] = status
    return q


@app.get("/attendance")
def list_attendance(
    emp_code: Optional[str] = None,
    date_from: Optional[str] = Query(None, pattern=DATE_RE),
    date_to: Optional[str] = Query(None, pattern=DATE_RE),
    status: Optional[Status] = None,
    page: int = Query(1, ge=1, le=10_000_000),
    page_size: int = Query(20, ge=1, le=100),
):
    q = attendance_query(emp_code, date_from, date_to, status)
    total = LOGS.count_documents(q)
    cur = LOGS.find(q, {"_id": 0}).sort(ATT_SORT).skip((page - 1) * page_size).limit(page_size)
    return {"items": [record_out(d) for d in cur], "total": total, "page": page, "page_size": page_size}


def build_regularization(emp: dict, rec: dict, body: RegularizeIn) -> tuple[dict, dict]:
    """Validate a correction and return (new field values, history 'changes'). Raises 422 on rule violations."""
    old_status = rec["status"]
    old_in = aware(rec["punch_in"]) if rec.get("punch_in") else None
    old_out = aware(rec["punch_out"]) if rec.get("punch_out") else None
    new_status = body.status or old_status

    if new_status in NO_PUNCH:
        if body.punch_in is not None or body.punch_out is not None:
            raise unprocessable(f"status {new_status} clears punch times; do not supply punch_in/punch_out")
        new_in = new_out = None
    else:
        new_in = ms_to_ts(body.punch_in) if body.punch_in is not None else old_in
        new_out = ms_to_ts(body.punch_out) if body.punch_out is not None else old_out
        if new_in is None:
            raise unprocessable("a presence status requires a punch_in")
        if body.punch_in is not None:
            if attendance_date(new_in, emp["shift_start"], emp["shift_end"]).isoformat() != rec["date"]:
                raise unprocessable("punch_in must stay on the record's attendance date")
        if new_out is not None and (body.punch_in is not None or body.punch_out is not None):
            if new_out <= new_in:
                raise unprocessable("punch_out must be after punch_in")
            if new_out > new_in + timedelta(hours=24):
                raise unprocessable("punch_out must be within 24 hours of punch_in")

    if new_status == old_status and new_in == old_in and new_out == old_out:
        raise unprocessable("the request changes nothing")

    old = {
        "status": old_status, "punch_in": old_in, "punch_out": old_out,
        "work_hours": rec.get("work_hours"),
        "late_minutes": rec.get("late_minutes") or 0,
        "overtime_minutes": rec.get("overtime_minutes") or 0,
        "half_day": bool(rec.get("half_day") or False),
    }
    new = {"status": new_status, "punch_in": new_in, "punch_out": new_out,
           **derive_fields(emp, rec["date"], new_status, new_in, new_out)}
    changes = {k: {"from": old[k], "to": new[k]} for k in old if old[k] != new[k]}
    return new, changes


def _history_filter(rec: dict) -> Any:
    h = rec.get("history")
    if isinstance(h, list):
        return {"$size": len(h)}
    return {"$exists": False} if "history" not in rec else None


@app.patch("/attendance/{emp_code}/{date}")
def regularize(emp_code: str, body: RegularizeIn, date: str = Path(pattern=DATE_RE)):
    parse_date(date)
    emp = get_employee(emp_code)
    for _ in range(5):  # optimistic concurrency: re-read and retry if somebody else wrote in between
        rec = LOGS.find_one({"emp_code": emp_code, "date": date})
        if rec is None:
            raise HTTPException(404, "no attendance record for that date")
        new, changes = build_regularization(emp, rec, body)
        now = datetime.now(UTC)
        entry = {
            "at": now.replace(microsecond=(now.microsecond // 1000) * 1000),
            "by": body.regularized_by,
            "reason": body.reason,
            "changes": changes,
        }
        update: dict = {"$set": dict(new)}
        if isinstance(rec.get("history"), list):
            update["$push"] = {"history": entry}  # append, never overwrite
        else:
            update["$set"]["history"] = [entry]
        flt = {
            "_id": rec["_id"],
            "status": rec["status"],
            "punch_in": rec.get("punch_in"),
            "punch_out": rec.get("punch_out"),
            "history": _history_filter(rec),
        }
        updated = LOGS.find_one_and_update(flt, update, return_document=ReturnDocument.AFTER)
        if updated is not None:
            return record_out(updated)
    raise HTTPException(409, "concurrent modification, please retry")


# --------------------------------------------------------------------------- #
# 5. Aggregation pipelines and analytics endpoints
# --------------------------------------------------------------------------- #
# Reusable aggregation expressions. They read the STORED derived values; nothing is recomputed.
IS_PRESENT = {"$in": ["$status", list(PRESENCE)]}
IS_HALF = {"$eq": [{"$ifNull": ["$half_day", False]}, True]}
LATE_MIN = {"$ifNull": ["$late_minutes", 0]}
IS_LATE = {"$gt": [LATE_MIN, 0]}
IS_WEEKDAY = {"$in": [{"$dayOfWeek": {"$dateFromString": {"dateString": "$date", "format": "%Y-%m-%d"}}},
                      [2, 3, 4, 5, 6]]}  # $dayOfWeek: 1 = Sunday ... 7 = Saturday


def present_value(weekdays_only: bool) -> dict:
    """1 for a present record, 0.5 for a present half day, 0 otherwise (R5-R7)."""
    conds = [IS_PRESENT] + ([IS_WEEKDAY] if weekdays_only else [])
    return {"$cond": [{"$and": conds}, {"$cond": [IS_HALF, 0.5, 1]}, 0]}


def monthly_pipeline(emp_code: str, first: Date, last: Date) -> list:
    return [
        {"$match": {"emp_code": emp_code, "date": {"$gte": first.isoformat(), "$lte": last.isoformat()}}},
        {"$group": {
            "_id": None,
            "present_days": {"$sum": present_value(True)},
            "leave_days": {"$sum": {"$cond": [{"$eq": ["$status", "LEAVE"]}, 1, 0]}},
            "late_count": {"$sum": {"$cond": [IS_LATE, 1, 0]}},
            "total_late_minutes": {"$sum": {"$cond": [IS_LATE, LATE_MIN, 0]}},
            "total_overtime_minutes": {"$sum": {"$ifNull": ["$overtime_minutes", 0]}},
        }},
    ]


def summary_pipeline(first: Date, last: Date, department: Optional[str]) -> list:
    """Starts from `employees` so people with zero logs still count (R9)."""
    match: dict = {"joined_on": {"$lte": last.isoformat()}}
    if department is not None:
        match["department"] = department
    counted = {"$and": [IS_PRESENT, {"$isNumber": "$work_hours"}]}
    per_log = {
        "_id": 0,
        "pd": present_value(True),
        "wh": {"$cond": [counted, "$work_hours", 0]},
        "whc": {"$cond": [counted, 1, 0]},
        "lc": {"$cond": [IS_LATE, 1, 0]},
        "lm": {"$cond": [IS_LATE, LATE_MIN, 0]},
        "lv": {"$cond": [{"$eq": ["$status", "LEAVE"]}, 1, 0]},
        "od": {"$cond": [{"$eq": ["$status", "ON_DUTY"]}, 1, 0]},
    }
    return [
        {"$match": match},
        {"$lookup": {
            "from": "attendance_logs",
            "let": {"c": "$emp_code"},
            "pipeline": [
                {"$match": {"$expr": {"$and": [
                    {"$eq": ["$emp_code", "$$c"]},
                    {"$gte": ["$date", first.isoformat()]},
                    {"$lte": ["$date", last.isoformat()]},
                ]}}},
                {"$project": per_log},
            ],
            "as": "logs",
        }},
        # per employee first (at most ~31 logs each), then per department: no giant arrays
        {"$project": {
            "_id": 0, "department": 1,
            "pd": {"$sum": "$logs.pd"}, "wh": {"$sum": "$logs.wh"}, "whc": {"$sum": "$logs.whc"},
            "lc": {"$sum": "$logs.lc"}, "lm": {"$sum": "$logs.lm"},
            "lv": {"$sum": "$logs.lv"}, "od": {"$sum": "$logs.od"},
        }},
        {"$group": {
            "_id": "$department",
            "headcount": {"$sum": 1},
            "present_days": {"$sum": "$pd"},
            "wh_sum": {"$sum": "$wh"}, "wh_count": {"$sum": "$whc"},
            "late_count": {"$sum": "$lc"}, "total_late_minutes": {"$sum": "$lm"},
            "leave_count": {"$sum": "$lv"}, "on_duty_count": {"$sum": "$od"},
        }},
        {"$sort": {"_id": 1}},
    ]


def leaderboard_pipeline(first: Date, last: Date, limit: int, department: Optional[str]) -> list:
    p: list = [
        {"$match": {"date": {"$gte": first.isoformat(), "$lte": last.isoformat()}, "late_minutes": {"$gt": 0}}},
        {"$group": {"_id": "$emp_code", "total_late_minutes": {"$sum": "$late_minutes"}, "late_count": {"$sum": 1}}},
        {"$lookup": {"from": "employees", "localField": "_id", "foreignField": "emp_code", "as": "emp"}},
        {"$unwind": "$emp"},  # logs of unknown employees disappear here
    ]
    if department is not None:
        p.append({"$match": {"emp.department": department}})  # rank inside the department only
    p += [
        # $rank = standard competition ranking: ties share a rank, the next rank is skipped (1,2,2,4)
        {"$setWindowFields": {"sortBy": {"total_late_minutes": -1}, "output": {"rank": {"$rank": {}}}}},
        {"$match": {"rank": {"$lte": limit}}},  # limit applies AFTER ranking
        {"$sort": {"total_late_minutes": -1, "_id": 1}},
        {"$project": {"_id": 0, "rank": 1, "emp_code": "$_id", "name": "$emp.name",
                      "department": "$emp.department", "total_late_minutes": 1, "late_count": 1}},
    ]
    return p


def trend_pipeline(codes: list, joined: list, d_from: Date, d_to: Date) -> list:
    lower = datetime(d_from.year, d_from.month, d_from.day, tzinfo=UTC)
    upper = datetime(d_to.year, d_to.month, d_to.day, tzinfo=UTC) + timedelta(days=1)  # upper bound is exclusive
    return [
        {"$match": {"emp_code": {"$in": codes}, "date": {"$gte": d_from.isoformat(), "$lte": d_to.isoformat()}}},
        {"$group": {"_id": "$date",
                    "present": {"$sum": present_value(False)},
                    "late": {"$sum": {"$cond": [IS_LATE, 1, 0]}}}},
        {"$addFields": {"d": {"$dateFromString": {"dateString": "$_id", "format": "%Y-%m-%d"}}}},
        # gap filling: one document per calendar day in the range
        {"$densify": {"field": "d", "range": {"step": 1, "unit": "day", "bounds": [lower, upper]}}},
        {"$addFields": {
            "date": {"$dateToString": {"date": "$d", "format": "%Y-%m-%d"}},
            "is_working_day": {"$in": [{"$dayOfWeek": "$d"}, [2, 3, 4, 5, 6]]},
            "present_count": {"$ifNull": ["$present", 0]},
            "late_count": {"$ifNull": ["$late", 0]},
        }},
        {"$addFields": {"headcount": {"$size": {"$filter": {
            "input": joined, "as": "j", "cond": {"$lte": ["$$j", "$date"]}}}}}},
        {"$addFields": {"attendance_rate": {"$cond": [
            {"$and": ["$is_working_day", {"$gt": ["$headcount", 0]}]},
            {"$divide": ["$present_count", "$headcount"]},
            None]}}},
        # mean of the non-null rates in the current row and up to 6 preceding rows ($avg skips nulls)
        {"$setWindowFields": {"sortBy": {"d": 1},
                              "output": {"moving": {"$avg": "$attendance_rate", "window": {"documents": [-6, 0]}}}}},
        {"$sort": {"d": 1}},
        {"$project": {"_id": 0, "date": 1, "is_working_day": 1, "headcount": 1, "present_count": 1,
                      "late_count": 1, "attendance_rate": 1, "moving_avg_7d": "$moving"}},
    ]


def load_department(department: str) -> tuple[list, list]:
    emps = list(EMPLOYEES.find({"department": department}, {"_id": 0, "emp_code": 1, "joined_on": 1}))
    if not emps:
        raise HTTPException(404, "unknown department")
    return [e["emp_code"] for e in emps], sorted(e["joined_on"] for e in emps)


def validate_range(d_from: str, d_to: str) -> tuple[Date, Date]:
    a, b = parse_date(d_from), parse_date(d_to)
    if b < a:
        raise unprocessable("'to' must not be before 'from'")
    if (b - a).days + 1 > 92:
        raise unprocessable("range must not exceed 92 days")
    return a, b


def num(v: Any, places: int) -> float:
    return float(round_half_up(v, places))


@app.get("/analytics/employees/{emp_code}/monthly")
def employee_monthly(emp_code: str, month: str = Query(pattern=MONTH_RE)):
    first, last = month_bounds(month)
    emp = get_employee(emp_code)
    joined = Date.fromisoformat(emp["joined_on"])
    working_days = count_working_days(max(first, joined), last)
    rows = list(LOGS.aggregate(monthly_pipeline(emp_code, first, last)))
    r = rows[0] if rows else {}
    present = Decimal(str(r.get("present_days", 0)))
    return {
        "emp_code": emp_code,
        "month": month,
        "working_days": working_days,
        "present_days": num(present, 2),
        "leave_days": int(r.get("leave_days", 0)),
        "late_count": int(r.get("late_count", 0)),
        "total_late_minutes": int(r.get("total_late_minutes", 0)),
        "total_overtime_minutes": int(r.get("total_overtime_minutes", 0)),
        "attendance_pct": num(present / working_days * 100, 2) if working_days else None,
    }


def summary_hint(department: Optional[str]) -> str:
    return "emp_dept_code" if department is not None else "emp_joined_on"


@app.get("/analytics/departments/summary")
def department_summary(month: str = Query(pattern=MONTH_RE), department: Optional[str] = None):
    first, last = month_bounds(month)
    rows = EMPLOYEES.aggregate(summary_pipeline(first, last, department), hint=summary_hint(department))
    items = []
    for r in rows:
        avg = None
        if r["wh_count"]:
            avg = num(Decimal(str(round(r["wh_sum"], 6))) / Decimal(int(r["wh_count"])), 2)
        items.append({
            "department": r["_id"],
            "headcount": int(r["headcount"]),
            "present_days": num(r["present_days"], 2),
            "avg_work_hours": avg,
            "late_count": int(r["late_count"]),
            "total_late_minutes": int(r["total_late_minutes"]),
            "leave_count": int(r["leave_count"]),
            "on_duty_count": int(r["on_duty_count"]),
        })
    return {"month": month, "items": items}


@app.get("/analytics/leaderboard/late")
def late_leaderboard(month: str = Query(pattern=MONTH_RE), limit: int = Query(10, ge=1, le=50),
                     department: Optional[str] = None):
    first, last = month_bounds(month)
    rows = LOGS.aggregate(leaderboard_pipeline(first, last, limit, department), hint="log_late_cover")
    return {"month": month, "items": [{
        "rank": int(r["rank"]), "emp_code": r["emp_code"], "name": r["name"], "department": r["department"],
        "total_late_minutes": int(r["total_late_minutes"]), "late_count": int(r["late_count"]),
    } for r in rows]}


@app.get("/analytics/departments/{department}/trend")
def department_trend(department: str, date_from: str = Query(alias="from", pattern=DATE_RE),
                     date_to: str = Query(alias="to", pattern=DATE_RE)):
    d_from, d_to = validate_range(date_from, date_to)
    codes, joined = load_department(department)
    rows = LOGS.aggregate(trend_pipeline(codes, joined, d_from, d_to))
    items = []
    for r in rows:
        rate, mov = r.get("attendance_rate"), r.get("moving_avg_7d")
        items.append({
            "date": r["date"],
            "is_working_day": bool(r["is_working_day"]),
            "headcount": int(r["headcount"]),
            "present_count": float(r["present_count"]),
            "late_count": int(r["late_count"]),
            "attendance_rate": num(rate, 4) if rate is not None else None,
            "moving_avg_7d": num(mov, 4) if mov is not None else None,
        })
    return {"department": department, "items": items}


# --------------------------------------------------------------------------- #
# 6. Explain endpoint (same builders as the real endpoints)
# --------------------------------------------------------------------------- #
ExplainTarget = Literal["attendance_list", "employee_monthly", "department_summary", "late_leaderboard",
                        "department_trend"]


def _need(**params: Any) -> None:
    missing = [k for k, v in params.items() if v is None]
    if missing:
        raise unprocessable("missing required query parameter(s): " + ", ".join(missing))


@app.get("/admin/explain/{endpoint}")
def explain(
    endpoint: ExplainTarget,
    emp_code: Optional[str] = None,
    month: Optional[str] = Query(None, pattern=MONTH_RE),
    department: Optional[str] = None,
    limit: int = Query(10, ge=1, le=50),
    date_from: Optional[str] = Query(None, pattern=DATE_RE),
    date_to: Optional[str] = Query(None, pattern=DATE_RE),
    status: Optional[Status] = None,
    from_: Optional[str] = Query(None, alias="from", pattern=DATE_RE),
    to: Optional[str] = Query(None, pattern=DATE_RE),
    page: int = Query(1, ge=1, le=10_000_000),
    page_size: int = Query(20, ge=1, le=100),
):
    if endpoint == "attendance_list":
        q = attendance_query(emp_code, date_from, date_to, status)
        coll = "attendance_logs"
        cmd: dict = {"find": coll, "filter": q, "sort": dict(ATT_SORT), "projection": {"_id": 0},
                     "skip": (page - 1) * page_size, "limit": page_size}
    else:
        if endpoint == "employee_monthly":
            _need(emp_code=emp_code, month=month)
            first, last = month_bounds(month)
            coll, pipeline, hint = "attendance_logs", monthly_pipeline(emp_code, first, last), None
        elif endpoint == "department_summary":
            _need(month=month)
            first, last = month_bounds(month)
            coll, pipeline, hint = "employees", summary_pipeline(first, last, department), summary_hint(department)
        elif endpoint == "late_leaderboard":
            _need(month=month)
            first, last = month_bounds(month)
            coll = "attendance_logs"
            pipeline, hint = leaderboard_pipeline(first, last, limit, department), "log_late_cover"
        else:  # department_trend
            _need(department=department, **{"from": from_, "to": to})
            d_from, d_to = validate_range(from_, to)
            codes, joined = load_department(department)
            coll, pipeline, hint = "attendance_logs", trend_pipeline(codes, joined, d_from, d_to), None
        cmd = {"aggregate": coll, "pipeline": pipeline, "cursor": {}}
        if hint:
            cmd["hint"] = hint
    result = db.command({"explain": cmd, "verbosity": "executionStats"})
    plain = json.loads(json_util.dumps(result, json_options=json_util.RELAXED_JSON_OPTIONS))
    return {"endpoint": endpoint, "collection": coll, "explain": plain}
