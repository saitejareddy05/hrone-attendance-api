# Employee Attendance & Analytics API

A backend for a small HR attendance system, built with **FastAPI** and **MongoDB**. Employees punch in and out, managers correct records with a full audit trail, and the business can ask questions of the data (who is late, which department is slipping, what the trend looks like) through MongoDB aggregation pipelines.

All application code lives in a single file: [`app/main.py`](app/main.py).

## Features

- Employee management with database-enforced unique `emp_code`
- Punch-in / punch-out with IST time handling, overnight shifts, lateness, overtime, work hours and half-day rules (R1-R5)
- Manual regularization (`PATCH`) that recomputes derived fields and appends to an audit `history`
- Four analytics endpoints built as aggregation pipelines: employee monthly summary, department summary (cross-collection), late leaderboard (tie-aware ranking), department trend (gap filling + 7-day moving average)
- `/admin/explain/{endpoint}` returns MongoDB `executionStats` for each endpoint's main query
- Race-safe by design: unique indexes and compare-and-set updates, no check-then-insert

## Tech stack

Python 3.11+, FastAPI, Pydantic v2, PyMongo, MongoDB 6.0+ (uses `$setWindowFields`, `$densify`, `$rank`).

## Project structure

```
app/main.py          the whole application (config, rules, endpoints, pipelines, explain)
tests/test_rules.py  unit tests for the business rules (no database needed)
tools/gen_data.py    optional: generates ~100k awkward records for local testing
REVIEW.md            Part A: defects found in the starter code
DECISIONS.md         Part C: design decisions
requirements.txt
```

## Getting started

**1. Start MongoDB** (either option)

```bash
docker run -d --name mongo -p 27017:27017 mongo:7
```
or create a free MongoDB Atlas (M0) cluster and copy its connection string.

**2. Install**

```bash
python -m venv .venv
source .venv/bin/activate        # Windows: .venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

**3. Configure**

| Variable | Meaning | Example |
|---|---|---|
| `MONGO_URI` | MongoDB connection string | `mongodb://localhost:27017` |
| `MONGO_DB` | Database name | `attendance_db` |

Set them as environment variables or in a local `.env` file. Real environment variables always take priority over `.env`.

**4. Run**

```bash
uvicorn app.main:app --port 8000
```

Then open http://localhost:8000/health (expects `{"status":"ok"}`) and http://localhost:8000/docs for the interactive API docs. All indexes are created automatically at startup, idempotently.

## API overview

| Method | Path | Purpose |
|---|---|---|
| GET | `/health` | 200 only when MongoDB answers, else 503 |
| POST | `/employees` | Create employee (409 duplicate, 422 invalid) |
| GET | `/employees` | List, filter by department, paginated, sorted by `emp_code` |
| POST | `/attendance/punch-in` | Create the day's record (R1, R2) |
| POST | `/attendance/punch-out` | Close the record, compute hours / overtime / half day (R3-R5) |
| GET | `/attendance` | Filter by employee, dates, status; paginated |
| PATCH | `/attendance/{emp_code}/{date}` | Regularize one record, append to `history` |
| GET | `/analytics/employees/{emp_code}/monthly` | Monthly summary for one employee |
| GET | `/analytics/departments/summary` | Department summary for a month |
| GET | `/analytics/leaderboard/late` | Late-comers ranking (1, 2, 2, 4 style ties) |
| GET | `/analytics/departments/{department}/trend` | Daily series with 7-day moving average |
| GET | `/admin/explain/{endpoint}` | MongoDB explain for an endpoint's main query |

The full contract (parameters, status codes, rules R1-R10) is in `openapi.yaml` from the assignment kit.

## Design notes

- **Time:** instants are epoch milliseconds in the API and UTC BSON dates in MongoDB. The client uses `tz_aware=True`, so no naive datetimes are ever handled. Dates and shift times are IST strings, and instants are truncated to whole seconds before any calculation.
- **Rounding:** reported numbers use `Decimal` with half-up rounding after the pipeline, because MongoDB's `$round` is banker's rounding.
- **Concurrency:** punch-in relies on the unique `(emp_code, date)` index; punch-out uses a compare-and-set `find_one_and_update`; PATCH uses optimistic concurrency with a few retries and returns 409 if it still loses.
- **Analytics:** pipelines read the stored derived values rather than recomputing them. The department summary starts from `employees`, so people with no logs still count towards headcount.
- **Indexes:** see `DECISIONS.md` for what was created and why.

## Testing

```bash
pytest tests/                 # business-rule unit tests, no database needed
python tools/gen_data.py      # loads ~100k awkward records (DROPS employees and attendance_logs in MONGO_DB)
```

After loading the data, call `/admin/explain/<endpoint>` and check the output contains `IXSCAN` and no `COLLSCAN`. The generated data deliberately includes employees with no logs, a mid-month joiner, an overnight-shift team, weekend records, missing weekdays, ties in late minutes, legacy documents and logs of an unknown employee.

## Assumptions

- `moving_avg_7d` averages the unrounded daily attendance rates and rounds the result to 4 decimals.
- Records dated before an employee's `joined_on` are not specially excluded from totals.

## Status

All endpoints in the contract are implemented. <!-- Edit before submitting: say what you tested and anything that is missing. -->
