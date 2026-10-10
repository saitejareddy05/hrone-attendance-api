# Employee Attendance & Analytics API

FastAPI + MongoDB. All application code is in `app/main.py`.

## Run
```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
export MONGO_URI="mongodb://localhost:27017"   # or a .env file; real env vars win
export MONGO_DB="attendance_db"
uvicorn app.main:app --port 8000
```
MongoDB 6.0+ is required (`$setWindowFields`, `$densify`, `$rank`). Indexes are created at startup, idempotently.

## Status
Everything in `openapi.yaml` is implemented: health, employees, punch-in/out, list, PATCH regularization, the four analytics endpoints and `/admin/explain/{endpoint}`. `REVIEW.md` and `DECISIONS.md` are filled in.

## Notes / assumptions
* Analytics read the stored derived values; rounding is done half-up with `Decimal` after the pipeline (MongoDB's `$round` is banker's rounding).
* `moving_avg_7d` averages the unrounded daily rates, then rounds to 4 decimals.
* `PATCH` retries a few times on a concurrent write and returns 409 if it still loses.
* Optional helpers: `pytest tests/` runs the rule unit tests (no database); `python tools/gen_data.py` loads ~100k awkward records for checking `/admin/explain/*` (it drops the two collections first).
