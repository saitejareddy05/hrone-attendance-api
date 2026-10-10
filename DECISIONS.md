# DECISIONS.md

1. **Indexes.** On `attendance_logs`: unique `(emp_code, date)` (punch-in race guard, PATCH lookup, monthly summary, `$lookup`); `(date desc, emp_code)` for the list sort; `(status, date desc, emp_code)` for status filters; `(date, late_minutes, emp_code)` which covers the leaderboard; `(emp_code, punch_in desc)` to find the record to punch out. On `employees`: unique `emp_code`, `(department, emp_code)`, `joined_on`. Rejected: a single-field `status` index, because it has few distinct values and the compound one also gives the sort order.
2. **Punch-in race.** Both requests compute the same IST date and call `insert_one`. The unique `(emp_code, date)` index lets only one insert succeed; the other gets `DuplicateKeyError`, which I return as 409. There is no check-then-insert gap.
3. **Ties.** The leaderboard uses `$rank`, so tied people share a rank and `limit` filters on rank afterwards. A tie at the cutoff returns every tied person, so more than `limit` rows, ordered by `emp_code`.
4. **Headcount.** The summary pipeline starts from `employees` (joined on or before month end) and `$lookup`s their logs, so someone with zero logs still counts once with zero contributions.
5. **100x data.** Keep monthly and daily per-employee totals in a summary collection updated on each punch, so analytics stop scanning raw logs, and shard logs by `emp_code`.
