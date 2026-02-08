# sqlite-cron

![Coverage](https://img.shields.io/badge/coverage-94.84%25-brightgreen)
![Platform](https://img.shields.io/badge/platform-Linux%20%7C%20macOS%20%7C%20Windows-blue)
![Tests](https://img.shields.io/badge/tests-32%20passed-success)
![Language](https://img.shields.io/badge/language-C-orange)
![License](https://img.shields.io/badge/license-MIT-green)

A SQLite extension for scheduling background SQL jobs, inspired by `pg_cron`.


## Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                           SQLite Connection                             │
├─────────────────────────────────────────────────────────────────────────┤
│  cron_init('thread')              │  cron_init('callback')              │
│  ─────────────────────            │  ──────────────────────             │
│                                   │                                     │
│  ┌─────────────────────┐          │  ┌─────────────────────┐            │
│  │   Main Thread       │          │  │   Main Thread       │            │
│  │   (Your App)        │          │  │   (Your App)        │            │
│  └──────────┬──────────┘          │  └──────────┬──────────┘            │
│             │                     │             │                       │
│             │ spawns              │             │ progress_handler      │
│             ▼                     │             ▼                       │
│  ┌─────────────────────┐          │  ┌─────────────────────┐            │
│  │  Background Thread  │          │  │  cron_tick()        │            │
│  │  cron_thread_worker │          │  │  (piggybacks on     │            │
│  │                     │          │  │   your queries)     │            │
│  │  - Opens own DB     │          │  └──────────┬──────────┘            │
│  │  - Polls every 1s   │          │             │                       │
│  │  - Executes jobs    │          │             │ rate-limited          │
│  └──────────┬──────────┘          │             ▼                       │
│             │                     │  ┌─────────────────────┐            │
│             ▼                     │  │  Execute due jobs   │            │
│  ┌─────────────────────┐          │  └─────────────────────┘            │
│  │  __cron_jobs table  │◄─────────┼──────────────────────────────────►  │
│  │  (interval / cron)  │          │                                     │
│  └─────────────────────┘          │                                     │
└─────────────────────────────────────────────────────────────────────────┘

                        Graceful Shutdown Flow
                        ──────────────────────

   cron_stop() or connection close
              │
              ▼
   ┌──────────────────────┐
   │ ref_count-- > 0?     │──Yes──► Keep running (other connections)
   └──────────┬───────────┘
              │ No
              ▼
   ┌──────────────────────┐
   │ active = 0           │  Signal thread to exit
   │ cond_broadcast()     │  Wake sleeping thread
   │ sqlite3_interrupt()  │  Cancel running query
   └──────────┬───────────┘
              │
              ▼
   ┌──────────────────────┐
   │ pthread_join() or    │  Wait for thread exit
   │ WaitForSingleObject  │  (Windows)
   └──────────┬───────────┘
              │
              ▼
   ┌──────────────────────┐
   │ Cleanup: free paths  │
   │ Reset global state   │
   └──────────────────────┘
```

### Scheduling Cycle (`cron_tick`)
1. **Check**: The thread wakes up (every second) and queries `__cron_jobs` for active jobs where `next_run <= now`.
2. **Execute**: It spawns a separate transaction to run the job's SQL command.
3. **Reschedule**: After execution, it calculates the **new** `next_run`:
   - **Cron Expression**: If `cron_expr` is set (e.g., `*/5 * * * * *`), it uses `ccronexpr` to calculate the next match based on the *current wall-clock time*.
   - **Interval**: If `cron_expr` is NULL, it simply adds `schedule_interval` seconds to the current time.

## Installation

1. Compile the extension: `make linux` (or `macos`, `windows`).
2. Load it into your SQLite session: `.load build/sqlite_cron`

## Running Tests

### Linux / macOS
```bash
make test
```

### Windows
1. Install **MinGW-w64** (for `gcc`) and **Python**.
2. Run the build: `make windows`
3. Run the tests: `pytest tests/test_cron.py`


### `SELECT cron_init(mode, [p1], [p2]);`
Initializes the cron engine.
- `mode`:
  - `thread`: (Recommended) Spawns a background OS thread.
  - `callback`: Piggybacks on your existing queries.
- **Thread Parameters** (`p1`):
  - `poll_seconds`: How often the thread checks for jobs (default 16s).
- **Callback Parameters** (`p1`, `p2`):
  - `rate_limit_seconds`: Minimum seconds between checks (default 1s).
  - `opcode_threshold`: Number of SQL opcodes between progress callbacks (default 100 in production, 1 in test).

```sql
SELECT cron_init('thread', 4);    -- Thread mode, 4s poll
SELECT cron_init('callback', 1, 10); -- Callback mode, 1s limit, check every 10 opcodes
```

### `SELECT cron_schedule(name, seconds, sql_command, [timeout_ms]);`
Schedules a task with a unique name.
- `timeout_ms` (optional): Maximum execution time in milliseconds. If exceeded, the job is interrupted.

```sql
SELECT cron_schedule('vacuum_job', 3600, 'VACUUM;');
-- Schedule with a 5-second timeout
SELECT cron_schedule('long_job', 60, 'DELETE FROM large_table;', 5000);
```

### `SELECT cron_schedule_cron(name, cron_expr, sql_command, [timeout_ms]);`
Schedules a task using a cron expression.
- `timeout_ms` (optional): Maximum execution time in milliseconds.

```sql
SELECT cron_schedule_cron('daily_vacuum', '0 30 23 * * *', 'VACUUM;');
-- Schedule with timeout
SELECT cron_schedule_cron('heavy_report', '0 0 1 * * *', 'INSERT INTO report...', 10000);
```

### `SELECT cron_get(name);`
Returns a JSON object describing the job.
```sql
SELECT cron_get('vacuum_job');
-- {"name":"vacuum_job","command":"VACUUM;","interval":3600,"active":1,"next_run":...}
```

### `SELECT cron_list();`
Returns a JSON array of all scheduled jobs.

### `SELECT cron_pause(name);`
Temporarily stops a job from executing.

### `SELECT cron_resume(name);`
Resumes a paused job.

### `SELECT cron_update(name, seconds);`
Updates the execution interval for a job.

### `SELECT cron_set_retry(name, max_retries, retry_interval_seconds);`
Configures retry logic for a specific job.
- `max_retries`: Number of times to retry a failed job.
- `retry_interval_seconds`: Seconds to wait before retrying.

```sql
SELECT cron_schedule('unstable_job', 3600, 'INSERT INTO ...');
SELECT cron_set_retry('unstable_job', 3, 10); -- Retry up to 3 times, wait 10s between attempts
```

### `SELECT cron_delete(name);`
Deletes a scheduled job.

### `SELECT cron_run(name);`
Manually executes a job immediately, regardless of its schedule.
```sql
SELECT cron_run('job_name');
```

### `SELECT cron_purge_logs(retention);`
Purges execution logs older than a specific period. Uses SQLite's [date modifiers](https://www.sqlite.org/lang_datefunc.html).
```sql
SELECT cron_purge_logs('-7 days'); -- Keep last 7 days
SELECT cron_purge_logs('-1 month'); -- Keep last month
```

### `SELECT cron_status();`
Returns a JSON object describing the current state of the cron engine.
```json
{
  "mode": "thread",
  "active": 1,
  "job_count": 5,
  "last_check": 1678900000
}
```

### `SELECT cron_job_next(name);`
Returns the next scheduled run time (Unix timestamp) for a specific job.

### `SELECT cron_next_job();`
Returns the single nearest upcoming job as a JSON object.
```json
{"name": "job2", "next_run": 1678902000}
```

### `SELECT cron_next_job_time();`
Returns a JSON object mapping all active job names to their next scheduled run times.
```json
{"job1": 1678901000, "job2": 1678902000}
```

### `SELECT cron_reset();`
Completely resets the global state of the extension (useful for testing).

## Schema
The extension uses two internal tables:
- `__cron_jobs`: Stores the schedule and state of jobs.
- `__cron_log`: Stores the execution history (start time, duration, status, and error messages).

## Features

- **Standard Cron Syntax**: Supports standard cron expressions (e.g., `* * * * *`, `0 9 * * 1-5`) using [ccronexpr](https://github.com/staticlibs/ccronexpr).
- **Interval Scheduling**: Run jobs every N seconds.
- **Job Persistence**: Jobs are stored in a table (`__cron_jobs`) and survive restarts.
- **Execution Logging**: Tracks job execution history in `__cron_log`.
- **Mode Flexibility**:
    - **Thread Mode**: Runs in a background thread (requires file-based DB).
    - **Callback Mode**: Runs synchronously hooked into SQLite's progress handler (works in WASM, in-memory DBs).
- **Reliability**:
    - **Retry Logic**: Automatically retry failed jobs with configurable intervals.
    - **Timeouts**: Terminate long-running jobs.
    - **Graceful Shutdown**: Ensures cleanup on exit.

## Future Improvements
- **Cron Syntax**: Support for standard crontab expressions (e.g., `* * * * *`).
- **Persistence Modes**: Option for transient (in-memory only) jobs.
- **Retry Logic**: Configurable retry policies for failed jobs.

## Acknowledgments
- **[pg_cron](https://github.com/citusdata/pg_cron)**: The inspiration for this extension's design and functionality.
- **[supertinycron](https://github.com/exander77/supertinycron)**: The `ccronexpr` library used for parsing cron expressions is licensed under the Apache License 2.0.

## License
MIT License

This project includes code from `ccronexpr` (supertinycron), which is licensed under the Apache License 2.0.
