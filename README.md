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
│  │  __cron_log  table  │          │                                     │
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


### `SELECT cron_init(mode);` or `SELECT cron_init(mode, poll_seconds);`
Initializes the cron engine.
- `mode`:
  - `thread`: (Recommended) Spawns a background OS thread. Best for disk-based databases. Requires WAL mode (`PRAGMA journal_mode=WAL`).
  - `callback`: No extra threads. Piggybacks on your existing queries. Best for in-memory databases.
- `poll_seconds` (optional): How often the thread checks for due jobs. Default is **16 seconds**. Lower values = more precision, higher CPU usage.

```sql
SELECT cron_init('thread');       -- Default 16 second poll
SELECT cron_init('thread', 4);    -- 4 second poll for sub-minute jobs
SELECT cron_init('callback');     -- Callback mode (no poll interval)
```

### `SELECT cron_schedule(name, seconds, sql_command);`
Schedules a task with a unique name.
```sql
SELECT cron_schedule('vacuum_job', 3600, 'VACUUM;');
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

### `SELECT cron_delete(name);`
Deletes a scheduled job.

### `SELECT cron_reset();`
Completely resets the global state of the extension (useful for testing).

## Schema
The extension uses two internal tables:
- `__cron_jobs`: Stores the schedule and state of jobs.
- `__cron_log`: Stores the execution history (start time, duration, status, and error messages).

## Future Improvements
- **Cron Syntax**: Support for standard crontab expressions (e.g., `* * * * *`).
- **Job Timeouts**: Automatic termination of long-running jobs.
- **Persistence Modes**: Option for transient (in-memory only) jobs.
- **Retry Logic**: Configurable retry policies for failed jobs.

## License
MIT
