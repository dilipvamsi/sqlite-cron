# sqlite-cron

![Coverage](https://img.shields.io/badge/coverage-91%25-brightgreen)
![Platform](https://img.shields.io/badge/platform-Linux%20%7C%20macOS%20%7C%20Windows-blue)
![Tests](https://img.shields.io/badge/tests-56%20passed-success)
![Language](https://img.shields.io/badge/language-C-orange)
![License](https://img.shields.io/badge/license-MIT-green)

A powerful SQLite extension for scheduling jobs directly within your database. Think of it as `pg_cron` for SQLite.

## Features

- 🕒 **Standard Cron Syntax**: Schedule jobs using familiar syntax (e.g., `*/5 * * * *`).
- 🔄 **Interval Scheduling**: Run jobs every N seconds.
- 💾 **Persisted Jobs**: Schedules are stored in tables and survive restarts.
- ⚡ **Zero-Config**: Works out of the box with reasonable defaults.
- 🧵 **Flexible Modes**:
    - **Thread Mode**: Spawns a dedicated background thread (recommended).
    - **Callback Mode**: Piggybacks on your existing queries (perfect for WASM/embedded).
- 🛡️ **Reliable**: Built-in crash recovery, timeouts, and retry logic.
- 📊 **Introspection**: Query job status, history, and next run times as JSON.

## Installation

### 1. Download
Grab the latest release from the [Releases](https://github.com/dilipvamsi/sqlite-cron/releases) page.
- `sqlite_cron-linux-x64.so`
- `sqlite_cron-windows-x64.dll`
- `sqlite_cron-macos-universal.dylib`

### 2. Load Extension
```sql
.load path/to/sqlite_cron
```
*Or in Python:*
```python
import sqlite3
conn = sqlite3.connect("my.db")
conn.enable_load_extension(True)
conn.load_extension("./sqlite_cron")
```

## Quick Start

### Initialize
Start the cron engine. **Thread mode** is best for most apps.
```sql
-- Check for jobs every 4 seconds
SELECT cron_init('thread', 4);
```

### Schedule a Job
Schedule a SQL command to run periodically.
```sql
-- Run VACUUM every day at 3:30 AM
SELECT cron_schedule_cron('daily_maintenance', '30 3 * * *', 'VACUUM;');

-- Run a cleanup every 10 seconds with a 5s execution timeout
SELECT cron_schedule('cleanup', 10, 'DELETE FROM events WHERE created_at < date("now", "-7 days");', 5000);
```

### Manage & Monitor
```sql
-- Manually trigger a job right now
SELECT cron_run('cleanup');

-- Check engine health and active job count
SELECT cron_status();

-- Find when the next job is due
SELECT cron_next_job();
```

## Execution Modes

`sqlite-cron` supports two distinct modes of operation. Choose the one that fits your environment.

| Feature | `thread` (Recommended) | `callback` |
| :--- | :--- | :--- |
| **Mechanism** | Spawns a dedicated OS thread. | Hooks into `sqlite3_progress_handler`. |
| **Concurrency** | **High**. Runs jobs in parallel to your app. | **Low**. Pauses your main query to run jobs. |
| **Reliability** | **High**. Independent of main app activity. | **Variable**. Only runs *while* you are executing queries. |
| **Best For** | Servers, Daemons, CLI tools. | WASM, Mobile, strict single-threaded envs. |
| **Prerequisites** | Requires thread support (pthreads/Windows). | None. Pure SQLite API. |

### 1. Thread Mode
The default and most robust mode. It creates a background thread that wakes up periodically to check for jobs.
```sql
-- Check every 5 seconds
SELECT cron_init('thread', 5);
```

### 2. Callback Mode
For environments where spawning threads is forbidden or impossible (e.g., some sandboxed environments, WASM). It relies on the "heartbeat" of your own SQL queries.
```sql
-- Check every 1 second, but only after every 10 SQL opcodes
SELECT cron_init('callback', 1, 10);
```

## Example Use Cases

### 1. Database Maintenance
Keep your database healthy by scheduling regular VACUUM and ANALYZE commands.
```sql
-- Vacuum every night at 4 AM
SELECT cron_schedule_cron('vacuum', '0 4 * * *', 'VACUUM;');
-- Analyze statistics every Sunday
SELECT cron_schedule_cron('analyze', '0 0 * * 0', 'ANALYZE;');
```

### 2. Data Retention Policy
Automatically delete old data to manage database size.
```sql
-- Delete events older than 30 days, check every hour
SELECT cron_schedule('prune_events', 3600, 'DELETE FROM events WHERE created_at < date("now", "-30 days");');
```

### 3. Periodic Reporting
Generate summary tables for dashboards.
```sql
-- Update daily stats table every 5 minutes
SELECT cron_schedule('refresh_stats', 300,
  'INSERT INTO daily_stats (category, count)
   SELECT category, count(*) FROM visits WHERE date(timestamp) = date("now")
   GROUP BY category
   ON CONFLICT(category) DO UPDATE SET count=excluded.count;');
```

## FAQ & Troubleshooting

### **Q: Does this work with WAL mode?**
**A:** Yes! In fact, WAL mode is recommended for concurrent access. The background thread opens its own connection, so WAL helps avoid locking issues.

### **Q: What happens if I restart my application?**
**A:** Jobs are persisted in the `__cron_jobs` table. When you restart and call `cron_init()`, the engine re-reads the schedule and continues where it left off.

### **Q: My jobs aren't running!**
- Did you call `cron_init()`?
- Is the polling interval too long?
- Check `__cron_log` for errors: `SELECT * FROM __cron_log ORDER BY id DESC LIMIT 5;`

## Architecture

### System Overview
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
```

### Graceful Shutdown Flow
```
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

## API Reference

### 1. Initialization
`cron_init` handles setup based on the chosen mode.

#### Thread Mode (Recommended)
| Signature | Description |
| :--- | :--- |
| `cron_init('thread')` | 1s poll interval, system local timezone. |
| `cron_init('thread', poll_sec)` | Custom poll interval (seconds). |
| `cron_init('thread', timezone)` | Custom Timezone (e.g., '+05:30', 'Z'). |
| `cron_init('thread', poll_sec, timezone)` | Full configuration. |

#### Callback Mode
| Signature | Description |
| :--- | :--- |
| `cron_init('callback')` | Default (1s rate limit, 100 opcodes). |
| `cron_init('callback', rate_limit)` | Custom rate limit (seconds). |
| `cron_init('callback', timezone)` | Custom Timezone. |
| `cron_init('callback', rate_limit, opcodes)` | Custom limit & opcode threshold. |

---

### 2. Job Management

#### `cron_schedule(name, interval_sec, sql, [timeout_ms])`
Schedules a task to run every N seconds.
```sql
-- Run cleanup every hour with a 5s timeout
SELECT cron_schedule('cleanup', 3600, 'DELETE FROM logs;', 5000);
```

#### `cron_schedule_cron(name, cron_expr, sql, [timeout_ms])`
Schedules a task using standard cron syntax (`min hour day month dow`).
```sql
-- Run at 3:30 AM every day
SELECT cron_schedule_cron('backup', '30 3 * * *', 'VACUUM;');
```

#### `cron_pause(name)` / `cron_resume(name)`
Temporarily disable or re-enable a job without deleting it.
```sql
SELECT cron_pause('backup');
SELECT cron_resume('backup');
```

#### `cron_update(name, interval_sec)`
Updates the interval for an existing non-cron job.
```sql
SELECT cron_update('cleanup', 7200); -- Change to every 2 hours
```

#### `cron_run(name)`
Manually triggers a job immediately, regardless of its schedule. Useful for manual overrides or testing.
```sql
SELECT cron_run('backup');
```

#### `cron_delete(name)`
Permanently removes a job from the schedule.
```sql
SELECT cron_delete('cleanup');
```

#### `cron_set_retry(name, max_retries, retry_interval_sec)`
Configures automatic retries for a specific job if it fails.
```sql
-- Retry 3 times, waiting 10s between attempts
SELECT cron_set_retry('api_sync', 3, 10);
```

---

### 3. Introspection & Maintenance

| Function | Description | Returns |
| :--- | :--- | :--- |
| `cron_get(name)` | Detailed settings for a specific job. | JSON Object |
| `cron_list()` | List all jobs with their current state. | JSON Array |
| `cron_status()` | Engine health, mode, and job counts. | JSON Object |
| `cron_job_next(name)` | Next planned timestamp (epoch) for a specific job. | Integer |
| `cron_next_job()` | Details of the single nearest upcoming job. | JSON Object |
| `cron_next_job_time()` | Map of all job names to their next run timestamps. | JSON Object |
| `cron_purge_logs(retention)` | Delete logs older than period (e.g., '-7 days'). | Integer (count) |
| `cron_reset()` | Stop engine and wipe all non-persistent state (for testing). | String |

---

### 4. Configuration
Settings are persisted in the `__cron_config` table.

| Function | Description | Example |
| :--- | :--- | :--- |
| `cron_set_config(k, v)` | Set a persistent configuration value. | `SELECT cron_set_config('timezone', '+05:30');` |
| `cron_get_config(k)` | Retrieve a configuration value. | `SELECT cron_get_config('timezone');` |

## Building from Source

### Prerequisites
- **Linux**: `gcc`, `make`, `libsqlite3-dev`
- **Windows**: `mingw-w64`
- **macOS**: `Xcode Command Line Tools`

### Build
```bash
make linux    # or make windows, make macos
```

### Test
```bash
make test
```

### Leak Check (Linux)
Requires `valgrind`.
```bash
make leak-check
```

### Clean
```bash
make clean
```

### Windows (via Wine on Linux/macOS)
If you don't have a Windows machine, you can run tests using Wine.
1. Install `wine` and `mingw-w64`.
2. Run smoke tests:
   ```bash
   make test-wine-quick
   ```
3. Run full Python test suite (automatically installs Python in Wine):
   ```bash
   make test-wine-full
   ```

### Timezone Support
By default, the cron engine uses UTC. You can configure a global timezone offset to run jobs according to your local time.

```sql
-- Set timezone to India Standard Time (+05:30)
SELECT cron_set_config('timezone', '+05:30');

-- Set timezone to EST (-05:00)
SELECT cron_set_config('timezone', '-05:00');

-- Reset to UTC
SELECT cron_set_config('timezone', 'Z');
```

**Persistence**: The timezone setting is stored in a `__cron_config` table and persists across database restarts.

Alternatively, you can set the timezone during initialization:
```sql
-- Thread mode: cron_init(mode, poll_interval, timezone)
SELECT cron_init('thread', 1, '+05:30');

-- Thread mode: cron_init(mode, timezone) - Uses default poll interval (1s)
SELECT cron_init('thread', '+05:30');

-- Callback mode: cron_init(mode, rate_limit, opcode_threshold, timezone)
SELECT cron_init('callback', 1, 100, '+05:30');

-- Callback mode: cron_init(mode, timezone) - Uses default rate/opcodes
SELECT cron_init('callback', '+05:30');
```

### Compile-Time Options
You can customize the extension via `CFLAGS`:

| Flag | Description | Default |
| :--- | :--- | :--- |
| `-DCRON_USE_LOCAL_TIME` | Forces `ccronexpr` to use `localtime()`. Not recommended since native timezone support covers this. | Disabled |
| `-DTEST_MODE` | Aggressive polling (1s thread) and low callback threshold for testing. | Disabled |
| `-DDEBUG` | Enable verbose debug logging to stderr. | Disabled |
| `-DCRON_SQL_LOG` | Log executed SQL commands to stdout for auditing. | Disabled |

```bash
# Example: Build with debug symbols and logging
make linux CFLAGS="-g -O3 -DDEBUG -DCRON_SQL_LOG -Iheaders -Iexternal"
```

## License
MIT License.
Includes code from `ccronexpr` (Apache 2.0).
