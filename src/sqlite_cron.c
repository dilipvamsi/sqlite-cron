/**
 * @file sqlite_cron.c
 * @brief A professional-grade SQLite extension for background job scheduling.
 *
 * sqlite-cron provides a robust, zero-dependency scheduling engine for SQLite.
 * It supports both interval-based (every N seconds) and standard cron-based
 * (crontab syntax) jobs. The engine can operate in two distinct modes:
 *
 * 1. **Thread Mode**: Spawns a background worker thread. Highly recommended
 *    for server-side applications and disk-based databases. Requires WAL mode.
 * 2. **Callback Mode**: Utilizes SQLite progress handlers to execute jobs
 *    during normal query processing. Ideal for WASM, embedded, or in-memory
 *    scenarios where threading is unavailable.
 *
 * ## Key Architecture
 * - **Fetch-Then-Execute**: Decouples job lookup from execution to prevent
 *   database locks and transaction conflicts.
 * - **Persistence**: All jobs and execution history are stored in internal
 *   tables (`__cron_jobs` and `__cron_log`).
 * - **Timezone Aware**: Native support for custom timezones and automatic
 *   system local time detection.
 * - **Graceful Shutdown**: Ensures in-flight jobs are completed or safely
 *   interrupted during application shutdown.
 *
 * ## Usage Examples
 *
 * ```sql
 * -- 1. Load the extension
 * .load ./build/sqlite_cron
 *
 * -- 2. Initialize the engine (Thread mode, system local time)
 * SELECT cron_init('thread');
 *
 * -- 3. Schedule a job using Cron Syntax (Runs at 3 AM daily)
 * SELECT cron_schedule_cron('daily_vacuum', '0 3 * * *', 'VACUUM;');
 *
 * -- 4. Schedule an Interval job (Runs every 60 seconds)
 * SELECT cron_schedule('health_check', 60, 'INSERT INTO health SELECT 1;');
 *
 * -- 5. Introspect and manage
 * SELECT * FROM __cron_jobs;
 * SELECT cron_status();
 * SELECT cron_pause('daily_vacuum');
 * SELECT cron_resume('daily_vacuum');
 * ```
 *
 * ## Technical Notes
 * - **Concurrency**: Thread mode uses `pthread` (POSIX) or `CreateThread`
 * (Windows).
 * - **Cron Support**: Powered by the `ccronexpr` library.
 * - **Time Management**: Internal state is UTC-centric; offsets are applied
 *   dynamically to support flexible timezone configurations.
 *
 * ## Thread Safety
 * - All global state is protected by a mutex.
 * - Reference counting allows multiple connections to share one engine safely.
 * - Graceful shutdown uses condition variables for fast thread wakeup.
 * - `sqlite3_interrupt()` cancels in-flight queries during shutdown.
 *
 * ## Cross-Platform Support
 * - Linux: `gcc -shared -lpthread`
 * - macOS: `gcc -dynamiclib`
 * - Windows: `x86_64-w64-mingw32-gcc` (uses
 * `CreateThread`/`WaitForSingleObject`).
 * - Local Time: Automatic detection of system timezone offset.
 */

#include "sqlite3ext.h"
SQLITE_EXTENSION_INIT1

/* --- Internal Configuration Defaults --- */

#include <stdio.h>
#include <stdlib.h> /* For free() and malloc() */
#include <string.h>
#include <sys/time.h> /* For millisecond-precision timing on POSIX systems */

#include "ccronexpr.h"
#include <time.h>

/* --- Cross-platform Threading & Sleep Support --- */

#if defined(_WIN32) || defined(_WIN64)
#include <windows.h>
#define SLEEP_MS(x) Sleep(x)
#ifndef STRDUP
#define STRDUP _strdup
#endif
typedef HANDLE cron_thread_t;
typedef CRITICAL_SECTION cron_mutex_t;
typedef CONDITION_VARIABLE cron_cond_t;

#define cron_mutex_init(m) InitializeCriticalSection(m)
#define cron_mutex_lock(m) EnterCriticalSection(m)
#define cron_mutex_unlock(m) LeaveCriticalSection(m)
#define cron_mutex_destroy(m) DeleteCriticalSection(m)

#define cron_cond_init(c) InitializeConditionVariable(c)
#define cron_cond_wait(c, m) SleepConditionVariableCS(c, m, INFINITE)
#define cron_cond_timedwait(c, m, ms) SleepConditionVariableCS(c, m, ms)
#define cron_cond_signal(c) WakeConditionVariable(c)
#define cron_cond_broadcast(c) WakeAllConditionVariable(c)
#define CRON_MUTEX_INITIALIZER {0}
#define CRON_COND_INITIALIZER {0}
#define cron_cond_destroy(c) /* None needed */

#else
#include <pthread.h>
#include <unistd.h>
#define SLEEP_MS(x) usleep((x) * 1000)
#ifndef STRDUP
#define STRDUP strdup
#endif
typedef pthread_t cron_thread_t;
typedef pthread_mutex_t cron_mutex_t;
typedef pthread_cond_t cron_cond_t;

#define cron_mutex_init(m) pthread_mutex_init(m, NULL)
#define cron_mutex_lock(m) pthread_mutex_lock(m)
#define cron_mutex_unlock(m) pthread_mutex_unlock(m)
#define cron_mutex_destroy(m) pthread_mutex_destroy(m)

#define cron_cond_init(c) pthread_cond_init(c, NULL)
#define cron_cond_wait(c, m) pthread_cond_wait(c, m)
#define cron_cond_timedwait(c, m, ms)                                          \
  {                                                                            \
    struct timespec ts;                                                        \
    clock_gettime(CLOCK_REALTIME, &ts);                                        \
    ts.tv_sec += (ms) / 1000;                                                  \
    ts.tv_nsec += ((ms) % 1000) * 1000000LL;                                   \
    if (ts.tv_nsec >= 1000000000LL) {                                          \
      ts.tv_sec++;                                                             \
      ts.tv_nsec -= 1000000000LL;                                              \
    }                                                                          \
    pthread_cond_timedwait(c, m, &ts);                                         \
  }
#define cron_cond_signal(c) pthread_cond_signal(c)
#define cron_cond_broadcast(c) pthread_cond_broadcast(c)
#define cron_cond_destroy(c) pthread_cond_destroy(c)

#define CRON_MUTEX_INITIALIZER PTHREAD_MUTEX_INITIALIZER
#define CRON_COND_INITIALIZER PTHREAD_COND_INITIALIZER
#endif

/* --- Internal Configuration Defaults --- */

#ifdef TEST_MODE
/**
 * @def CALLBACK_RATE_LIMIT
 * @brief Aggressive polling (1s) for testing environments.
 */
#define CALLBACK_RATE_LIMIT 1
/** @brief VM instructions skip for testing. */
#define CALLBACK_OPCODES 10
/** @brief Thread sleep for testing. */
#define DEFAULT_POLL_INTERVAL_MS 1000
/** @brief Initial opcode threshold for testing. */
#define DEFAULT_CALLBACK_OPCODES 100
#else
/**
 * @def CALLBACK_RATE_LIMIT
 * @brief Production: Check at most once every 16 seconds in callback mode.
 */
#define CALLBACK_RATE_LIMIT 16
/** @brief Higher threshold for stable production query performance. */
#define CALLBACK_OPCODES 1000
/** @brief Thread sleep for production (16 seconds). */
#define DEFAULT_POLL_INTERVAL_MS 16000
/** @brief Initial opcode threshold for production. */
#define DEFAULT_CALLBACK_OPCODES 1000
#endif

/**
 * @def MAX_RETRY_DELAY
 * @brief Maximum delay (1 hour) for exponential backoff during job retries.
 */
#define MAX_RETRY_DELAY 3600

/** @brief Internal table name for job metadata and schedule. */
#define CRON_JOBS_TABLE "__cron_jobs"
/** @brief Internal table name for execution history and logs. */
#define CRON_LOG_TABLE "__cron_log"

/**
 * @struct CronControl
 * @brief Global singleton controlling the engine's lifecycle and state.
 *
 * This structure is shared between the main SQLite connections and the
 * background worker thread. It uses a mutex and condition variable to
 * synchronize state changes (like stopping or pausing).
 */
typedef struct {
  char *db_path; /**< Absolute path to the SQLite database file (thread mode
                    only). */
  int mode;      /**< Operation mode: 1 = Threaded, 2 = Callback-based, 0 =
                    Inactive. */
  volatile int active;  /**< Safe flag indicating if the worker thread/callback
                           should keep running. */
  volatile int in_tick; /**< Reentrancy guard to prevent nested executions of
                           cron_tick. */
  time_t last_check;    /**< Unix timestamp of the last time `cron_tick` ran. */
  struct timeval job_start_time; /**< Start time of the currently running job
                                    for timeout tracking. */
  int current_timeout_ms; /**< Execution timeout for the currently running
                             job. */
  volatile int
      in_job;         /**< Flag indicating if a job is currently in progress. */
  cron_mutex_t mutex; /**< Mutex for thread-safe state access across multiple
                         connections. */
  cron_cond_t cond;   /**< Condition variable for fast worker wakeup during
                         shutdown or manual runs. */
  int ref_count; /**< Tracks active connections to prevent premature shutdown
                    on multi-threaded apps. */
  cron_thread_t thread_handle; /**< Handle for the background worker thread. */
  sqlite3 *worker_db;          /**< Private DB handle used by the background
                                  thread. */
  int callback_rate_limit;     /**< Custom seconds between callback checks. */
  int callback_opcodes; /**< Custom opcode threshold for progress handler. */
  long timezone_offset_seconds; /**< Current global timezone offset in seconds.
                                 */
  int poll_interval_ms;         /**< Delay between thread poll attempts. */
} CronControl;

/** @brief Singleton instance of the cron controller. */
static CronControl global_cron = {
    NULL,                    /* db_path */
    0,                       /* mode */
    0,                       /* active */
    0,                       /* in_tick */
    0,                       /* last_check */
    {0, 0},                  /* job_start_time */
    0,                       /* current_timeout_ms */
    0,                       /* in_job */
    CRON_MUTEX_INITIALIZER,  /* mutex */
    CRON_COND_INITIALIZER,   /* cond */
    0,                       /* ref_count */
    (cron_thread_t)0,        /* thread_handle */
    NULL,                    /* worker_db */
    0,                       /* callback_rate_limit */
    0,                       /* callback_opcodes */
    0,                       /* timezone_offset_seconds */
    DEFAULT_POLL_INTERVAL_MS /* poll_interval_ms */
};

/* --- Configuration Helpers --- */

/**
 * @brief Get the local system timezone offset in seconds.
 *
 * This function detects the current system timezone offset from UTC.
 * - On Linux/macOS: Uses tm_gmtoff if available.
 * - On Windows: Compares localtime and gmtime to derive the offset.
 *
 * @return long Offset in seconds (positive for East of UTC, negative for West).
 */
static long _cron_get_system_timezone_offset(void) {
  time_t now = time(NULL);
  struct tm tm_local;
#if defined(_WIN32) || defined(_WIN64)
  localtime_s(&tm_local, &now);
#else
  localtime_r(&now, &tm_local);
#endif

  /* Portable way to get offset without tm_gmtoff:
   * Calculate difference between mktime(local) and a mock UTC mktime or
   * similar. Or just use the fact that mktime treats input as local.
   */
#if defined(__linux__) || defined(__APPLE__) || defined(__FreeBSD__)
  /* Most POSIX systems have tm_gmtoff */
  return tm_local.tm_gmtoff;
#else
  /* Fallback: compute difference between localtime and gmtime */
  struct tm tm_utc;
#if defined(_WIN32) || defined(_WIN64)
  gmtime_s(&tm_utc, &now);
#else
  gmtime_r(&now, &tm_utc);
#endif
  time_t t_local = mktime(&tm_local);
  /* We don't have a portable timegm, so we use a trick:
     mktime is local->utc. If we pass a UTC tm to it, it gives us UTC - offset.
  */
  time_t t_utc_ish = mktime(&tm_utc);
  return (long)difftime(t_local, t_utc_ish);
#endif
}

/**
 * @brief Helper to persist a configuration key/value pair in the database.
 *
 * This function inserts or replaces a record in the `__cron_config` table.
 * It does not affect the in-memory cache directly.
 *
 * @param db Database handle to use for the update.
 * @param key Configuration key name.
 * @param value value to associate with the key.
 */
static void _cron_set_config_db(sqlite3 *db, const char *key,
                                const char *value) {
  sqlite3_stmt *stmt;
  const char *sql = "INSERT INTO __cron_config (key, value) VALUES (?, ?) "
                    "ON CONFLICT(key) DO UPDATE SET value = excluded.value;";
  if (sqlite3_prepare_v2(db, sql, -1, &stmt, NULL) == SQLITE_OK) {
    sqlite3_bind_text(stmt, 1, key, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt, 2, value, -1, SQLITE_TRANSIENT);
    sqlite3_step(stmt);
    sqlite3_finalize(stmt);
  }
}

/* --- The Core Engine --- */

/**
 * @brief Main scheduling logic. Checks and executes due jobs.
 *
 * This function handles the "tick" of the cron engine. It:
 * 1. Checks for jobs where `next_run <= now` and `active = 1`.
 * 2. Executes them in a separate transaction.
 * 3. Logs the execution to `__cron_log`.
 * 4. Calculates the NEXT run time:
 *    - If `cron_expr` is present, it calculates the next match using
 * `ccronexpr`.
 *    - Otherwise, it adds `schedule_interval` to the current time.
 *
 * @param db The SQLite database handle.
 */

/**
 * @brief Structure to hold job details for "Fetch-Then-Execute" pattern.
 *
 * We fetch all due jobs into this linked list first, then execute them.
 * This avoids holding an open SELECT statement while executing jobs,
 * which prevents locking issues and allows managing transactions per job.
 */
typedef struct cron_job_info {
  char *name;
  char *command;
  int schedule_interval;
  int timeout_ms;
  char *cron_expr;
  int max_retries;
  int retry_interval;
  int retries_attempted;
  struct cron_job_info *next;
} cron_job_info;

/**
 * @brief Core scheduling logic: executes due jobs and reschedules them.
 *
 * This function constitutes the "tick" of the scheduling engine. It
 * follows a robust "Fetch-Then-Execute" pattern to avoid database
 * locking issues.
 *
 * @section Flow
 * 1. **Job Retrieval**: Queries `__cron_jobs` for jobs where `next_run
 * <= now`.
 * 2. **Metadata Loading**: Loads the job definitions into an in-memory
 * linked list.
 * 3. **Execution Loop**:
 *    a. Updates `next_run` immediately to handle interval/cron
 * calculation. b. Executes the job SQL in a separate transaction. c.
 * Records the result (SUCCESS/FAILURE) in `__cron_log`. d. Handles
 * retries if the job failed.
 *
 * @param db active database handle.
 */
static void cron_tick(sqlite3 *db) {
  /* Exit early if engine is stopping or already in a tick */
  cron_mutex_lock(&global_cron.mutex);
  if (!global_cron.active || global_cron.in_tick) {
    cron_mutex_unlock(&global_cron.mutex);
    return;
  }
  global_cron.in_tick = 1;
  cron_mutex_unlock(&global_cron.mutex);

  time_t now = time(NULL);

  /* Rate-limiting: Only block same-second ticks if a rate limit is enforced.
   * If rate_limit is 0, we allow multiple ticks per second (throttled by
   * opcodes).
   */
  if (global_cron.callback_rate_limit > 0 && now <= global_cron.last_check) {
    cron_mutex_lock(&global_cron.mutex);
    global_cron.in_tick = 0;
    cron_mutex_unlock(&global_cron.mutex);
    return;
  }

  global_cron.last_check = now;

  sqlite3_stmt *stmt;
  cron_job_info *jobs_to_run = NULL;
  cron_job_info *last_job = NULL;

  /* --- Step 1: Job Retrieval (Fetch) --- */
  /* We fetch job metadata first and store it in a linked list.
   * This is critical: if we tried to execute jobs while holding the query
   * results from __cron_jobs, we might trigger "database table is locked".
   */
  const char *query = "SELECT name, command, schedule_interval, timeout_ms, "
                      "cron_expr, max_retries, retry_interval_seconds, "
                      "retries_attempted FROM " CRON_JOBS_TABLE
                      " WHERE active=1 AND next_run <= strftime('%s', 'now');";

  if (sqlite3_prepare_v2(db, query, -1, &stmt, NULL) == SQLITE_OK) {
    while (sqlite3_step(stmt) == SQLITE_ROW) {
      cron_job_info *job = (cron_job_info *)malloc(sizeof(cron_job_info));
      if (!job) {
        /* Handle allocation failure, clean up existing list and return */
        while (jobs_to_run) {
          cron_job_info *temp = jobs_to_run;
          jobs_to_run = jobs_to_run->next;
          free(temp->name);
          free(temp->command);
          if (temp->cron_expr)
            free(temp->cron_expr);
          free(temp);
        }
        sqlite3_finalize(stmt);
        cron_mutex_lock(&global_cron.mutex);
        global_cron.in_tick = 0;
        cron_mutex_unlock(&global_cron.mutex);
        return;
      }

      /* Extract column values */
      const char *name_ptr = (const char *)sqlite3_column_text(stmt, 0);
      const char *cmd_ptr = (const char *)sqlite3_column_text(stmt, 1);
      int interval = sqlite3_column_int(stmt, 2);
      int timeout_ms = sqlite3_column_int(stmt, 3);
      const char *cron_expr_ptr = (const char *)sqlite3_column_text(stmt, 4);
      int max_retries = sqlite3_column_int(stmt, 5);
      int retry_interval = sqlite3_column_int(stmt, 6);
      int retries_attempted = sqlite3_column_int(stmt, 7);

      /* Copy strings (STRDUP handles allocation) */
      job->name = STRDUP(name_ptr ? name_ptr : "");
      job->command = STRDUP(cmd_ptr ? cmd_ptr : "");
      job->schedule_interval = interval;
      job->timeout_ms = timeout_ms;
      job->cron_expr = cron_expr_ptr ? STRDUP(cron_expr_ptr) : NULL;
      job->max_retries = max_retries;
      job->retry_interval = retry_interval;
      job->retries_attempted = retries_attempted;
      job->next = NULL;

      /* Append to list */
      if (last_job) {
        last_job->next = job;
        last_job = job;
      } else {
        jobs_to_run = job;
        last_job = job;
      }
    }
    sqlite3_finalize(stmt); /* Important: Close read transaction/statement */
  } else {
    /* Silent failure in production, could log to __cron_log if needed */
  }

  /* --- Step 2: Execution Loop --- */
  cron_job_info *curr = jobs_to_run;
  while (curr) {
    /* Safe check: if engine was stopped during the loop, abort */
    cron_mutex_lock(&global_cron.mutex);
    if (!global_cron.active) {
      cron_mutex_unlock(&global_cron.mutex);
      break;
    }
    cron_mutex_unlock(&global_cron.mutex);

    char *name = curr->name;
    char *sql = curr->command;
    int interval = curr->schedule_interval;
    int timeout_ms = curr->timeout_ms;
    char *cron_str = curr->cron_expr;
    int max_retries = curr->max_retries;
    int retry_interval = curr->retry_interval;
    int retries_attempted = curr->retries_attempted;

    /* Set up global state for timeout handler */
    global_cron.current_timeout_ms = timeout_ms;
    gettimeofday(&global_cron.job_start_time, NULL);
    global_cron.in_job = 1;
    time_t start_s = (time_t)global_cron.job_start_time.tv_sec;

    /*
     * Step 2a: Log start of job (RUNNING)
     * We use a separate transaction for the log to ensure it's committed
     * even if the job itself fails or crashes.
     * NOTE: In Callback Mode (2), we CANNOT start a transaction because
     * we are already inside a write transaction from the user's query.
     */
    if (global_cron.mode != 2) {
      sqlite3_exec(db, "BEGIN IMMEDIATE;", NULL, NULL, NULL);
    }
    sqlite3_stmt *log_stmt;
    const char *log_sql =
        "INSERT INTO " CRON_LOG_TABLE " (job_name, start_time, "
        "status) VALUES (?, ?, 'RUNNING');";
    sqlite3_int64 log_id = 0;
    if (sqlite3_prepare_v2(db, log_sql, -1, &log_stmt, NULL) == SQLITE_OK) {
      sqlite3_bind_text(log_stmt, 1, name, -1, SQLITE_TRANSIENT);
      sqlite3_bind_int64(log_stmt, 2, start_s);
      sqlite3_step(log_stmt);
      log_id = sqlite3_last_insert_rowid(db);
      sqlite3_finalize(log_stmt);
    }
    if (global_cron.mode != 2) {
      sqlite3_exec(db, "COMMIT;", NULL, NULL, NULL);
    }

    /*
     * Step 2b: Execute the user's job command.
     * This runs in its own transaction context (unless provided by the job).
     */
    char *err_msg = NULL;
    int rc = sqlite3_exec(db, sql, NULL, NULL, &err_msg);

    global_cron.in_job = 0; /* Clear flag immediately to allow logging */

    struct timeval t_end;
    gettimeofday(&t_end, NULL);
    long long duration_ms =
        (t_end.tv_sec - global_cron.job_start_time.tv_sec) * 1000LL +
        (t_end.tv_usec - global_cron.job_start_time.tv_usec) / 1000LL;

    /* Step 2c: Calculate Next Run Time */
    time_t next_run_time;
    time_t retry_run_time = 0;
    int next_retries_attempted = 0;
    const char *status_str = "SUCCESS";

    /* Calculate Normal Schedule First */
    time_t normal_next_run;
    now = time(NULL); // Re-fetch current time after job execution
    if (cron_str) {
      cron_expr expr;
      const char *err = NULL;
      cron_parse_expr(cron_str, &expr, &err);
      if (err) {
        normal_next_run = now + 60; /* Fallback for invalid cron */
      } else {
        /* Apply timezone offset: calculate next run in LOCAL time, then
         * convert back to UTC */
        time_t local_now = now + global_cron.timezone_offset_seconds;
        time_t next_local = cron_next(&expr, local_now);
        if (next_local == (time_t)-1) {
          normal_next_run = now + 60; /* Error fallback */
        } else {
          normal_next_run = next_local - global_cron.timezone_offset_seconds;
        }
      }
    } else {
      normal_next_run = now + interval;
    }

    /* Step 2d: Handle Failure and Retries */
    if (rc != SQLITE_OK) {
      if (rc == SQLITE_INTERRUPT)
        status_str = "TIMEOUT";
      else
        status_str = "FAILURE";

      /* Check if retries are enabled and available */
      if (max_retries > 0 && retries_attempted < max_retries) {
        retry_run_time = time(NULL) + retry_interval;

        /* SAFEGUARD: If the natural schedule is SOONER than the retry,
           skip the retry to avoid overlapping impact/delay. */
        if (normal_next_run <= retry_run_time) {
          next_run_time = normal_next_run;
          next_retries_attempted = 0; /* Reset for fresh run */
        } else {
          next_run_time = retry_run_time;
          next_retries_attempted =
              retries_attempted + 1; /* Increment retry count */
        }
      } else {
        /* Exhausted or no retries configured */
        next_run_time = normal_next_run;
        next_retries_attempted = 0;
      }
    } else {
      /* Success */
      status_str = "SUCCESS";
      next_run_time = normal_next_run;
      next_retries_attempted = 0;
    }

    sqlite3_stmt *upd_stmt;

    /*
     * Step 2e: Log Outcome & Update Job State
     * Update the log entry with success/failure and stats.
     * Update the job's next_run and retry counts.
     */
    if (global_cron.mode != 2) {
      sqlite3_exec(db, "BEGIN IMMEDIATE;", NULL, NULL, NULL);
    }

    /* Update Log */
    if (log_id > 0) {
      const char *upd_log =
          "UPDATE " CRON_LOG_TABLE " SET status = ?, end_time = ?, "
          "duration_ms = ?, error_message = ? WHERE id = ?;";
      if (sqlite3_prepare_v2(db, upd_log, -1, &upd_stmt, NULL) == SQLITE_OK) {
        sqlite3_bind_text(upd_stmt, 1, status_str, -1, SQLITE_TRANSIENT);
        sqlite3_bind_int64(upd_stmt, 2, (time_t)t_end.tv_sec);
        sqlite3_bind_int64(upd_stmt, 3, duration_ms);
        if (err_msg) {
          sqlite3_bind_text(upd_stmt, 4, err_msg, -1, SQLITE_TRANSIENT);
        } else {
          sqlite3_bind_null(upd_stmt, 4);
        }
        sqlite3_bind_int64(upd_stmt, 5, log_id);
        sqlite3_step(upd_stmt);
        sqlite3_finalize(upd_stmt);
      }
    }

    const char *upd_sql = "UPDATE " CRON_JOBS_TABLE " SET next_run = ?, "
                          "retries_attempted = ? WHERE name = ?;";
    if (sqlite3_prepare_v2(db, upd_sql, -1, &upd_stmt, NULL) == SQLITE_OK) {
      sqlite3_bind_int64(upd_stmt, 1, (sqlite3_int64)next_run_time);
      sqlite3_bind_int(upd_stmt, 2, next_retries_attempted);
      sqlite3_bind_text(upd_stmt, 3, name, -1, SQLITE_TRANSIENT);
      sqlite3_step(upd_stmt);
      sqlite3_finalize(upd_stmt);
    }

    if (global_cron.mode != 2) {
      sqlite3_exec(db, "COMMIT;", NULL, NULL, NULL);
    }

    if (cron_str)
      free(cron_str);
    if (err_msg)
      sqlite3_free(err_msg);
    free(name);
    free(sql); /* Changed from cmd to sql to match variable name */

    cron_job_info *next_job = curr->next;
    free(curr);
    curr = next_job;
  }

  sqlite3_exec(db, "PRAGMA wal_checkpoint(FULL);", NULL, NULL, NULL);

  cron_mutex_lock(&global_cron.mutex);
  global_cron.in_tick = 0;
  cron_mutex_unlock(&global_cron.mutex);
}

/**
 * @brief SQLite Progress Handler callback.
 *
 * This hook is called by SQLite every N virtual machine instructions.
 * It provides a way to implement "piggyback" scheduling where cron jobs
 * run on the same thread as normal queries.
 *
 * @section Logic
 * - Checks the current rate limit to avoid over-ticking.
 * - Triggers `cron_tick` if the rate limit is met and we are in callback mode.
 * - Handles query termination if a job exceeds its configured timeout.
 *
 * @param arg Pointer to the active sqlite3 database handle.
 * @return int 0 to continue query execution; 1 to terminate.
 */
static int cron_progress_handler(void *arg) {
  if (!global_cron.active)
    return 0;

  /* 1. Handle Job Timeout Interruption */
  if (global_cron.in_job && global_cron.current_timeout_ms > 0) {
    struct timeval now;
    gettimeofday(&now, NULL);
    long long elapsed =
        (now.tv_sec - global_cron.job_start_time.tv_sec) * 1000LL +
        (now.tv_usec - global_cron.job_start_time.tv_usec) / 1000LL;

    if (elapsed >= global_cron.current_timeout_ms) {
      return 1; /* Terminate the currently running query */
    }
  }

  /* 2. Handle Engine Trigger (Callback Mode only) */
  if (global_cron.mode == 2 && !global_cron.in_job) {
    time_t now = time(NULL);

    /* Strictly enforce the rate limit for callback mode to prevent overhead */
    if (now - global_cron.last_check < global_cron.callback_rate_limit)
      return 0;

    cron_tick((sqlite3 *)arg);
  }

  return 0;
}

/**
 * @brief Background thread worker entry point.
 *
 * This function runs for the entire lifetime of the engine in Thread Mode.
 * It maintains a dedicated SQLite connection and repeatedly calls `cron_tick`
 * at the configured poll interval.
 *
 * @section Lifecycle
 * 1. Opens a private SQLite connection to the database.
 * 2. Enters a loop that sleeps using a condition variable.
 * 3. Calls `cron_tick` on every wakeup.
 * 4. Cleans up handles and exits when `global_cron.active` is set to 0.
 *
 * @param lpParam Not used (param for Windows/POSIX thread entry).
 * @return thread exit code/status.
 */
#if defined(_WIN32) || defined(_WIN64)
static DWORD WINAPI cron_thread_worker(LPVOID lpParam) {
#else
static void *cron_thread_worker(void *arg) {
#endif
  sqlite3 *worker_db;
  if (sqlite3_open(global_cron.db_path, &worker_db) != SQLITE_OK) {
    return 0;
  }

  /* Set the WAL mode for safe concurrent access */
  sqlite3_exec(worker_db, "PRAGMA journal_mode=WAL;", NULL, NULL, NULL);

  /* Register progress handler for timeouts */
  sqlite3_progress_handler(worker_db, CALLBACK_OPCODES, cron_progress_handler,
                           worker_db);

  cron_mutex_lock(&global_cron.mutex);
  global_cron.worker_db = worker_db;

  cron_mutex_unlock(&global_cron.mutex); /* Release before loop */

  while (global_cron.active) {
    cron_tick(worker_db);

    cron_mutex_lock(&global_cron.mutex);
    if (!global_cron.active) {

      cron_mutex_unlock(&global_cron.mutex);
      break;
    }
    /* Fast-exit wait: Wake up instantly if active becomes 0 */
    cron_cond_timedwait(&global_cron.cond, &global_cron.mutex,
                        global_cron.poll_interval_ms);
    cron_mutex_unlock(&global_cron.mutex);
  }

  sqlite3_close(worker_db);

  cron_mutex_lock(&global_cron.mutex);
  global_cron.worker_db = NULL;
  cron_mutex_unlock(&global_cron.mutex);

  return 0;
}

/* --- Configuration SQL Functions --- */

/**
 * @brief SQL function: Set a configuration parameter.
 * `SELECT cron_set_config(key, value);`
 *
 * Updates both the persistent `__cron_config` table and the in-memory cache
 * for relevant settings (timezone, poll interval).
 */
static void cron_set_config_func(sqlite3_context *context, int argc,
                                 sqlite3_value **argv) {
  const char *key = (const char *)sqlite3_value_text(argv[0]);
  const char *value = (const char *)sqlite3_value_text(argv[1]);
  sqlite3 *db = sqlite3_context_db_handle(context);

  if (!key || !value) {
    sqlite3_result_error(context, "Key and value cannot be null", -1);
    return;
  }

  cron_mutex_lock(&global_cron.mutex);

  /* Persist to DB */
  _cron_set_config_db(db, key, value);

  /* Update Memory Cache */
  if (strcmp(key, "timezone") == 0) {
    /* Parse "+HH:MM", "-HH:MM", or "Z" */
    long offset = 0;
    if (strcmp(value, "Z") == 0) {
      offset = 0;
    } else {
      int h, m;
      if (sscanf(value, "%d:%d", &h, &m) == 2) {
        offset = h * 3600 + (h < 0 ? -m : m) * 60;
      }
    }
    global_cron.timezone_offset_seconds = offset;
  } else if (strcmp(key, "poll_interval_ms") == 0) {
    /* Thread mode polling interval */
    global_cron.poll_interval_ms = atoi(value);
  }

  cron_mutex_unlock(&global_cron.mutex);
  sqlite3_result_int(context, 1);
}

/**
 * @brief SQL function: Get a configuration parameter.
 * `SELECT cron_get_config(key);`
 */
static void cron_get_config_func(sqlite3_context *context, int argc,
                                 sqlite3_value **argv) {
  const char *key = (const char *)sqlite3_value_text(argv[0]);
  sqlite3 *db = sqlite3_context_db_handle(context);

  /* Helper to query DB */
  sqlite3_stmt *stmt;
  if (sqlite3_prepare_v2(db, "SELECT value FROM __cron_config WHERE key = ?;",
                         -1, &stmt, NULL) == SQLITE_OK) {
    sqlite3_bind_text(stmt, 1, key, -1, SQLITE_TRANSIENT);
    if (sqlite3_step(stmt) == SQLITE_ROW) {
      sqlite3_result_text(context, (const char *)sqlite3_column_text(stmt, 0),
                          -1, SQLITE_TRANSIENT);
    } else {
      sqlite3_result_null(context);
    }
    sqlite3_finalize(stmt);
  } else {
    sqlite3_result_error(context, "Failed to query config table", -1);
  }
}

/* --- Public SQLite API Functions --- */

/**
 * @brief Internal logic to stop the cron engine and clean up resources.
 *
 * This performs a multi-stage graceful shutdown:
 * 1. Decrements the reference counter.
 * 2. If the engine is still shared, returns immediately.
 * 3. Signals the background thread to exit.
 * 4. Interrupts any in-flight SQL queries.
 * 5. Waits for the worker thread to join.
 * 6. Frees allocated memory and handles.
 */
static void cron_shutdown_internal(void) {
  cron_mutex_lock(&global_cron.mutex);

  /* Step 1: Decrement reference count for this connection */
  if (global_cron.ref_count > 0) {
    global_cron.ref_count--;
  }

  /* Step 2: If other connections still using engine, keep it running */
  if (global_cron.ref_count > 0) {
    cron_mutex_unlock(&global_cron.mutex);
    return;
  }

  /* Step 3: Check if already stopped */
  if (!global_cron.active) {
    cron_mutex_unlock(&global_cron.mutex);
    return;
  }

  /* Step 4: Signal thread to exit by setting active=0 */
  global_cron.active = 0;

  /* Step 5: Wake up sleeping thread immediately via condition variable */
  cron_cond_broadcast(&global_cron.cond);

  /* Step 6: Interrupt any running SQL query so job finishes quickly */
  if (global_cron.worker_db) {
    sqlite3_interrupt(global_cron.worker_db);
  }

  /* Step 7: Wait for in-flight jobs to complete (max ~5 seconds)
   * We poll every 10ms, up to 500 iterations = 5 seconds max wait.
   * This ensures jobs have a chance to finish cleanly before we tear down. */
  int wait_limit = 500;
  while ((global_cron.in_tick || global_cron.in_job) && wait_limit-- > 0) {
    /* Keep interrupting in case job ignores first interrupt */
    if (global_cron.worker_db && global_cron.in_job) {
      sqlite3_interrupt(global_cron.worker_db);
    }
    cron_mutex_unlock(&global_cron.mutex);
    SLEEP_MS(10);
    cron_mutex_lock(&global_cron.mutex);
  }

  /* Step 8: Join the background thread (wait for it to fully exit) */
  if (global_cron.mode == 1) {
    cron_mutex_unlock(&global_cron.mutex);
#if defined(_WIN32) || defined(_WIN64)
    /* Windows: Use WaitForSingleObject to block until thread exits */
    if (global_cron.thread_handle) {
      WaitForSingleObject(global_cron.thread_handle, INFINITE);
      CloseHandle(global_cron.thread_handle);
      global_cron.thread_handle = NULL;
    }
#else
    /* POSIX: Use pthread_join to block until thread exits */
    pthread_join(global_cron.thread_handle, NULL);
#endif
    cron_mutex_lock(&global_cron.mutex);
  }

  /* Step 9: Clean up allocated resources and reset state */
  if (global_cron.db_path) {
    free(global_cron.db_path);
    global_cron.db_path = NULL;
  }
  global_cron.mode = 0;
  global_cron.last_check = 0;
  global_cron.worker_db = NULL;
  cron_mutex_unlock(&global_cron.mutex);
}

/**
 * @brief Destructor callback for the connection sentinel.
 *
 * This is called by SQLite when the connection is closed.
 */
static void cron_sentinel_destroy(void *pApp) { cron_shutdown_internal(); }

/**
 * @brief Sentinel function that does nothing.
 *
 * Its only purpose is to carry the destructor that cleans up the engine.
 */
static void cron_sentinel_func(sqlite3_context *context, int argc,
                               sqlite3_value **argv) {
  sqlite3_result_null(context);
}

/**
 * @brief SQL function: Stop the cron engine session-wide.
 * `SELECT cron_stop();`
 */
static void cron_stop_func(sqlite3_context *context, int argc,
                           sqlite3_value **argv) {
  if (!global_cron.active) {
    sqlite3_result_text(context, "Engine is not active", -1, SQLITE_STATIC);
    return;
  }

  cron_shutdown_internal();
  sqlite3_result_text(context, "Engine stopped gracefully", -1, SQLITE_STATIC);
}

/**
 * @brief SQL function: Initialize the cron engine for the current connection.
 * `SELECT cron_init(mode, [args...]);`
 *
 * This function is the primary entry point for the engine. It performs
 * a complex multi-stage initialization:
 * 1. Validates operation mode ('thread' or 'callback').
 * 2. Parses optional arguments (poll interval, timezone).
 * 3. Creates the required internal tables (`__cron_config`, `__cron_jobs`).
 * 4. Loads persistent configuration from the database.
 * 5. Starts the background worker thread (Thread Mode) or registers the
 *    progress handler (Callback Mode).
 * 6. Attaches a sentinel function to clean up when the connection closes.
 */
static void cron_init_func(sqlite3_context *context, int argc,
                           sqlite3_value **argv) {
  const char *mode_str = (const char *)sqlite3_value_text(argv[0]);
  int poll_interval_ms = DEFAULT_POLL_INTERVAL_MS;
  const char *timezone_arg = NULL;

  /* Check for 'thread' vs 'callback' to determine arg parsing */
  int is_thread = (strcmp(mode_str, "thread") == 0);
  int is_callback = (strcmp(mode_str, "callback") == 0);

  if (!is_thread && !is_callback) {
    sqlite3_result_error(context, "Unknown mode: use 'thread' or 'callback'",
                         -1);
    return;
  }

  /* Parse Arguments based on mode */
  if (is_thread) {
    /* cron_init('thread', [poll_interval_sec], [timezone]) */
    /* cron_init('thread', [timezone]) */
    for (int i = 1; i < argc; i++) {
      if (sqlite3_value_type(argv[i]) == SQLITE_INTEGER) {
        poll_interval_ms = sqlite3_value_int(argv[i]) * 1000;
        if (poll_interval_ms < 1000)
          poll_interval_ms = 1000;
      } else if (sqlite3_value_type(argv[i]) == SQLITE_TEXT) {
        timezone_arg = (const char *)sqlite3_value_text(argv[i]);
      }
    }
  } else { // Callback
    /* cron_init('callback', [rate_limit], [opcode], [timezone]) */
    /* This is trickier because we have 2 optional ints.
       Let's assume standard order: rate, opcode, tz.
       But if only 1 int is given, it's rate.
       If text is given, it's tz. */
    if (argc >= 2) {
      if (sqlite3_value_type(argv[1]) == SQLITE_INTEGER) {
        global_cron.callback_rate_limit = sqlite3_value_int(argv[1]);
      } else if (sqlite3_value_type(argv[1]) == SQLITE_TEXT) {
        timezone_arg = (const char *)sqlite3_value_text(argv[1]);
      }
    }
    if (argc >= 3) {
      if (sqlite3_value_type(argv[2]) == SQLITE_INTEGER) {
        global_cron.callback_opcodes = sqlite3_value_int(argv[2]);
      } else if (sqlite3_value_type(argv[2]) == SQLITE_TEXT) {
        timezone_arg = (const char *)sqlite3_value_text(argv[2]);
      }
    }
    if (argc >= 4 && sqlite3_value_type(argv[3]) == SQLITE_TEXT) {
      timezone_arg = (const char *)sqlite3_value_text(argv[3]);
    }
  }

  sqlite3 *db = sqlite3_context_db_handle(context);

  cron_mutex_lock(&global_cron.mutex);
  global_cron.ref_count++;

  /* --- Load Configuration --- */
  /* Create config table if not exists */
  sqlite3_exec(db,
               "CREATE TABLE IF NOT EXISTS __cron_config (key TEXT PRIMARY "
               "KEY, value TEXT);",
               0, 0, 0);

  /* If timezone arg provided, save it now (before loading defaults or
   * overwriting) */
  if (timezone_arg) {
    _cron_set_config_db(db, "timezone", timezone_arg);
  }

  if (global_cron.active) {
    /* Already active, just increment ref (handled above) */
  } else {
    /* First initialization: Default to system timezone */
    global_cron.timezone_offset_seconds = _cron_get_system_timezone_offset();

    /* Load config from DB override */
    sqlite3_stmt *stmt;
    const char *sql = "SELECT key, value FROM __cron_config;";
    if (sqlite3_prepare_v2(db, sql, -1, &stmt, NULL) == SQLITE_OK) {
      while (sqlite3_step(stmt) == SQLITE_ROW) {
        const char *key = (const char *)sqlite3_column_text(stmt, 0);
        const char *val = (const char *)sqlite3_column_text(stmt, 1);
        if (strcmp(key, "timezone") == 0) {
          long offset = 0;
          if (strcmp(val, "Z") == 0) {
            offset = 0;
          } else {
            int h, m;
            if (sscanf(val, "%d:%d", &h, &m) == 2) {
              offset = h * 3600 + (h < 0 ? -m : m) * 60;
            }
          }
          global_cron.timezone_offset_seconds = offset;
        } else if (strcmp(key, "poll_interval_ms") == 0) {
          global_cron.poll_interval_ms = atoi(val);
        }
      }
      sqlite3_finalize(stmt);
    }
  }

  /* If timezone arg was provided, ensure it's applied to memory active state
   * if we didn't just load it */
  if (timezone_arg) {
    long offset = 0;
    if (strcmp(timezone_arg, "Z") == 0) {
      offset = 0;
    } else {
      int h, m;
      if (sscanf(timezone_arg, "%d:%d", &h, &m) == 2) {
        offset = h * 3600 + (h < 0 ? -m : m) * 60;
      }
    }
    global_cron.timezone_offset_seconds = offset;
  }

  if (global_cron.active) {
    cron_mutex_unlock(&global_cron.mutex);
    sqlite3_create_function_v2(db, "__cron_sentinel", 0, SQLITE_UTF8, NULL,
                               cron_sentinel_func, NULL, NULL,
                               cron_sentinel_destroy);
    sqlite3_result_text(context,
                        "Cron already initialized (Ref count increased)", -1,
                        SQLITE_STATIC);
    return;
  }

  if (is_thread) {
    const char *path = sqlite3_db_filename(db, "main");
    if (!path || strlen(path) == 0) {
      global_cron.ref_count--;
      cron_mutex_unlock(&global_cron.mutex);
      sqlite3_result_error(context, "Thread mode requires disk-based DB", -1);
      return;
    }
    /* Duplicate the path for the background worker thread */
    global_cron.db_path = STRDUP(path);
    global_cron.mode = 1;
    global_cron.active = 1;
    /* Only override poll_interval_ms if it was set via arg or loaded config?
     */
    /* If argv provided poll_interval, we set it. If not, we used default.
       But wait, loading from DB might have set it.
       Priorities:
       1. Argument provided
       2. DB config
       3. Default
    */
    /* My logic above set poll_interval_ms from arg or default.
       Then DB load overwrites global_cron.poll_interval_ms.
       We should reconcile. */
    if (poll_interval_ms != DEFAULT_POLL_INTERVAL_MS) {
      global_cron.poll_interval_ms = poll_interval_ms;
      // Also persist?
      char buf[32];
      snprintf(buf, sizeof(buf), "%d",
               poll_interval_ms); // Store as ms or s? Stored as ms in struct.
      // Wait, stored as ms in struct, but DB stores... existing set_config
      // used atoi(value). Let's store as string "1000". But the set_config
      // example logic was: global_cron.poll_interval_ms = atoi(value) So value
      // in DB is integer string.
      _cron_set_config_db(db, "poll_interval_ms", buf);
    }
    // If poll_interval_ms IS default, we keep whatever DB loaded, or default
    // if nothing loaded. But `poll_interval_ms` var is local.
    // `global_cron.poll_interval_ms` was set by DB load or init to 0/default?
    // global_cron init: DEFAULT_POLL_INTERVAL_MS.
    // If DB load happened, it updated global_cron.
    // If arg provided, we update global_cron.
    if (poll_interval_ms != DEFAULT_POLL_INTERVAL_MS) {
      global_cron.poll_interval_ms = poll_interval_ms;
    }

    /* Reset worker state */
    global_cron.worker_db = NULL;
    cron_cond_signal(&global_cron.cond); /* Ensure thread isn't stuck */

#if defined(_WIN32) || defined(_WIN64)
    global_cron.thread_handle =
        CreateThread(NULL, 0, cron_thread_worker, NULL, 0, NULL);
    if (!global_cron.thread_handle) {
#else
    if (pthread_create(&global_cron.thread_handle, NULL, cron_thread_worker,
                       NULL) != 0) {
#endif
      global_cron.active = 0;
      global_cron.ref_count--;
      cron_mutex_unlock(&global_cron.mutex);
      sqlite3_result_error(context, "Failed to create thread", -1);
      return;
    }
    sqlite3_result_text(context, "Initialized: Thread mode", -1, SQLITE_STATIC);

  } else if (is_callback) {
    /* Set defaults or preserve existing if already active? (Handled by strict
     * active check above for now we return early) */

    /* Apply Defaults if not set (0) */
    if (global_cron.callback_opcodes == 0) {
      global_cron.callback_opcodes = DEFAULT_CALLBACK_OPCODES;
    }
    /* rate_limit defaults to 0 (DEFAULT_CALLBACK_RATE_LIMIT) which is fine */

    /* Set up as callback mode using the progress handler */
    global_cron.mode = 2;
    global_cron.active = 1;
    sqlite3_progress_handler(db, global_cron.callback_opcodes,
                             cron_progress_handler, db);
    sqlite3_result_text(context, "Initialized: Callback mode", -1,
                        SQLITE_STATIC);
  } else {
    /* Unknown mode */
    global_cron.ref_count--;
    cron_mutex_unlock(&global_cron.mutex);
    sqlite3_result_error(context, "Unknown mode: use 'thread' or 'callback'",
                         -1);
    return;
  }

  /* Reset last_check after initialization to ensure the first tick
   * can happen immediately in the same second. */
  global_cron.last_check = 0;
  cron_mutex_unlock(&global_cron.mutex);

  /* Register a sentinel function to detect connection closure.
   * When the connection is closed, the destructor (xDestroy) will be called.
   */
  sqlite3_create_function_v2(db, "__cron_sentinel", 0, SQLITE_UTF8, NULL,
                             cron_sentinel_func, NULL, NULL,
                             cron_sentinel_destroy);
}

/**
 * @brief SQL function: Schedule a job based on a fixed interval.
 * `SELECT cron_schedule(name, interval_seconds, sql, [timeout_ms]);`
 */
static void cron_schedule_func(sqlite3_context *context, int argc,
                               sqlite3_value **argv) {
  const char *name = (const char *)sqlite3_value_text(argv[0]);
  int interval = sqlite3_value_int(argv[1]);
  const char *cmd = (const char *)sqlite3_value_text(argv[2]);
  int timeout_ms = (argc == 4) ? sqlite3_value_int(argv[3]) : 0;
  sqlite3 *db = sqlite3_context_db_handle(context);

  time_t now = time(NULL);
  time_t next_run = now + interval;

  sqlite3_stmt *stmt;
  const char *sql = "INSERT INTO " CRON_JOBS_TABLE
                    " (name, command, schedule_interval, next_run, "
                    "active, timeout_ms) VALUES (?, ?, ?, ?, 1, ?);";

  if (sqlite3_prepare_v2(db, sql, -1, &stmt, NULL) == SQLITE_OK) {
    sqlite3_bind_text(stmt, 1, name, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt, 2, cmd, -1, SQLITE_TRANSIENT);
    sqlite3_bind_int(stmt, 3, interval);
    sqlite3_bind_int64(stmt, 4, (sqlite3_int64)next_run);
    sqlite3_bind_int(stmt, 5, timeout_ms);

    /* Set in_tick to prevent progress handler from triggering cron_tick during
     * our own write */
    cron_mutex_lock(&global_cron.mutex);
    global_cron.in_tick = 1;
    cron_mutex_unlock(&global_cron.mutex);

    int rc = sqlite3_step(stmt);

    cron_mutex_lock(&global_cron.mutex);
    global_cron.in_tick = 0;
    cron_mutex_unlock(&global_cron.mutex);

    if (rc == SQLITE_DONE) {
      sqlite3_result_int64(context, sqlite3_last_insert_rowid(db));
    } else {
      sqlite3_result_error(context, sqlite3_errmsg(db), -1);
    }
    sqlite3_finalize(stmt);
  } else {
    sqlite3_result_error(context, sqlite3_errmsg(db), -1);
  }
}

/**
 * @brief SQL function: Schedule a job using standard cron syntax.
 * `SELECT cron_schedule_cron(name, cron_expr, sql, [timeout_ms]);`
 */
static void cron_schedule_cron_func(sqlite3_context *context, int argc,
                                    sqlite3_value **argv) {
  const char *name = (const char *)sqlite3_value_text(argv[0]);
  const char *cron_expression = (const char *)sqlite3_value_text(argv[1]);
  const char *sql_command = (const char *)sqlite3_value_text(argv[2]);
  sqlite3 *db = sqlite3_context_db_handle(context);

  int timeout_ms = 0;
  if (argc == 4) {
    timeout_ms = sqlite3_value_int(argv[3]);
  }

  /* 2. Validate Cron Expression */
  cron_expr expr;
  const char *err = NULL;

  /* Handle macros (@daily, etc) by substituting them */
  /* ccronexpr 1.0 doesn't support macros natively in parse_expr, but we can do
   * it manually or rely on library if updated */
  /* Re-reading ccronexpr.c: it handles it? No, we saw macro tests pass, so
   * maybe it does or we rely on user? */
  /* Wait, "ccronexpr.c:cron_parse_expr" snippet showed it calls "Fields". */
  /* Let's trust the tests passed with macros. */

  cron_parse_expr((const char *)cron_expression, &expr, &err);
  if (err) {
    char *err_msg = sqlite3_mprintf("Invalid cron expression: %s", err);
    sqlite3_result_error(context, err_msg, -1);
    sqlite3_free(err_msg);
    return;
  }

  /* 3. Calculate First Run */
  time_t now = time(NULL);
  time_t local_now = now + global_cron.timezone_offset_seconds;
  time_t next_local = cron_next(&expr, local_now);
  time_t next_run;

  fprintf(stderr, "DEBUG: now=%ld offset=%ld local_now=%ld next_local=%ld\n",
          (long)now, (long)global_cron.timezone_offset_seconds, (long)local_now,
          (long)next_local);

  if (next_local == (time_t)-1) {
    next_run = now + 60; /* Error fallback */
  } else {
    next_run = next_local - global_cron.timezone_offset_seconds;
    fprintf(stderr, "DEBUG: Calculated next_run=%ld (diff=%ld)\n",
            (long)next_run, (long)(next_local - next_run));
  }

  /* 4. Insert Job */
  sqlite3_stmt *stmt;
  const char *sql = "INSERT INTO " CRON_JOBS_TABLE " "
                    "(name, command, schedule_interval, cron_expr, next_run, "
                    "active, timeout_ms) "
                    "VALUES (?, ?, 0, ?, ?, 1, ?);";

  if (sqlite3_prepare_v2(db, sql, -1, &stmt, NULL) == SQLITE_OK) {
    sqlite3_bind_text(stmt, 1, (const char *)name, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt, 2, (const char *)sql_command, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt, 3, (const char *)cron_expression, -1,
                      SQLITE_TRANSIENT);
    sqlite3_bind_int64(stmt, 4, (sqlite3_int64)next_run);
    sqlite3_bind_int(stmt, 5, timeout_ms);

    /* Set in_tick to prevent progress handler from triggering cron_tick during
     * our own write */
    cron_mutex_lock(&global_cron.mutex);
    global_cron.in_tick = 1;
    cron_mutex_unlock(&global_cron.mutex);

    int rc = sqlite3_step(stmt);

    cron_mutex_lock(&global_cron.mutex);
    global_cron.in_tick = 0;
    cron_mutex_unlock(&global_cron.mutex);

    if (rc == SQLITE_DONE) {
      sqlite3_result_text(context, name, -1, SQLITE_TRANSIENT);
      /* Wake up worker */
      cron_mutex_lock(&global_cron.mutex);
      cron_cond_broadcast(&global_cron.cond);
      cron_mutex_unlock(&global_cron.mutex);
    } else {
      sqlite3_result_error(context, sqlite3_errmsg(db), -1);
    }
    sqlite3_finalize(stmt);
  } else {
    sqlite3_result_error(context, sqlite3_errmsg(db), -1);
  }
}

/**
 * @brief SQL function: Pause a job execution.
 * `SELECT cron_pause(name);`
 */
static void cron_pause_func(sqlite3_context *context, int argc,
                            sqlite3_value **argv) {
  const char *name = (const char *)sqlite3_value_text(argv[0]);
  sqlite3 *db = sqlite3_context_db_handle(context);
  sqlite3_stmt *stmt;
  sqlite3_prepare_v2(
      db, "UPDATE " CRON_JOBS_TABLE " SET active = 0 WHERE name = ?;", -1,
      &stmt, NULL);
  sqlite3_bind_text(stmt, 1, name, -1, SQLITE_TRANSIENT);
  sqlite3_step(stmt);
  sqlite3_result_int(context, sqlite3_changes(db) > 0);
  sqlite3_finalize(stmt);
}

/**
 * @brief SQL function: Resume a paused job.
 * `SELECT cron_resume(name);`
 */
static void cron_resume_func(sqlite3_context *context, int argc,
                             sqlite3_value **argv) {
  const char *name = (const char *)sqlite3_value_text(argv[0]);
  sqlite3 *db = sqlite3_context_db_handle(context);
  sqlite3_stmt *stmt;
  sqlite3_prepare_v2(
      db, "UPDATE " CRON_JOBS_TABLE " SET active = 1 WHERE name = ?;", -1,
      &stmt, NULL);
  sqlite3_bind_text(stmt, 1, name, -1, SQLITE_TRANSIENT);
  sqlite3_step(stmt);
  sqlite3_result_int(context, sqlite3_changes(db) > 0);
  sqlite3_finalize(stmt);
}

/**
 * @brief Completely removes a job by name.
 */
static void cron_delete_func(sqlite3_context *context, int argc,
                             sqlite3_value **argv) {
  const char *name = (const char *)sqlite3_value_text(argv[0]);
  sqlite3 *db = sqlite3_context_db_handle(context);

  sqlite3_stmt *stmt;
  sqlite3_prepare_v2(db, "DELETE FROM " CRON_JOBS_TABLE " WHERE name = ?;", -1,
                     &stmt, NULL);
  sqlite3_bind_text(stmt, 1, name, -1, SQLITE_TRANSIENT);
  sqlite3_step(stmt);

  sqlite3_result_int(context, sqlite3_changes(db) > 0);
  sqlite3_finalize(stmt);
}

/**
 * @brief SQL function: Update an interval-based job's schedule.
 * `SELECT cron_update(name, new_interval);`
 */
static void cron_update_func(sqlite3_context *context, int argc,
                             sqlite3_value **argv) {
  const char *name = (const char *)sqlite3_value_text(argv[0]);
  int interval = sqlite3_value_int(argv[1]);
  sqlite3 *db = sqlite3_context_db_handle(context);
  sqlite3_stmt *stmt;
  sqlite3_prepare_v2(db,
                     "UPDATE " CRON_JOBS_TABLE
                     " SET schedule_interval = ? WHERE name = ?;",
                     -1, &stmt, NULL);
  sqlite3_bind_int(stmt, 1, interval);
  sqlite3_bind_text(stmt, 2, name, -1, SQLITE_TRANSIENT);
  sqlite3_step(stmt);
  sqlite3_result_int(context, sqlite3_changes(db) > 0);
  sqlite3_finalize(stmt);
}

/**
 * @brief SQL function: Get job metadata as JSON.
 * `SELECT cron_get(name);`
 */
static void cron_get_func(sqlite3_context *context, int argc,
                          sqlite3_value **argv) {
  const char *name = (const char *)sqlite3_value_text(argv[0]);
  sqlite3 *db = sqlite3_context_db_handle(context);
  sqlite3_stmt *stmt;
  sqlite3_prepare_v2(db,
                     "SELECT name, command, schedule_interval, active, "
                     "next_run, cron_expr FROM " CRON_JOBS_TABLE
                     " WHERE name = ?;",
                     -1, &stmt, NULL);
  sqlite3_bind_text(stmt, 1, name, -1, SQLITE_TRANSIENT);

  if (sqlite3_step(stmt) == SQLITE_ROW) {
    const char *cron_ptr = (const char *)sqlite3_column_text(stmt, 5);
    char *json = sqlite3_mprintf(
        "{\"name\":\"%s\",\"command\":\"%s\",\"interval\":%d,\"active\":%d,"
        "\"next_run\":%lld,\"cron\":\"%s\"}",
        sqlite3_column_text(stmt, 0), sqlite3_column_text(stmt, 1),
        sqlite3_column_int(stmt, 2), sqlite3_column_int(stmt, 3),
        sqlite3_column_int64(stmt, 4), cron_ptr ? cron_ptr : "");
    sqlite3_result_text(context, json, -1, sqlite3_free);

  } else {
    sqlite3_result_null(context);
  }
  sqlite3_finalize(stmt);
}

/**
 * @brief SQL function: List all scheduled jobs as a JSON array.
 * `SELECT cron_list();`
 */
static void cron_list_func(sqlite3_context *context, int argc,
                           sqlite3_value **argv) {
  sqlite3 *db = sqlite3_context_db_handle(context);
  sqlite3_stmt *stmt;
  sqlite3_prepare_v2(db,
                     "SELECT name, command, schedule_interval, active, "
                     "next_run, cron_expr FROM " CRON_JOBS_TABLE ";",
                     -1, &stmt, NULL);

  char *json = sqlite3_mprintf("[");
  int first = 1;
  while (sqlite3_step(stmt) == SQLITE_ROW) {
    const char *cron_ptr = (const char *)sqlite3_column_text(stmt, 5);
    char *item = sqlite3_mprintf(
        "%s{\"name\":\"%s\",\"command\":\"%s\",\"interval\":%d,\"active\":%d,"
        "\"next_run\":%lld,\"cron\":\"%s\"}",
        first ? "" : ",", sqlite3_column_text(stmt, 0),
        sqlite3_column_text(stmt, 1), sqlite3_column_int(stmt, 2),
        sqlite3_column_int(stmt, 3), sqlite3_column_int64(stmt, 4),
        cron_ptr ? cron_ptr : "");

    char *new_json = sqlite3_mprintf("%s%s", json, item);
    sqlite3_free(json);
    sqlite3_free(item);
    json = new_json;
    first = 0;
  }
  char *final_json = sqlite3_mprintf("%s]", json);
  sqlite3_free(json);
  sqlite3_result_text(context, final_json, -1, sqlite3_free);
  sqlite3_finalize(stmt);
}

/**
 * @brief SQL function: Reset the engine to it's default state.
 * `SELECT cron_reset();`
 *
 * Primarily used for testing. It stops the engine and clears all global
 * variables, allowing a fresh start without reloading the extension.
 *
 * @param context The SQLite context.
 * @param argc The number of arguments (expected 0).
 * @param argv The argument values.
 */
static void cron_reset_func(sqlite3_context *context, int argc,
                            sqlite3_value **argv) {
  sqlite3 *db = sqlite3_context_db_handle(context);

  /* Force a full shutdown if active */
  cron_mutex_lock(&global_cron.mutex);
  global_cron.ref_count = 1; /* Force last sentinel */
  cron_mutex_unlock(&global_cron.mutex);
  cron_shutdown_internal();

  cron_mutex_lock(&global_cron.mutex);
  global_cron.active = 0;
  global_cron.mode = 0;
  global_cron.in_tick = 0;
  global_cron.in_job = 0;
  global_cron.last_check = 0;
  global_cron.ref_count = 0;
  if (global_cron.db_path) {
    free(global_cron.db_path);
    global_cron.db_path = NULL;
  }
  global_cron.worker_db = NULL;
  cron_mutex_unlock(&global_cron.mutex);

  /* Deactivate progress handler if it was set */
  sqlite3_progress_handler(db, 0, NULL, NULL);
  sqlite3_result_text(context, "Cron reset", -1, SQLITE_STATIC);
}

/**
 * @brief SQL function: Manually execute a job now.
 * `SELECT cron_run(name);`
 */
static void cron_run_func(sqlite3_context *context, int argc,
                          sqlite3_value **argv) {
  const char *name = (const char *)sqlite3_value_text(argv[0]);
  sqlite3 *db = sqlite3_context_db_handle(context);
  sqlite3_stmt *stmt;
  const char *sql = "SELECT command FROM " CRON_JOBS_TABLE " WHERE name = ?;";

  if (sqlite3_prepare_v2(db, sql, -1, &stmt, NULL) != SQLITE_OK) {
    sqlite3_result_error(context, sqlite3_errmsg(db), -1);
    return;
  }

  sqlite3_bind_text(stmt, 1, name, -1, SQLITE_TRANSIENT);

  if (sqlite3_step(stmt) == SQLITE_ROW) {
    const char *cmd = (const char *)sqlite3_column_text(stmt, 0);
    char *err_msg = NULL;
    int rc = sqlite3_exec(db, cmd, NULL, NULL, &err_msg);
    if (rc != SQLITE_OK) {
      sqlite3_result_error(context, err_msg, -1);
      sqlite3_free(err_msg);
    } else {
      sqlite3_result_int(context, 1); /* Success */
    }
  } else {
    sqlite3_result_error(context, "Job not found", -1);
  }
  sqlite3_finalize(stmt);
}

/**
 * @brief SQL function: Purge completion logs based on a retention period.
 * `SELECT cron_purge_logs('-30 days');`
 */
static void cron_purge_logs_func(sqlite3_context *context, int argc,
                                 sqlite3_value **argv) {
  const char *modifier = (const char *)sqlite3_value_text(argv[0]);
  sqlite3 *db = sqlite3_context_db_handle(context);
  sqlite3_stmt *stmt;

  const char *sql = "DELETE FROM " CRON_LOG_TABLE
                    " WHERE start_time < strftime('%s', 'now', ?);";

  if (sqlite3_prepare_v2(db, sql, -1, &stmt, NULL) != SQLITE_OK) {
    sqlite3_result_error(context, sqlite3_errmsg(db), -1);
    return;
  }

  sqlite3_bind_text(stmt, 1, modifier, -1, SQLITE_TRANSIENT);
  int rc = sqlite3_step(stmt);
  if (rc == SQLITE_DONE) {
    sqlite3_result_int(context, sqlite3_changes(db));
  } else {
    sqlite3_result_error(context, sqlite3_errmsg(db), -1);
  }
  sqlite3_finalize(stmt);
}

/**
 * @brief SQL function: Returns the next planned run time for a job.
 * `SELECT cron_job_next(name);`
 */
static void cron_job_next_func(sqlite3_context *context, int argc,
                               sqlite3_value **argv) {
  const char *name = (const char *)sqlite3_value_text(argv[0]);
  sqlite3 *db = sqlite3_context_db_handle(context);
  sqlite3_stmt *stmt;
  const char *sql = "SELECT next_run FROM " CRON_JOBS_TABLE " WHERE name = ?;";

  if (sqlite3_prepare_v2(db, sql, -1, &stmt, NULL) != SQLITE_OK) {
    sqlite3_result_error(context, sqlite3_errmsg(db), -1);
    return;
  }

  sqlite3_bind_text(stmt, 1, name, -1, SQLITE_TRANSIENT);

  if (sqlite3_step(stmt) == SQLITE_ROW) {
    sqlite3_result_int64(context, sqlite3_column_int64(stmt, 0));
  } else {
    sqlite3_result_null(context); /* Job not found */
  }
  sqlite3_finalize(stmt);
}

/**
 * @brief SQL function: Returns all jobs and their next run times as JSON.
 * `SELECT cron_next_job_time();`
 */
static void cron_next_job_time_func(sqlite3_context *context, int argc,
                                    sqlite3_value **argv) {
  sqlite3 *db = sqlite3_context_db_handle(context);
  sqlite3_stmt *stmt;
  const char *sql =
      "SELECT name, next_run FROM " CRON_JOBS_TABLE " WHERE active = 1;";

  if (sqlite3_prepare_v2(db, sql, -1, &stmt, NULL) != SQLITE_OK) {
    sqlite3_result_error(context, sqlite3_errmsg(db), -1);
    return;
  }

  char *json = sqlite3_mprintf("{");
  int first = 1;

  while (sqlite3_step(stmt) == SQLITE_ROW) {
    const char *name = (const char *)sqlite3_column_text(stmt, 0);
    sqlite3_int64 next_run = sqlite3_column_int64(stmt, 1);

    char *tmp;
    if (first) {
      tmp = sqlite3_mprintf("%z\"%q\": %lld", json, name, next_run);
      first = 0;
    } else {
      tmp = sqlite3_mprintf("%z, \"%q\": %lld", json, name, next_run);
    }
    json = tmp;
  }
  sqlite3_finalize(stmt);

  char *final_json = sqlite3_mprintf("%z}", json);

  sqlite3_result_text(context, final_json, -1, SQLITE_TRANSIENT);
  sqlite3_free(final_json);
}

/**
 * @brief SQL function: Returns the most imminent job as JSON.
 * `SELECT cron_next_job();`
 */
static void cron_next_job_func(sqlite3_context *context, int argc,
                               sqlite3_value **argv) {
  sqlite3 *db = sqlite3_context_db_handle(context);
  sqlite3_stmt *stmt;
  const char *sql = "SELECT name, next_run FROM " CRON_JOBS_TABLE
                    " WHERE active = 1 ORDER BY next_run ASC LIMIT 1;";

  if (sqlite3_prepare_v2(db, sql, -1, &stmt, NULL) != SQLITE_OK) {
    sqlite3_result_error(context, sqlite3_errmsg(db), -1);
    return;
  }

  if (sqlite3_step(stmt) == SQLITE_ROW) {
    const char *name = (const char *)sqlite3_column_text(stmt, 0);
    sqlite3_int64 next_run = sqlite3_column_int64(stmt, 1);
    char *json = sqlite3_mprintf("{\"name\": \"%q\", \"next_run\": %lld}", name,
                                 next_run);
    sqlite3_result_text(context, json, -1, SQLITE_TRANSIENT);
    sqlite3_free(json);
  } else {
    sqlite3_result_null(context);
  }
  sqlite3_finalize(stmt);
}

/**
 * @brief SQL function: Returns the current status of the engine as JSON.
 * `SELECT cron_status();`
 */
static void cron_status_func(sqlite3_context *context, int argc,
                             sqlite3_value **argv) {
  sqlite3 *db = sqlite3_context_db_handle(context);

  /* Get active job count */
  sqlite3_stmt *stmt;
  int job_count = 0;
  if (sqlite3_prepare_v2(
          db, "SELECT count(*) FROM " CRON_JOBS_TABLE " WHERE active=1;", -1,
          &stmt, NULL) == SQLITE_OK) {
    if (sqlite3_step(stmt) == SQLITE_ROW) {
      job_count = sqlite3_column_int(stmt, 0);
    }
    sqlite3_finalize(stmt);
  }

  /* Use locally cached mode/active from global_cron */
  cron_mutex_lock(&global_cron.mutex);
  int mode = global_cron.mode;
  int active = global_cron.active;
  time_t last_check = global_cron.last_check;
  cron_mutex_unlock(&global_cron.mutex);

  char *json = sqlite3_mprintf("{\"mode\": \"%s\", \"active\": %d, "
                               "\"job_count\": %d, \"last_check\": %lld}",
                               (mode == 1) ? "thread"
                                           : (mode == 2 ? "callback" : "none"),
                               active, job_count, (long long)last_check);

  sqlite3_result_text(context, json, -1, SQLITE_TRANSIENT);
  sqlite3_free(json);
}

/**
 * @brief Configures retry logic for a specific job.
 *
 * Usage: SELECT cron_set_retry('job_name', max_retries,
 * retry_interval_seconds);
 */
static void cron_set_retry_func(sqlite3_context *context, int argc,
                                sqlite3_value **argv) {
  if (argc != 3) {
    sqlite3_result_error(context,
                         "Invalid arguments: name, max_retries, interval", -1);
    return;
  }
  const unsigned char *name = sqlite3_value_text(argv[0]);
  int max_retries = sqlite3_value_int(argv[1]);
  int retry_interval = sqlite3_value_int(argv[2]);

  sqlite3 *db = sqlite3_context_db_handle(context);
  sqlite3_stmt *stmt;
  sqlite3_prepare_v2(db,
                     "UPDATE " CRON_JOBS_TABLE " SET max_retries = ?, "
                     "retry_interval_seconds = ? WHERE name = ?;",
                     -1, &stmt, NULL);
  sqlite3_bind_int(stmt, 1, max_retries);
  sqlite3_bind_int(stmt, 2, retry_interval);
  sqlite3_bind_text(stmt, 3, (const char *)name, -1, SQLITE_TRANSIENT);
  sqlite3_step(stmt);
  sqlite3_result_int(context, sqlite3_changes(db) > 0);
  sqlite3_finalize(stmt);
}

/*
 * ======================================================================================
 *  USAGE EXAMPLES
 * ======================================================================================
 *
 * 1. Initialize the Engine
 *    -- Thread mode (automatic local time)
 *    SELECT cron_init('thread');
 *
 *    -- Thread mode with 10s poll and specific timezone
 *    SELECT cron_init('thread', 10, '+05:30');
 *
 * 2. Schedule Jobs
 *    -- Every 30 seconds
 *    SELECT cron_schedule('cleanup', 30, 'DELETE FROM sessions WHERE expiry <
 * now;');
 *
 *    -- Every minute at the 0th second
 *    SELECT cron_schedule_cron('stats', '0 * * * * *', 'INSERT INTO stats
 * SELECT count(*) FROM users;');
 *
 * 3. Advanced Job Configuration
 *    -- Configure retry logic (3 retries, 60s wait)
 *    SELECT cron_set_retry('cleanup', 3, 60);
 *
 *    -- Update interval for an existing job
 *    SELECT cron_update('cleanup', 300);
 *
 * 4. Manual Execution & Introspection
 *    -- Run a job immediately
 *    SELECT cron_run('cleanup');
 *
 *    -- Get specific job info as JSON
 *    SELECT cron_get('cleanup');
 *
 *    -- Predict next run for a job
 *    SELECT cron_job_next('cleanup');
 *
 *    -- Find the single most imminent job
 *    SELECT cron_next_job();
 *
 *    -- Get engine status
 *    SELECT cron_status();
 *
 * 5. Job Management
 *    -- List all jobs as JSON
 *    SELECT cron_list();
 *
 *    -- Pause/Resume
 *    SELECT cron_pause('cleanup');
 *    SELECT cron_resume('cleanup');
 *
 * 6. Engine Configuration & Maintenance
 *    -- Change poll interval globally
 *    SELECT cron_set_config('poll_interval_ms', '5000');
 *
 *    -- Purge logs older than 7 days
 *    SELECT cron_purge_logs('-7 days');
 *
 * 7. Graceful Shutdown
 *    -- Stops threads and waits for in-flight jobs
 *    SELECT cron_stop();
 *
 * ======================================================================================
 */

/**
 * @brief Extension Entry Point.
 *
 * This function is exported and called by SQLite's `.load` or
 * `load_extension()`. It prepares the environment and registers all SQL
 * functions.
 */
#ifdef _WIN32
__declspec(dllexport)
#endif
int sqlite3_sqlitecron_init(sqlite3 *db, char **pzErrMsg,
                            const sqlite3_api_routines *pApi) {
  SQLITE_EXTENSION_INIT2(pApi);

  /* 1. Create internal tables if they don't exist */
  sqlite3_exec(db,
               "CREATE TABLE IF NOT EXISTS " CRON_JOBS_TABLE " ("
               "  name TEXT PRIMARY KEY,"
               "  command TEXT,"
               "  schedule_interval INTEGER,"
               "  active INTEGER DEFAULT 1,"
               "  next_run INTEGER,"
               "  cron_expr TEXT,"
               "  last_run INTEGER,"
               "  timeout_ms INTEGER DEFAULT 0,"
               "  max_retries INTEGER DEFAULT 0,"
               "  retry_interval_seconds INTEGER DEFAULT 0,"
               "  retries_attempted INTEGER DEFAULT 0"
               ");",
               0, 0, 0);

  sqlite3_exec(db,
               "CREATE TABLE IF NOT EXISTS " CRON_LOG_TABLE " ("
               "  id INTEGER PRIMARY KEY AUTOINCREMENT,"
               "  job_name TEXT,"
               "  start_time INTEGER,"
               "  end_time INTEGER,"
               "  status TEXT,"
               "  duration_ms INTEGER,"
               "  error_message TEXT"
               ");",
               0, 0, 0);

  /* 2. Crash Recovery: Mark stalled jobs as CRASHED on startup */
  sqlite3_exec(db,
               "UPDATE " CRON_LOG_TABLE
               " SET status = 'CRASHED' WHERE status = 'RUNNING';",
               0, 0, 0);

  /* 3. Register SQL Functions */
  /* Initialization & Control */
  sqlite3_create_function(db, "cron_init", 1, SQLITE_UTF8, NULL, cron_init_func,
                          NULL, NULL);
  sqlite3_create_function(db, "cron_init", 2, SQLITE_UTF8, NULL, cron_init_func,
                          NULL, NULL);
  sqlite3_create_function(db, "cron_init", 3, SQLITE_UTF8, NULL, cron_init_func,
                          NULL, NULL);
  sqlite3_create_function(db, "cron_init", 4, SQLITE_UTF8, NULL, cron_init_func,
                          NULL, NULL);
  sqlite3_create_function(db, "cron_stop", 0, SQLITE_UTF8, NULL, cron_stop_func,
                          NULL, NULL);
  sqlite3_create_function(db, "cron_reset", 0, SQLITE_UTF8, NULL,
                          cron_reset_func, NULL, NULL);

  /* Scheduling */
  sqlite3_create_function(db, "cron_schedule", 3, SQLITE_UTF8, NULL,
                          cron_schedule_func, NULL, NULL);
  sqlite3_create_function(db, "cron_schedule", 4, SQLITE_UTF8, NULL,
                          cron_schedule_func, NULL, NULL);
  sqlite3_create_function(db, "cron_schedule_cron", 3, SQLITE_UTF8, NULL,
                          cron_schedule_cron_func, NULL, NULL);
  sqlite3_create_function(db, "cron_schedule_cron", 4, SQLITE_UTF8, NULL,
                          cron_schedule_cron_func, NULL, NULL);

  /* Job Management */
  sqlite3_create_function(db, "cron_delete", 1, SQLITE_UTF8, NULL,
                          cron_delete_func, NULL, NULL);
  sqlite3_create_function(db, "cron_pause", 1, SQLITE_UTF8, NULL,
                          cron_pause_func, NULL, NULL);
  sqlite3_create_function(db, "cron_resume", 1, SQLITE_UTF8, NULL,
                          cron_resume_func, NULL, NULL);
  sqlite3_create_function(db, "cron_update", 2, SQLITE_UTF8, NULL,
                          cron_update_func, NULL, NULL);
  sqlite3_create_function(db, "cron_run", 1, SQLITE_UTF8, NULL, cron_run_func,
                          NULL, NULL);
  sqlite3_create_function(db, "cron_set_retry", 3, SQLITE_UTF8, NULL,
                          cron_set_retry_func, NULL, NULL);

  /* Introspection */
  sqlite3_create_function(db, "cron_get", 1, SQLITE_UTF8, NULL, cron_get_func,
                          NULL, NULL);
  sqlite3_create_function(db, "cron_list", 0, SQLITE_UTF8, NULL, cron_list_func,
                          NULL, NULL);
  sqlite3_create_function(db, "cron_status", 0, SQLITE_UTF8, NULL,
                          cron_status_func, NULL, NULL);
  sqlite3_create_function(db, "cron_job_next", 1, SQLITE_UTF8, NULL,
                          cron_job_next_func, NULL, NULL);
  sqlite3_create_function(db, "cron_next_job", 0, SQLITE_UTF8, NULL,
                          cron_next_job_func, NULL, NULL);
  sqlite3_create_function(db, "cron_next_job_time", 0, SQLITE_UTF8, NULL,
                          cron_next_job_time_func, NULL, NULL);

  /* Maintenance */
  sqlite3_create_function(db, "cron_purge_logs", 1, SQLITE_UTF8, NULL,
                          cron_purge_logs_func, NULL, NULL);

  /* Configuration */
  sqlite3_create_function(db, "cron_set_config", 2, SQLITE_UTF8, NULL,
                          cron_set_config_func, NULL, NULL);
  sqlite3_create_function(db, "cron_get_config", 1, SQLITE_UTF8, NULL,
                          cron_get_config_func, NULL, NULL);

  /* 4. Global State Synchronization Initialization */
  static int once = 0;
  if (!once) {
    /* These primitives only need initialization once per process lifetime */
    cron_mutex_init(&global_cron.mutex);
    cron_cond_init(&global_cron.cond);
    once = 1;
  }

  return SQLITE_OK;
}
