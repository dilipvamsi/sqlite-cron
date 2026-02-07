/**
 * @file sqlite_cron.c
 * @brief A SQLite extension for scheduling background SQL jobs, inspired by
 * pg_cron.
 *
 * sqlite-cron is a pure C SQLite loadable extension that provides background
 * job scheduling without external dependencies. Jobs are stored in SQLite
 * tables and executed automatically based on their configured intervals.
 *
 * ## Features
 * - Two execution modes (thread-based and callback-based)
 * - Configurable poll interval for thread mode
 * - Job pause/resume, update, and deletion
 * - Execution logging with duration and error tracking
 * - Graceful shutdown with in-flight job cancellation
 * - Crash recovery for interrupted jobs
 *
 * ## Execution Modes
 *
 * ### Thread Mode (Recommended for disk-based databases)
 * Spawns a background OS thread that polls the database for due jobs.
 * Requires WAL mode for safe concurrent access.
 * ```sql
 * PRAGMA journal_mode=WAL;
 * SELECT cron_init('thread');      -- 16 second poll (default)
 * SELECT cron_init('thread', 4);   -- 4 second poll for sub-minute jobs
 * ```
 *
 * ### Callback Mode (For in-memory databases)
 * Uses SQLite's progress handler to check for due jobs during query execution.
 * No extra threads, but jobs only run when other queries are executing.
 * ```sql
 * SELECT cron_init('callback');
 * ```
 *
 * ## API Reference
 *
 * ### cron_init(mode, [poll_seconds])
 * Initialize the cron engine.
 * - mode: 'thread' or 'callback'
 * - poll_seconds: (optional, thread mode only) seconds between checks, default
 * 16
 *
 * ### cron_schedule(name, interval_seconds, sql_command, [timeout_ms])
 * Schedule a recurring job.
 * - name: unique job identifier
 * - interval_seconds: how often to run
 * - sql_command: SQL to execute
 * - timeout_ms: (optional) max execution time before interruption
 *
 * ### cron_get(name)
 * Returns JSON object describing the job, or NULL if not found.
 *
 * ### cron_list()
 * Returns JSON array of all scheduled jobs.
 *
 * ### cron_pause(name) / cron_resume(name)
 * Temporarily disable or re-enable a job.
 *
 * ### cron_update(name, new_interval_seconds)
 * Update the interval for an existing job.
 *
 * ### cron_delete(name)
 * Remove a job from the schedule.
 *
 * ### cron_stop()
 * Gracefully stop the cron engine.
 *
 * ### cron_reset()
 * Force-reset all global state (useful for testing).
 *
 * ## Internal Tables
 *
 * ### __cron_jobs
 * | Column            | Type    | Description                          |
 * |-------------------|---------|--------------------------------------|
 * | id                | INTEGER | Primary key                          |
 * | name              | TEXT    | Unique job identifier                |
 * | command           | TEXT    | SQL to execute                       |
 * | schedule_interval | INTEGER | Seconds between runs                 |
 * | active            | INTEGER | 1=enabled, 0=paused                  |
 * | next_run          | INTEGER | Unix timestamp of next execution     |
 * | timeout_ms        | INTEGER | Max execution time (0=no limit)      |
 *
 * ### __cron_log
 * | Column        | Type    | Description                          |
 * |---------------|---------|--------------------------------------|
 * | id            | INTEGER | Primary key                          |
 * | job_name      | TEXT    | Job that was executed                |
 * | start_time    | INTEGER | Unix timestamp when job started      |
 * | duration_ms   | INTEGER | How long the job took                |
 * | status        | TEXT    | SUCCESS, ERROR, TIMEOUT, or CRASHED  |
 * | error_message | TEXT    | Error details if status=ERROR        |
 *
 * ## Usage Example
 * ```sql
 * -- Load extension
 * .load ./build/sqlite_cron
 *
 * -- Initialize with thread mode and 8-second poll
 * SELECT cron_init('thread', 8);
 *
 * -- Schedule cleanup job every 5 minutes (300 seconds)
 * SELECT cron_schedule('cleanup_logs', 300,
 *     'DELETE FROM logs WHERE created_at < datetime("now", "-7 days")');
 *
 * -- Schedule with timeout (job killed after 10 seconds)
 * SELECT cron_schedule('slow_query', 60, 'SELECT expensive_func()', 10000);
 *
 * -- View all jobs
 * SELECT cron_list();
 *
 * -- Pause during maintenance
 * SELECT cron_pause('cleanup_logs');
 *
 * -- Resume
 * SELECT cron_resume('cleanup_logs');
 *
 * -- Check execution history
 * SELECT * FROM __cron_log ORDER BY id DESC LIMIT 10;
 *
 * -- Update interval to every 10 minutes
 * SELECT cron_update('cleanup_logs', 600);
 *
 * -- Delete job
 * SELECT cron_delete('cleanup_logs');
 *
 * -- Stop engine
 * SELECT cron_stop();
 * ```
 *
 * ## Thread Safety
 * - All global state is protected by a mutex
 * - Reference counting allows multiple connections to share one engine
 * - Graceful shutdown uses condition variables for fast thread wakeup
 * - sqlite3_interrupt() cancels in-flight queries during shutdown
 *
 * ## Cross-Platform Support
 * - Linux: gcc -shared -lpthread
 * - macOS: gcc -dynamiclib
 * - Windows: x86_64-w64-mingw32-gcc (uses CreateThread/WaitForSingleObject)
 */

#include "sqlite3ext.h"

SQLITE_EXTENSION_INIT1
#include <stdio.h>
#include <stdlib.h> /* For free() and malloc() */
#include <string.h>
#include <sys/time.h> /* For millisecond-precision timing on POSIX systems */

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
#endif

/* --- Internal Configuration Defaults --- */
#ifdef TEST_MODE
/* Aggressive settings for testing environments to reduce wait times */
#define CALLBACK_RATE_LIMIT 1 /* Seconds between checks in callback mode */
#define CALLBACK_OPCODES 10   /* VM instructions between checks */
#define DEFAULT_POLL_INTERVAL_MS 1000 /* 1 second for tests */
#else
#define CALLBACK_RATE_LIMIT 60  /* Default: check at most once per minute */
#define CALLBACK_OPCODES 100000 /* Default: check every 100k instructions */
#define DEFAULT_POLL_INTERVAL_MS 16000 /* 16 seconds default */
#endif

/**
 * @struct CronControl
 * @brief Global state container for the cron engine.
 */
typedef struct {
  char *db_path;        /**< Absolute path to the database for thread mode */
  int mode;             /**< 1 = Threaded, 2 = Callback-based */
  volatile int active;  /**< flag indicating if the engine is running */
  volatile int in_tick; /**< Reentrancy guard to prevent nested executions */
  time_t last_check;    /**< Timestamp of the last successful schedule check */
  struct timeval job_start_time; /**< Start time of the currently running job */
  int current_timeout_ms;        /**< Timeout for the currently running job */
  volatile int in_job; /**< Flag to indicate a job is currently executing */
  cron_thread_t thread_handle; /**< Handle for the background worker thread */
  sqlite3 *worker_db;          /**< Private DB handle for the worker thread */
  cron_mutex_t mutex;          /**< Mutex for thread-safe state access */
  cron_cond_t cond;            /**< Condition variable for fast exit/waking */
  int ref_count;        /**< Number of active connections using the engine */
  int poll_interval_ms; /**< Thread poll interval in milliseconds */
} CronControl;

/* Singleton instance of the cron controller */
static CronControl global_cron = {
    NULL, 0, 0,    0,   0,   {0, 0}, 0,
    0,    0, NULL, {0}, {0}, 0,      DEFAULT_POLL_INTERVAL_MS};

/* --- The Core Engine --- */

/**
 * @brief Main scheduling logic. Checks and executes due jobs.
 *
 * This function handles the "tick" of the cron engine. It uses an IMMEDIATE
 * transaction to safely interact with the `__cron_jobs` table and logs
 * execution results to `__cron_log`.
 *
 * @param db The SQLite database handle.
 */
static void cron_tick(sqlite3 *db) {
  cron_mutex_lock(&global_cron.mutex);
  if (global_cron.in_tick) {
    cron_mutex_unlock(&global_cron.mutex);
    return;
  }
  global_cron.in_tick = 1;
  cron_mutex_unlock(&global_cron.mutex);

  time_t now = time(NULL);

  /* Rate-limiting */
  if (now <= global_cron.last_check) {
    cron_mutex_lock(&global_cron.mutex);
    global_cron.in_tick = 0;
    cron_mutex_unlock(&global_cron.mutex);
    return;
  }

  global_cron.last_check = now;

  /* Find jobs that are due (next_run <= now) and are active */
  const char *query =
      "SELECT name, command, schedule_interval, timeout_ms FROM __cron_jobs "
      "WHERE next_run <= strftime('%s', 'now') AND active = 1;";

  sqlite3_stmt *stmt;
  if (sqlite3_prepare_v2(db, query, -1, &stmt, NULL) == SQLITE_OK) {
    while (sqlite3_step(stmt) == SQLITE_ROW) {
      const char *name_ptr = (const char *)sqlite3_column_text(stmt, 0);
      const char *cmd_ptr = (const char *)sqlite3_column_text(stmt, 1);
      int interval = sqlite3_column_int(stmt, 2);
      int timeout_ms = sqlite3_column_int(stmt, 3);

      char *name = STRDUP(name_ptr ? name_ptr : "");
      char *cmd = STRDUP(cmd_ptr ? cmd_ptr : "");

      /* Set up timeout tracking */
      global_cron.current_timeout_ms = timeout_ms;
      gettimeofday(&global_cron.job_start_time, NULL);
      global_cron.in_job = 1;
      time_t start_s = (time_t)global_cron.job_start_time.tv_sec;

      /* 1. Log RUNNING (Independent Transaction) */
      sqlite3_exec(db, "BEGIN IMMEDIATE;", NULL, NULL, NULL);
      sqlite3_stmt *log_stmt;
      const char *log_sql = "INSERT INTO __cron_log (job_name, start_time, "
                            "status) VALUES (?, ?, 'RUNNING');";
      sqlite3_int64 log_id = 0;
      if (sqlite3_prepare_v2(db, log_sql, -1, &log_stmt, NULL) == SQLITE_OK) {
        sqlite3_bind_text(log_stmt, 1, name, -1, SQLITE_TRANSIENT);
        sqlite3_bind_int64(log_stmt, 2, start_s);
        sqlite3_step(log_stmt);
        log_id = sqlite3_last_insert_rowid(db);
        sqlite3_finalize(log_stmt);
      }
      sqlite3_exec(db, "COMMIT;", NULL, NULL, NULL);

      /* 2. EXECUTE Job (Managed separately) */
      char *err_msg = NULL;
      int rc = sqlite3_exec(db, cmd, NULL, NULL, &err_msg);
      global_cron.in_job = 0;

      struct timeval t_end;
      gettimeofday(&t_end, NULL);
      long long duration_ms =
          (t_end.tv_sec - global_cron.job_start_time.tv_sec) * 1000LL +
          (t_end.tv_usec - global_cron.job_start_time.tv_usec) / 1000LL;

      /* 3. Log Outcome (Independent Transaction on a fresh connection if
       * interrupted) */
      if (log_id > 0) {
        sqlite3 *log_db = db;
        int opened_fresh = 0;

        /* If interrupted, the main connection is tainted. Use a fresh one. */
        if (rc == SQLITE_INTERRUPT && global_cron.db_path) {
          if (sqlite3_open(global_cron.db_path, &log_db) == SQLITE_OK) {
            sqlite3_exec(log_db, "PRAGMA journal_mode=WAL;", NULL, NULL, NULL);
            opened_fresh = 1;
          } else {
            log_db = db; /* Fallback */
          }
        }

        sqlite3_exec(log_db, "BEGIN IMMEDIATE;", NULL, NULL, NULL);
        const char *upd_log = "UPDATE __cron_log SET duration_ms = ?, status = "
                              "?, error_message = ? WHERE id = ?;";
        if (sqlite3_prepare_v2(log_db, upd_log, -1, &log_stmt, NULL) ==
            SQLITE_OK) {
          sqlite3_bind_int64(log_stmt, 1, duration_ms);
          const char *status = "SUCCESS";
          if (rc == SQLITE_INTERRUPT)
            status = "TIMEOUT";
          else if (rc != SQLITE_OK)
            status = "FAILURE";
#ifdef TEST_MODE
          printf("DEBUG: cron_tick job=%s rc=%d status=%s duration=%lld\n",
                 name, rc, status, duration_ms);
#endif
          sqlite3_bind_text(log_stmt, 2, status, -1, SQLITE_STATIC);
          sqlite3_bind_text(log_stmt, 3, err_msg, -1, SQLITE_TRANSIENT);
          sqlite3_bind_int64(log_stmt, 4, log_id);
          sqlite3_step(log_stmt);
          sqlite3_finalize(log_stmt);
        }
        sqlite3_exec(log_db, "COMMIT;", NULL, NULL, NULL);

        if (opened_fresh) {
          sqlite3_close(log_db);
        }
      }

      /* 4. Update Next Run (Independent Transaction) */
      sqlite3_exec(db, "BEGIN IMMEDIATE;", NULL, NULL, NULL);
      sqlite3_stmt *upd_stmt;
      const char *upd_sql = "UPDATE __cron_jobs SET next_run = "
                            "strftime('%s','now') + ? WHERE name = ?;";
      if (sqlite3_prepare_v2(db, upd_sql, -1, &upd_stmt, NULL) == SQLITE_OK) {
        sqlite3_bind_int(upd_stmt, 1, interval);
        sqlite3_bind_text(upd_stmt, 2, name, -1, SQLITE_TRANSIENT);
        sqlite3_step(upd_stmt);
        sqlite3_finalize(upd_stmt);
      }
      sqlite3_exec(db, "COMMIT;", NULL, NULL, NULL);

      if (err_msg)
        sqlite3_free(err_msg);
      free(name);
      free(cmd);
    }
    sqlite3_finalize(stmt);
  }

  sqlite3_exec(db, "PRAGMA wal_checkpoint(FULL);", NULL, NULL, NULL);

  cron_mutex_lock(&global_cron.mutex);
  global_cron.in_tick = 0;
  cron_mutex_unlock(&global_cron.mutex);
}

/* --- Mode 2: Callback Handler Implementation --- */

/**
 * @brief SQLite Progress Handler callback.
 *
 * High-performance hook that triggers `cron_tick` during normal query
 * processing.
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
    if (now - global_cron.last_check < CALLBACK_RATE_LIMIT)
      return 0;

    cron_tick((sqlite3 *)arg);
  }

  return 0;
}

/* --- Mode 1: Threaded Worker Implementation --- */

/**
 * @brief Background thread worker that executes due jobs.
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

  cron_mutex_lock(&global_cron.mutex);
  global_cron.worker_db = worker_db;
#ifdef TEST_MODE
  printf("DEBUG: cron_thread_worker started for %s\n", global_cron.db_path);
#endif
  cron_mutex_unlock(&global_cron.mutex); /* Release before loop */

  while (global_cron.active) {
    cron_tick(worker_db);

    cron_mutex_lock(&global_cron.mutex);
    if (!global_cron.active) {
#ifdef TEST_MODE
      printf("DEBUG: cron_thread_worker exiting due to active=0\n");
#endif
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

/* --- Public SQLite API Functions --- */

/**
 * @brief Internal helper to stop the cron engine gracefully.
 *
 * Shutdown sequence:
 * 1. Decrement reference count
 * 2. If other connections still using engine, return early
 * 3. Set active=0 to signal thread to exit
 * 4. Broadcast condition variable to wake sleeping thread immediately
 * 5. Call sqlite3_interrupt() to cancel any running SQL query
 * 6. Poll for in-flight jobs to complete (with timeout)
 * 7. Join the thread (wait for it to fully exit)
 * 8. Clean up allocated resources
 */
static void cron_shutdown_internal(void) {
  cron_mutex_lock(&global_cron.mutex);

  /* Step 1: Decrement reference count for this connection */
  if (global_cron.ref_count > 0) {
    global_cron.ref_count--;
  }

#ifdef TEST_MODE
  printf("DEBUG: cron_shutdown_internal ref_count=%d active=%d\n",
         global_cron.ref_count, global_cron.active);
#endif

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
 * @brief Stops the cron engine gracefully.
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
 * @brief Initializes the cron engine session-wide.
 *
 * Usage: SELECT cron_init('thread') OR SELECT cron_init('callback')
 *        SELECT cron_init('thread', 8) -- with 8 second poll interval
 */
static void cron_init_func(sqlite3_context *context, int argc,
                           sqlite3_value **argv) {
  const char *mode_str = (const char *)sqlite3_value_text(argv[0]);
  int poll_interval_ms = DEFAULT_POLL_INTERVAL_MS;

  /* Optional second argument: poll interval in seconds */
  if (argc >= 2 && sqlite3_value_type(argv[1]) == SQLITE_INTEGER) {
    poll_interval_ms = sqlite3_value_int(argv[1]) * 1000;
    if (poll_interval_ms < 1000)
      poll_interval_ms = 1000; /* Minimum 1 second */
  }
  sqlite3 *db = sqlite3_context_db_handle(context);

  cron_mutex_lock(&global_cron.mutex);
  global_cron.ref_count++;

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

  if (strcmp(mode_str, "thread") == 0) {
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
    global_cron.poll_interval_ms = poll_interval_ms;

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
  } else if (strcmp(mode_str, "callback") == 0) {
    /* Callback mode doesn't use poll interval */
    if (argc >= 2 && sqlite3_value_type(argv[1]) == SQLITE_INTEGER) {
      global_cron.ref_count--;
      cron_mutex_unlock(&global_cron.mutex);
      sqlite3_result_error(context,
                           "Poll interval not supported in callback mode. Use "
                           "cron_init('callback') only.",
                           -1);
      return;
    }
    /* Set up as callback mode using the progress handler */
    global_cron.mode = 2;
    global_cron.active = 1;
    sqlite3_progress_handler(db, CALLBACK_OPCODES, cron_progress_handler, db);
    sqlite3_result_text(context, "Initialized: Callback mode", -1,
                        SQLITE_STATIC);
  } else {
    /* Unknown mode - reject with error */
    global_cron.ref_count--;
    cron_mutex_unlock(&global_cron.mutex);
    sqlite3_result_error(context, "Unknown mode: use 'thread' or 'callback'",
                         -1);
    return;
  }

  cron_mutex_unlock(&global_cron.mutex);

  /* Register a sentinel function to detect connection closure.
   * When the connection is closed, the destructor (xDestroy) will be called. */
  sqlite3_create_function_v2(db, "__cron_sentinel", 0, SQLITE_UTF8, NULL,
                             cron_sentinel_func, NULL, NULL,
                             cron_sentinel_destroy);
}

/**
 * @brief Schedules a new cron job.
 *
 * Usage: SELECT cron_schedule('my_job', 60, 'UPDATE ...')
 */
static void cron_schedule_func(sqlite3_context *context, int argc,
                               sqlite3_value **argv) {
  const char *name = (const char *)sqlite3_value_text(argv[0]);
  int interval = sqlite3_value_int(argv[1]);
  const char *cmd = (const char *)sqlite3_value_text(argv[2]);
  int timeout_ms = (argc == 4) ? sqlite3_value_int(argv[3]) : 0;
  sqlite3 *db = sqlite3_context_db_handle(context);

  sqlite3_stmt *stmt;
  const char *sql =
      "INSERT INTO __cron_jobs (name, command, schedule_interval, next_run, "
      "active, timeout_ms) VALUES (?, ?, ?, strftime('%s', 'now'), 1, ?);";

  if (sqlite3_prepare_v2(db, sql, -1, &stmt, NULL) == SQLITE_OK) {
    sqlite3_bind_text(stmt, 1, name, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt, 2, cmd, -1, SQLITE_TRANSIENT);
    sqlite3_bind_int(stmt, 3, interval);
    sqlite3_bind_int(stmt, 4, timeout_ms);

    if (sqlite3_step(stmt) == SQLITE_DONE) {
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
 * @brief Pauses an existing job by name.
 */
static void cron_pause_func(sqlite3_context *context, int argc,
                            sqlite3_value **argv) {
  const char *name = (const char *)sqlite3_value_text(argv[0]);
  sqlite3 *db = sqlite3_context_db_handle(context);
  sqlite3_stmt *stmt;
  sqlite3_prepare_v2(db, "UPDATE __cron_jobs SET active = 0 WHERE name = ?;",
                     -1, &stmt, NULL);
  sqlite3_bind_text(stmt, 1, name, -1, SQLITE_TRANSIENT);
  sqlite3_step(stmt);
  sqlite3_result_int(context, sqlite3_changes(db) > 0);
  sqlite3_finalize(stmt);
}

/**
 * @brief Resumes a paused job by name.
 */
static void cron_resume_func(sqlite3_context *context, int argc,
                             sqlite3_value **argv) {
  const char *name = (const char *)sqlite3_value_text(argv[0]);
  sqlite3 *db = sqlite3_context_db_handle(context);
  sqlite3_stmt *stmt;
  sqlite3_prepare_v2(db, "UPDATE __cron_jobs SET active = 1 WHERE name = ?;",
                     -1, &stmt, NULL);
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
  sqlite3_prepare_v2(db, "DELETE FROM __cron_jobs WHERE name = ?;", -1, &stmt,
                     NULL);
  sqlite3_bind_text(stmt, 1, name, -1, SQLITE_TRANSIENT);
  sqlite3_step(stmt);

  sqlite3_result_int(context, sqlite3_changes(db) > 0);
  sqlite3_finalize(stmt);
}

/**
 * @brief Updates the execution interval for a job.
 */
static void cron_update_func(sqlite3_context *context, int argc,
                             sqlite3_value **argv) {
  const char *name = (const char *)sqlite3_value_text(argv[0]);
  int interval = sqlite3_value_int(argv[1]);
  sqlite3 *db = sqlite3_context_db_handle(context);
  sqlite3_stmt *stmt;
  sqlite3_prepare_v2(
      db, "UPDATE __cron_jobs SET schedule_interval = ? WHERE name = ?;", -1,
      &stmt, NULL);
  sqlite3_bind_int(stmt, 1, interval);
  sqlite3_bind_text(stmt, 2, name, -1, SQLITE_TRANSIENT);
  sqlite3_step(stmt);
  sqlite3_result_int(context, sqlite3_changes(db) > 0);
  sqlite3_finalize(stmt);
}

/**
 * @brief Gets the state of a specific job as a JSON object.
 */
static void cron_get_func(sqlite3_context *context, int argc,
                          sqlite3_value **argv) {
  const char *name = (const char *)sqlite3_value_text(argv[0]);
  sqlite3 *db = sqlite3_context_db_handle(context);
  sqlite3_stmt *stmt;
  sqlite3_prepare_v2(db,
                     "SELECT name, command, schedule_interval, active, "
                     "next_run FROM __cron_jobs WHERE name = ?;",
                     -1, &stmt, NULL);
  sqlite3_bind_text(stmt, 1, name, -1, SQLITE_TRANSIENT);

  if (sqlite3_step(stmt) == SQLITE_ROW) {
    char *json = sqlite3_mprintf(
        "{\"name\":\"%s\",\"command\":\"%s\",\"interval\":%d,\"active\":%d,"
        "\"next_run\":%lld}",
        sqlite3_column_text(stmt, 0), sqlite3_column_text(stmt, 1),
        sqlite3_column_int(stmt, 2), sqlite3_column_int(stmt, 3),
        sqlite3_column_int64(stmt, 4));
    sqlite3_result_text(context, json, -1, sqlite3_free);
  } else {
    sqlite3_result_null(context);
  }
  sqlite3_finalize(stmt);
}

/**
 * @brief Lists all scheduled jobs as a JSON array.
 */
static void cron_list_func(sqlite3_context *context, int argc,
                           sqlite3_value **argv) {
  sqlite3 *db = sqlite3_context_db_handle(context);
  sqlite3_stmt *stmt;
  sqlite3_prepare_v2(db,
                     "SELECT name, command, schedule_interval, active, "
                     "next_run FROM __cron_jobs;",
                     -1, &stmt, NULL);

  char *json = sqlite3_mprintf("[");
  int first = 1;
  while (sqlite3_step(stmt) == SQLITE_ROW) {
    char *item = sqlite3_mprintf(
        "%s{\"name\":\"%s\",\"command\":\"%s\",\"interval\":%d,\"active\":%d,"
        "\"next_run\":%lld}",
        first ? "" : ",", sqlite3_column_text(stmt, 0),
        sqlite3_column_text(stmt, 1), sqlite3_column_int(stmt, 2),
        sqlite3_column_int(stmt, 3), sqlite3_column_int64(stmt, 4));
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
 * @brief Resets the global extension state. (Mainly for test isolation).
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

/* Redundant synchronization helpers removed */

/* --- Extension Boilerplate & Entry Point --- */

/**
 * @brief Main registration entry point for the SQLite extension.
 *
 * This function is called automatically by SQLite when the extension is loaded.
 * It initializes the internal tables and registers all `cron_*` functions.
 */
#ifdef _WIN32
__declspec(dllexport)
#endif
int sqlite3_sqlitecron_init(sqlite3 *db, char **pzErrMsg,
                            const sqlite3_api_routines *pApi) {
  SQLITE_EXTENSION_INIT2(pApi);

  /* Initialize job definition table */
  sqlite3_exec(
      db,
      "CREATE TABLE IF NOT EXISTS __cron_jobs (id INTEGER PRIMARY KEY, name "
      "TEXT UNIQUE, command "
      "TEXT, schedule_interval INTEGER, next_run INTEGER, active INTEGER, "
      "timeout_ms INTEGER DEFAULT 0);",
      0, 0, 0);

  /* Initialize execution log table */
  sqlite3_exec(db,
               "CREATE TABLE IF NOT EXISTS __cron_log (id INTEGER PRIMARY KEY, "
               "job_name TEXT, start_time INTEGER, duration_ms INTEGER, status "
               "TEXT, error_message TEXT);",
               0, 0, 0);

  /* Crash Recovery: Clean up stale RUNNING logs */
  sqlite3_exec(
      db, "UPDATE __cron_log SET status = 'CRASHED' WHERE status = 'RUNNING';",
      0, 0, 0);

  /* Register all functions with SQLite */
  sqlite3_create_function(db, "cron_init", 1, SQLITE_UTF8, NULL, cron_init_func,
                          NULL, NULL);
  sqlite3_create_function(db, "cron_init", 2, SQLITE_UTF8, NULL, cron_init_func,
                          NULL, NULL);
  sqlite3_create_function(db, "cron_stop", 0, SQLITE_UTF8, NULL, cron_stop_func,
                          NULL, NULL);
  sqlite3_create_function(db, "cron_schedule", 3, SQLITE_UTF8, NULL,
                          cron_schedule_func, NULL, NULL);
  sqlite3_create_function(db, "cron_schedule", 4, SQLITE_UTF8, NULL,
                          cron_schedule_func, NULL, NULL);
  sqlite3_create_function(db, "cron_delete", 1, SQLITE_UTF8, NULL,
                          cron_delete_func, NULL, NULL);
  sqlite3_create_function(db, "cron_pause", 1, SQLITE_UTF8, NULL,
                          cron_pause_func, NULL, NULL);
  sqlite3_create_function(db, "cron_resume", 1, SQLITE_UTF8, NULL,
                          cron_resume_func, NULL, NULL);
  sqlite3_create_function(db, "cron_get", 1, SQLITE_UTF8, NULL, cron_get_func,
                          NULL, NULL);
  sqlite3_create_function(db, "cron_list", 0, SQLITE_UTF8, NULL, cron_list_func,
                          NULL, NULL);
  sqlite3_create_function(db, "cron_update", 2, SQLITE_UTF8, NULL,
                          cron_update_func, NULL, NULL);
  sqlite3_create_function(db, "cron_reset", 0, SQLITE_UTF8, NULL,
                          cron_reset_func, NULL, NULL);

  /* Initialize mutex and cond var once */
  static int once = 0;
  if (!once) {
    cron_mutex_init(&global_cron.mutex);
    cron_cond_init(&global_cron.cond);
    once = 1;
  }

  /* Crash Recovery was moved up */

  return SQLITE_OK;
}
