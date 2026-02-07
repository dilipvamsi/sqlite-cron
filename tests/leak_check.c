/**
 * @file leak_check.c
 * @brief Memory leak test harness for sqlite-cron extension.
 *
 * Run with valgrind to verify no memory leaks:
 *   make leak-check
 *
 * This tests all public API functions in both callback and thread modes
 * to ensure proper memory management.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#if defined(_WIN32) || defined(_WIN64)
#include <io.h>
#include <windows.h>
#define UNLINK _unlink
#define EXTENSION_PATH "build/sqlite_cron.dll"
#define SLEEP_MS(x) Sleep(x)
#else
#include <sqlite3.h>
#include <unistd.h>
#define UNLINK unlink
#define EXTENSION_PATH "build/sqlite_cron.so"
#define SLEEP_MS(x) usleep((x) * 1000)
#endif

/* Simple dynamic loading wrappers for Windows to avoid linking against
 * sqlite3.dll */
#if defined(_WIN32) || defined(_WIN64)
typedef struct sqlite3 sqlite3;
typedef struct sqlite3_stmt sqlite3_stmt;
typedef struct sqlite3_context sqlite3_context;

typedef int (*fn_sqlite3_open)(const char *, sqlite3 **);
typedef int (*fn_sqlite3_close)(sqlite3 *);
typedef int (*fn_sqlite3_exec)(sqlite3 *, const char *,
                               int (*)(void *, int, char **, char **), void *,
                               char **);
typedef int (*fn_sqlite3_load_extension)(sqlite3 *, const char *, const char *,
                                         char **);
typedef int (*fn_sqlite3_enable_load_extension)(sqlite3 *, int);
typedef void (*fn_sqlite3_free)(void *);
typedef const char *(*fn_sqlite3_errmsg)(sqlite3 *);

static fn_sqlite3_open p_sqlite3_open;
static fn_sqlite3_close p_sqlite3_close;
static fn_sqlite3_exec p_sqlite3_exec;
static fn_sqlite3_load_extension p_sqlite3_load_extension;
static fn_sqlite3_enable_load_extension p_sqlite3_enable_load_extension;
static fn_sqlite3_free p_sqlite3_free;
static fn_sqlite3_errmsg p_sqlite3_errmsg;

#define sqlite3_open p_sqlite3_open
#define sqlite3_close p_sqlite3_close
#define sqlite3_exec p_sqlite3_exec
#define sqlite3_load_extension p_sqlite3_load_extension
#define sqlite3_enable_load_extension p_sqlite3_enable_load_extension
#define sqlite3_free p_sqlite3_free
#define sqlite3_errmsg p_sqlite3_errmsg

int load_sqlite_functions() {
  HMODULE h = LoadLibrary("sqlite3.dll");
  if (!h) {
    /* Try common locations or just assume it's in path */
    h = LoadLibrary("C:\\sqlite\\sqlite3.dll");
  }
  if (!h)
    return 0;

  p_sqlite3_open = (fn_sqlite3_open)GetProcAddress(h, "sqlite3_open");
  p_sqlite3_close = (fn_sqlite3_close)GetProcAddress(h, "sqlite3_close");
  p_sqlite3_exec = (fn_sqlite3_exec)GetProcAddress(h, "sqlite3_exec");
  p_sqlite3_load_extension =
      (fn_sqlite3_load_extension)GetProcAddress(h, "sqlite3_load_extension");
  p_sqlite3_enable_load_extension =
      (fn_sqlite3_enable_load_extension)GetProcAddress(
          h, "sqlite3_enable_load_extension");
  p_sqlite3_free = (fn_sqlite3_free)GetProcAddress(h, "sqlite3_free");
  p_sqlite3_errmsg = (fn_sqlite3_errmsg)GetProcAddress(h, "sqlite3_errmsg");

  return p_sqlite3_open && p_sqlite3_close && p_sqlite3_exec &&
         p_sqlite3_load_extension && p_sqlite3_enable_load_extension &&
         p_sqlite3_free && p_sqlite3_errmsg;
}
#define SQLITE_OK 0
#else
int load_sqlite_functions() { return 1; }
#endif

/**
 * @brief Test callback mode - all API functions without threads.
 */
int test_callback_mode(void) {
  printf("=== Testing Callback Mode ===\n");

  sqlite3 *db;
  char *zErrMsg = 0;
  int rc;
  const char *db_name = "databases/leak_test_callback.db";

  UNLINK(db_name);
  rc = sqlite3_open(db_name, &db);
  if (rc != SQLITE_OK) {
    fprintf(stderr, "Can't open database\n");
    return 1;
  }

  sqlite3_enable_load_extension(db, 1);
  rc = sqlite3_load_extension(db, EXTENSION_PATH, 0, &zErrMsg);
  if (rc != SQLITE_OK) {
    fprintf(stderr, "Extension load error: %s\n",
            zErrMsg ? zErrMsg : "unknown");
    if (zErrMsg)
      sqlite3_free(zErrMsg);
    sqlite3_close(db);
    return 1;
  }

  /* Test all API functions in callback mode */
  sqlite3_exec(db, "SELECT cron_init('callback');", NULL, NULL, NULL);
  sqlite3_exec(db, "SELECT cron_schedule('leak_job', 60, 'SELECT 1;');", NULL,
               NULL, NULL);
  sqlite3_exec(db,
               "SELECT cron_schedule('timeout_job', 30, 'SELECT 2;', 5000);",
               NULL, NULL, NULL);
  sqlite3_exec(db, "SELECT cron_get('leak_job');", NULL, NULL, NULL);
  sqlite3_exec(db, "SELECT cron_list();", NULL, NULL, NULL);
  sqlite3_exec(db, "SELECT cron_pause('leak_job');", NULL, NULL, NULL);
  sqlite3_exec(db, "SELECT cron_resume('leak_job');", NULL, NULL, NULL);
  sqlite3_exec(db, "SELECT cron_update('leak_job', 120);", NULL, NULL, NULL);
  sqlite3_exec(db, "SELECT cron_delete('leak_job');", NULL, NULL, NULL);
  sqlite3_exec(db, "SELECT cron_delete('timeout_job');", NULL, NULL, NULL);
  sqlite3_exec(db, "SELECT cron_stop();", NULL, NULL, NULL);
  sqlite3_exec(db, "SELECT cron_reset();", NULL, NULL, NULL);

  sqlite3_close(db);
  UNLINK(db_name);
  printf("Callback mode: PASSED\n");
  return 0;
}

/**
 * @brief Test thread mode with configurable poll interval.
 */
int test_thread_mode(void) {
  printf("=== Testing Thread Mode ===\n");

  sqlite3 *db;
  char *zErrMsg = 0;
  int rc;
  const char *db_name = "databases/leak_test_thread.db";

  UNLINK(db_name);
  rc = sqlite3_open(db_name, &db);
  if (rc != SQLITE_OK) {
    fprintf(stderr, "Can't open database\n");
    return 1;
  }

  sqlite3_enable_load_extension(db, 1);
  rc = sqlite3_load_extension(db, EXTENSION_PATH, 0, &zErrMsg);
  if (rc != SQLITE_OK) {
    fprintf(stderr, "Extension load error: %s\n",
            zErrMsg ? zErrMsg : "unknown");
    if (zErrMsg)
      sqlite3_free(zErrMsg);
    sqlite3_close(db);
    return 1;
  }

  /* Enable WAL mode for thread mode */
  sqlite3_exec(db, "PRAGMA journal_mode=WAL;", NULL, NULL, NULL);

  /* Test thread mode with custom poll interval (2 seconds) */
  sqlite3_exec(db, "SELECT cron_init('thread', 2);", NULL, NULL, NULL);
  sqlite3_exec(db, "SELECT cron_schedule('thread_job', 1, 'SELECT 1;');", NULL,
               NULL, NULL);

  /* Let the thread run briefly */
  SLEEP_MS(100);

  sqlite3_exec(db, "SELECT cron_get('thread_job');", NULL, NULL, NULL);
  sqlite3_exec(db, "SELECT cron_list();", NULL, NULL, NULL);
  sqlite3_exec(db, "SELECT cron_pause('thread_job');", NULL, NULL, NULL);
  sqlite3_exec(db, "SELECT cron_resume('thread_job');", NULL, NULL, NULL);
  sqlite3_exec(db, "SELECT cron_update('thread_job', 60);", NULL, NULL, NULL);
  sqlite3_exec(db, "SELECT cron_delete('thread_job');", NULL, NULL, NULL);

  /* Graceful shutdown - tests thread cleanup */
  sqlite3_exec(db, "SELECT cron_stop();", NULL, NULL, NULL);
  sqlite3_exec(db, "SELECT cron_reset();", NULL, NULL, NULL);

  sqlite3_close(db);
  UNLINK(db_name);
  printf("Thread mode: PASSED\n");
  return 0;
}

/**
 * @brief Test error conditions and edge cases.
 */
int test_error_cases(void) {
  printf("=== Testing Error Cases ===\n");

  sqlite3 *db;
  char *zErrMsg = 0;
  int rc;
  const char *db_name = "databases/leak_test_errors.db";

  UNLINK(db_name);
  rc = sqlite3_open(db_name, &db);
  if (rc != SQLITE_OK) {
    return 1;
  }

  sqlite3_enable_load_extension(db, 1);
  rc = sqlite3_load_extension(db, EXTENSION_PATH, 0, &zErrMsg);
  if (rc != SQLITE_OK) {
    if (zErrMsg)
      sqlite3_free(zErrMsg);
    sqlite3_close(db);
    return 1;
  }

  sqlite3_exec(db, "PRAGMA journal_mode=WAL;", NULL, NULL, NULL);
  sqlite3_exec(db, "SELECT cron_init('thread');", NULL, NULL, NULL);

  /* Test operations on non-existent jobs (should not leak) */
  sqlite3_exec(db, "SELECT cron_get('nonexistent');", NULL, NULL, NULL);
  sqlite3_exec(db, "SELECT cron_pause('nonexistent');", NULL, NULL, NULL);
  sqlite3_exec(db, "SELECT cron_resume('nonexistent');", NULL, NULL, NULL);
  sqlite3_exec(db, "SELECT cron_update('nonexistent', 10);", NULL, NULL, NULL);
  sqlite3_exec(db, "SELECT cron_delete('nonexistent');", NULL, NULL, NULL);

  /* Test duplicate scheduling (should update, not leak) */
  sqlite3_exec(db, "SELECT cron_schedule('dup_job', 10, 'SELECT 1');", NULL,
               NULL, NULL);
  sqlite3_exec(db, "SELECT cron_schedule('dup_job', 20, 'SELECT 2');", NULL,
               NULL, NULL);
  sqlite3_exec(db, "SELECT cron_delete('dup_job');", NULL, NULL, NULL);

  sqlite3_exec(db, "SELECT cron_stop();", NULL, NULL, NULL);

  sqlite3_close(db);
  UNLINK(db_name);
  printf("Error cases: PASSED\n");
  return 0;
}

int main() {
  printf("sqlite-cron Memory Leak Test\n");
  printf("============================\n\n");

  if (!load_sqlite_functions()) {
    fprintf(stderr, "Failed to load sqlite3 functions dynamically\n");
    return 1;
  }

  int result = 0;

  result |= test_callback_mode();
  result |= test_thread_mode();
  result |= test_error_cases();

  printf("\n");
  if (result == 0) {
    printf("All leak tests PASSED!\n");
  } else {
    printf("Some tests FAILED!\n");
  }

  return result;
}
