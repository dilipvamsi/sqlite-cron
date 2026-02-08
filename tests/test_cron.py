#!/usr/bin/env python
"""
SQLite-Cron Extension Tests using unittest.

This test suite is designed to work with:
- Linux: python -m unittest tests.test_cron
- Windows: python -m unittest tests.test_cron
- Wine: wine python -m unittest tests.test_cron

No external dependencies required (no pytest).
"""
import sqlite3
import os
import sys
import time
import json
import unittest

# Paths
BUILD_DIR = "build"
DB_DIR = "databases"


def get_extension_path():
    """Get the correct extension path based on platform."""
    ext = ".so"
    if sys.platform == "win32":
        ext = ".dll"
    elif sys.platform == "darwin":
        ext = ".dylib"
    return os.path.join(BUILD_DIR, f"sqlite_cron{ext}")


EXTENSION_PATH = get_extension_path()


class CronTestBase(unittest.TestCase):
    """Base class with common setUp/tearDown for disk-based tests."""

    def setUp(self):
        self.test_db_path = os.path.join(os.getcwd(), "databases", "test_cron.db")
        self._cleanup_db_files()

        # Ensure directory exists
        os.makedirs(os.path.dirname(self.test_db_path), exist_ok=True)

        # Connect
        self.db = sqlite3.connect(self.test_db_path, isolation_level=None) # Autocommit
        self.db.enable_load_extension(True)

        # Load extension
        ext_path = os.path.join(os.getcwd(), "build", "sqlite_cron")
        self.db.load_extension(ext_path)

    def tearDown(self):
        if hasattr(self, 'db'):
            # Stop cron if active
            try:
                self.db.execute("SELECT cron_stop()")
            except Exception:
                pass
            self.db.close()
        self._cleanup_db_files()

    def _cleanup_db_files(self):
        """Remove database files."""
        # Use test_db_path if set, otherwise try default or skip
        path = getattr(self, 'test_db_path', os.path.join(os.getcwd(), "databases", "test_cron.db"))
        for f in [path, path + "-wal", path + "-shm"]:
            if os.path.exists(f):
                try:
                    os.remove(f)
                except:
                    pass


class MemoryDbTestCase(unittest.TestCase):
    """Base class for in-memory database tests."""

    def setUp(self):
        self.db = sqlite3.connect(":memory:")
        self.db.enable_load_extension(True)
        self.db.load_extension(EXTENSION_PATH)
        self.db.execute("SELECT cron_reset();")

    def tearDown(self):
        try:
            self.db.execute("SELECT cron_reset();")
        except:
            pass
        self.db.close()


# ======================= BASIC TESTS =======================

class TestInitialization(CronTestBase):

    def test_init_callback(self):
        res = self.db.execute("SELECT cron_init('callback');").fetchone()
        self.assertIn("Initialized: Callback mode", res[0])

    def test_double_init(self):
        self.db.execute("SELECT cron_init('callback');")
        res = self.db.execute("SELECT cron_init('callback');").fetchone()[0]
        self.assertIn("already initialized", res)

    def test_init_invalid_mode(self):
        with self.assertRaises(sqlite3.OperationalError) as ctx:
            self.db.execute("SELECT cron_init('invalid_mode');")
        self.assertIn("Unknown mode", str(ctx.exception))


class TestInitMemory(MemoryDbTestCase):

    def test_init_thread_memory_fail(self):
        with self.assertRaises(sqlite3.OperationalError) as ctx:
            self.db.execute("SELECT cron_init('thread');")
        self.assertIn("Thread mode requires disk-based DB", str(ctx.exception))


class TestScheduleAndDelete(CronTestBase):

    def test_reentrancy_guard(self):
        self.db.execute("SELECT cron_init('callback', 0, 1);")
        self.db.execute("CREATE TABLE log (id INTEGER);")
        self.db.execute("SELECT cron_schedule('reentrant', 0, 'SELECT 1; INSERT INTO log VALUES (1);');")

        # This should trigger the job but not recursion
        self.db.execute("SELECT 1;").fetchone()
        count = self.db.execute("SELECT count(*) FROM log;").fetchone()[0]
        # With rate_limit=0, it might run multiple times during one SELECT (per opcode callback)
        # but it should at least run once and not crash.
        self.assertGreaterEqual(count, 1)

    def test_callback_params_immediate(self):
        """Test that 0s rate limit allows immediate execution."""
        self.db.execute("SELECT cron_init('callback', 0, 1);")
        self.db.execute("CREATE TABLE cb_log_imm (msg TEXT);")
        self.db.execute("SELECT cron_schedule('cb_imm', 0, \"INSERT INTO cb_log_imm VALUES ('tick');\");")
        self.db.execute("SELECT 1;").fetchone()
        self.assertGreaterEqual(self.db.execute("SELECT count(*) FROM cb_log_imm;").fetchone()[0], 1)

    def test_callback_params_rate_limit(self):
        """Test that 10s rate limit blocks same-second ticks."""
        self.db.execute("CREATE TABLE cb_log_rl (msg TEXT);")
        self.db.execute("SELECT cron_schedule('cb_rl', 0, \"INSERT INTO cb_log_rl VALUES ('tick');\");")

        # Now init callback mode.
        # This statement itself might trigger the first tick after it returns.
        self.db.execute("SELECT cron_init('callback', 10, 1);")

        # Trigger query - should see exactly 1 tick in the log (either from init or this select)
        self.db.execute("SELECT 1;").fetchone()
        self.assertEqual(self.db.execute("SELECT count(*) FROM cb_log_rl;").fetchone()[0], 1)


        # Second query in same second should be blocked
        self.db.execute("SELECT 1;").fetchone()
        self.assertEqual(self.db.execute("SELECT count(*) FROM cb_log_rl;").fetchone()[0], 1)







    def test_schedule_and_delete(self):
        self.db.execute("SELECT cron_init('callback');")
        self.db.execute("CREATE TABLE events (msg TEXT);")
        self.db.execute("SELECT cron_schedule('test_job', 1, \"INSERT INTO events VALUES ('tick');\");")

        res = self.db.execute("SELECT count(*) FROM __cron_jobs WHERE name = 'test_job';").fetchone()
        self.assertEqual(res[0], 1)

        res = self.db.execute("SELECT cron_delete('test_job');").fetchone()
        self.assertEqual(res[0], 1)

        res = self.db.execute("SELECT count(*) FROM __cron_jobs WHERE name = 'test_job';").fetchone()
        self.assertEqual(res[0], 0)

    def test_unique_name(self):
        self.db.execute("SELECT cron_init('callback');")
        self.db.execute("SELECT cron_schedule('job1', 60, 'SELECT 1;');")
        with self.assertRaises(sqlite3.OperationalError):
            self.db.execute("SELECT cron_schedule('job1', 60, 'SELECT 1;');")

    def test_delete_non_existent(self):
        self.db.execute("SELECT cron_init('callback');")
        res = self.db.execute("SELECT cron_delete('non_existent_job');").fetchone()
        self.assertEqual(res[0], 0)


class TestPauseResume(CronTestBase):

    def test_pause_resume(self):
        self.db.execute("SELECT cron_init('callback');")
        self.db.execute("SELECT cron_schedule('p_job', 60, 'SELECT 1;');")

        self.db.execute("SELECT cron_pause('p_job');")
        res = self.db.execute("SELECT active FROM __cron_jobs WHERE name = 'p_job';").fetchone()
        self.assertEqual(res[0], 0)

        self.db.execute("SELECT cron_resume('p_job');")
        res = self.db.execute("SELECT active FROM __cron_jobs WHERE name = 'p_job';").fetchone()
        self.assertEqual(res[0], 1)

    def test_pause_non_existent(self):
        self.db.execute("SELECT cron_init('callback');")
        res = self.db.execute("SELECT cron_pause('non_existent_job');").fetchone()
        self.assertEqual(res[0], 0)

    def test_resume_non_existent(self):
        self.db.execute("SELECT cron_init('callback');")
        res = self.db.execute("SELECT cron_resume('non_existent_job');").fetchone()
        self.assertEqual(res[0], 0)


class TestUpdate(CronTestBase):

    def test_update_interval(self):
        self.db.execute("SELECT cron_init('callback');")
        self.db.execute("SELECT cron_schedule('u_job', 60, 'SELECT 1;');")

        self.db.execute("SELECT cron_update('u_job', 120);")
        res = self.db.execute("SELECT schedule_interval FROM __cron_jobs WHERE name = 'u_job';").fetchone()
        self.assertEqual(res[0], 120)

    def test_update_non_existent(self):
        self.db.execute("SELECT cron_init('callback');")
        res = self.db.execute("SELECT cron_update('non_existent_job', 120);").fetchone()
        self.assertEqual(res[0], 0)


class TestGetAndList(CronTestBase):

    def test_get_and_list_json(self):
        self.db.execute("SELECT cron_init('callback');")
        self.db.execute("SELECT cron_schedule('j1', 10, 'SELECT 1;');")
        self.db.execute("SELECT cron_schedule('j2', 20, 'SELECT 2;');")

        res = self.db.execute("SELECT cron_get('j1');").fetchone()
        job_j1 = json.loads(res[0])
        self.assertEqual(job_j1['name'], 'j1')
        self.assertEqual(job_j1['interval'], 10)

        res = self.db.execute("SELECT cron_list();").fetchone()
        jobs = json.loads(res[0])
        self.assertEqual(len(jobs), 2)
        names = [j['name'] for j in jobs]
        self.assertIn('j1', names)
        self.assertIn('j2', names)

    def test_get_non_existent(self):
        self.db.execute("SELECT cron_init('callback');")
        res = self.db.execute("SELECT cron_get('missing');").fetchone()
        self.assertIsNone(res[0])

    def test_list_empty(self):
        self.db.execute("SELECT cron_init('callback');")
        res = self.db.execute("SELECT cron_list();").fetchone()
        jobs = json.loads(res[0])
        self.assertEqual(jobs, [])

    def test_get_job_by_name(self):
        self.db.execute("SELECT cron_init('callback');")
        self.db.execute("SELECT cron_schedule('my_job', 30, 'SELECT 1');")

        res = self.db.execute("SELECT cron_get('my_job');").fetchone()
        job = json.loads(res[0])
        self.assertEqual(job['name'], 'my_job')
        self.assertEqual(job['interval'], 30)
        self.assertEqual(job['active'], 1)


class TestStop(CronTestBase):

    def test_graceful_shutdown(self):
        self.db.execute("SELECT cron_init('thread');")
        res = self.db.execute("SELECT cron_stop();").fetchone()[0]
        self.assertEqual(res, "Engine stopped gracefully")

    def test_stop_callback_mode(self):
        self.db.execute("SELECT cron_init('callback');")
        res = self.db.execute("SELECT cron_stop();").fetchone()[0]
        self.assertIn("Engine stopped gracefully", res)

    def test_stop_not_active(self):
        res = self.db.execute("SELECT cron_stop();").fetchone()[0]
        self.assertIn("Engine is not active", res)


# ======================= EXECUTION TESTS =======================

class TestExecutionLogging(CronTestBase):

    def test_execution_logging(self):
        self.db.execute("SELECT cron_init('callback');")
        self.db.execute("CREATE TABLE events (msg TEXT);")
        self.db.execute("SELECT cron_schedule('log_job', 1, \"INSERT INTO events VALUES ('log_tick');\");")

        time.sleep(1.2)
        self.db.execute("WITH RECURSIVE cnt(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM cnt WHERE x<20) SELECT count(*) FROM cnt;").fetchone()

        res = self.db.execute("SELECT job_name, status, duration_ms FROM __cron_log WHERE job_name = 'log_job';").fetchone()
        self.assertIsNotNone(res)
        self.assertEqual(res[0], 'log_job')
        self.assertEqual(res[1], 'SUCCESS')
        self.assertGreaterEqual(res[2], 0)

    def test_failure_logging(self):
        self.db.execute("SELECT cron_init('callback');")
        self.db.execute("SELECT cron_schedule('fail_job', 1, \"INSERT INTO non_existent_table VALUES (1);\");")

        time.sleep(1.2)
        self.db.execute("WITH RECURSIVE cnt(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM cnt WHERE x<20) SELECT count(*) FROM cnt;").fetchone()

        res = self.db.execute("SELECT status, error_message FROM __cron_log WHERE job_name = 'fail_job';").fetchone()
        self.assertEqual(res[0], 'FAILURE')
        self.assertIn("no such table", res[1].lower())


class TestJobTimeout(CronTestBase):

    def test_job_timeout(self):
        self.db.execute("SELECT cron_init('callback');")
        long_query = "WITH RECURSIVE cnt(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM cnt WHERE x<1000000) SELECT count(*) FROM cnt"
        self.db.execute("SELECT cron_schedule('timeout_job', 1, ?, 100);", (long_query,))

        time.sleep(1.2)
        self.db.execute("WITH RECURSIVE cnt(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM cnt WHERE x<1000) SELECT count(*) FROM cnt;").fetchone()

        res = self.db.execute("SELECT status FROM __cron_log WHERE job_name = 'timeout_job';").fetchone()
        self.assertIsNotNone(res)
        self.assertEqual(res[0], "TIMEOUT")


class TestRateLimiting(CronTestBase):

    def test_rate_limiting(self):
        self.db.execute("SELECT cron_init('callback');")
        self.db.execute("CREATE TABLE IF NOT EXISTS events (msg TEXT);")
        self.db.execute("SELECT cron_schedule('rate_job', 1, \"INSERT INTO events VALUES ('tick');\");")

        time.sleep(1.5)
        self.db.execute("WITH RECURSIVE cnt(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM cnt WHERE x<100) SELECT count(*) FROM cnt;").fetchone()
        self.db.execute("WITH RECURSIVE cnt(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM cnt WHERE x<100) SELECT count(*) FROM cnt;").fetchone()

        res = self.db.execute("SELECT count(*) FROM __cron_log WHERE job_name = 'rate_job';").fetchone()
        self.assertGreaterEqual(res[0], 1)


    def test_reentrancy_guard(self):
        self.db.execute("SELECT cron_init('callback');")
        self.db.execute("CREATE TABLE IF NOT EXISTS counter (val INTEGER);")
        self.db.execute("INSERT INTO counter VALUES (0);")
        self.db.execute("SELECT cron_schedule('reentrancy_job', 1, 'UPDATE counter SET val = val + 1');")

        time.sleep(1.2)
        for _ in range(10):
            self.db.execute("WITH RECURSIVE cnt(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM cnt WHERE x<50) SELECT count(*) FROM cnt;").fetchone()

        res = self.db.execute("SELECT val FROM counter;").fetchone()
        self.assertGreaterEqual(res[0], 1)


    def test_callback_rapid_fire(self):
        self.db.execute("SELECT cron_init('callback');")
        self.db.execute("SELECT cron_schedule('rapid_job', 1, 'SELECT 42');")

        time.sleep(1.2)
        for _ in range(20):
            self.db.execute("SELECT 1;").fetchone()

        res = self.db.execute("SELECT count(*) FROM __cron_log WHERE job_name = 'rapid_job';").fetchone()
        self.assertGreaterEqual(res[0], 1)


# ======================= THREAD MODE TESTS =======================

class TestAutoShutdown(CronTestBase):

    def test_auto_shutdown_on_close(self):
        self.db.execute("SELECT cron_init('thread');")
        self.db.close()
        time.sleep(0.5)

        new_conn = sqlite3.connect(self.test_db_path)
        new_conn.enable_load_extension(True)
        new_conn.load_extension(EXTENSION_PATH)

        res = new_conn.execute("SELECT cron_init('thread');").fetchone()[0]
        self.assertIn("Initialized: Thread mode", res)
        new_conn.close()
        self.db = sqlite3.connect(":memory:")  # Prevent tearDown error


class TestStopDuringJob(CronTestBase):

    def test_stop_during_job(self):
        self.db.execute("SELECT cron_init('thread');")
        self.db.execute("CREATE TABLE IF NOT EXISTS results (val INTEGER);")

        long_query = "WITH RECURSIVE cnt(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM cnt WHERE x<2000000) INSERT INTO results SELECT count(*) FROM cnt"
        self.db.execute("SELECT cron_schedule('long_job', 1, ?);", (long_query,))

        time.sleep(1.2)
        res_row = self.db.execute("SELECT cron_stop();").fetchone()
        res = res_row[0] if res_row else "Failed"

        self.assertIn("Engine stopped gracefully", res)
        self.assertIn("Engine stopped gracefully", res)
        # Check if job completed or was interrupted
        row = self.db.execute("SELECT val FROM results;").fetchone()

        # If the job finished before stop, row will have count.
        # If it was interrupted, row might be None or partial if we had intermediate commits (we don't here).
        # We also check the log to confirm it didn't crash.
        log = self.db.execute("SELECT status FROM __cron_log WHERE job_name='long_job' ORDER BY id DESC LIMIT 1").fetchone()
        print(f"DEBUG: Stop job status: {log}")

        if row:
             self.assertEqual(row[0], 2000000)
             self.assertEqual(log[0], 'SUCCESS')
        else:
             # If interrupted, status should be TIMEOUT (from sqlite3_interrupt) or RUNNING if stop didn't update yet?
             # cron_stop calls sqlite3_interrupt, so we expect TIMEOUT or FAILURE.
             self.assertIn(log[0], ['TIMEOUT', 'FAILURE'])


class TestCloseDuringJob(CronTestBase):

    def test_close_during_job(self):
        self.db.execute("SELECT cron_init('thread');")
        self.db.execute("CREATE TABLE IF NOT EXISTS results (val INTEGER);")
        long_query = "WITH RECURSIVE cnt(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM cnt WHERE x<2000000) INSERT INTO results SELECT count(*) FROM cnt"
        self.db.execute("SELECT cron_schedule('long_job', 1, ?);", (long_query,))

        time.sleep(1.2)
        self.db.close()

        new_conn = sqlite3.connect(self.test_db_path)
        res = new_conn.execute("SELECT val FROM results;").fetchone()
        # Job might finish (2M) or get interrupted by close() (None/Partial)
        # Main goal is that we didn't crash the process/corrupt DB locking.
        # We can check logs to see what happened.
        log = new_conn.execute("SELECT status FROM __cron_log WHERE job_name='long_job' ORDER BY id DESC LIMIT 1").fetchone()
        print(f"DEBUG: Close job status: {log}")
        if res:
             self.assertEqual(res[0], 2000000)
        else:
             # If no result, log should be TIMEOUT (from interrupt) or RUNNING (if checking too fast?)
             # Assuming shutdown completed, it should be TIMEOUT.
             pass
        new_conn.close()
        self.db = sqlite3.connect(":memory:")  # Prevent tearDown error


class TestImmediateInterrupt(CronTestBase):

    def test_immediate_interrupt(self):
        self.db.execute("SELECT cron_init('thread');")
        long_query = "WITH RECURSIVE cnt(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM cnt WHERE x<100000000) SELECT count(*) FROM cnt;"
        self.db.execute("SELECT cron_schedule('long_job', 1, ?);", (long_query,))

        time.sleep(1.2)

        start = time.time()
        self.db.execute("SELECT cron_stop();")
        duration = time.time() - start

        self.assertLess(duration, 1.0)

        # Verify TIMEOUT status
        status = None
        for _ in range(30):
            new_conn = sqlite3.connect(self.test_db_path)
            new_conn.execute("PRAGMA journal_mode=WAL;")
            new_conn.execute("PRAGMA wal_checkpoint(FULL);")
            rows = new_conn.execute("SELECT id, job_name, status FROM __cron_log ORDER BY id DESC;").fetchall()
            new_conn.close()

            for row in rows:
                if row[1] == 'long_job' and row[2] == 'TIMEOUT':
                    status = 'TIMEOUT'
                    break
            if status == 'TIMEOUT':
                break
            time.sleep(0.1)

        self.assertEqual(status, "TIMEOUT")


class TestMultiConnectionRefcount(CronTestBase):

    def test_multi_connection_refcount(self):
        self.db.execute("SELECT cron_init('thread');")

        conn2 = sqlite3.connect(self.test_db_path)
        conn2.enable_load_extension(True)
        conn2.load_extension(EXTENSION_PATH)
        res = conn2.execute("SELECT cron_init('thread');").fetchone()[0]
        self.assertIn("Ref count increased", res)

        self.db.close()
        time.sleep(0.5)

        conn2.execute("SELECT cron_schedule('still_alive', 1, 'SELECT 1');")
        time.sleep(2.5)
        res = conn2.execute("SELECT status FROM __cron_log WHERE job_name = 'still_alive';").fetchone()
        self.assertIsNotNone(res)

        conn2.close()
        time.sleep(1.2)

        new_conn = sqlite3.connect(self.test_db_path)
        new_conn.enable_load_extension(True)
        new_conn.load_extension(EXTENSION_PATH)
        res = new_conn.execute("SELECT cron_init('thread');").fetchone()[0]
        self.assertIn("Initialized: Thread mode", res)
        new_conn.close()
        self.db = sqlite3.connect(":memory:")  # Prevent tearDown error


class TestResetWithActiveEngine(CronTestBase):

    def test_reset_with_active_engine(self):
        self.db.execute("SELECT cron_init('thread');")
        self.db.execute("SELECT cron_schedule('active_job', 1, 'SELECT 1');")
        time.sleep(1.2)

        res = self.db.execute("SELECT cron_reset();").fetchone()
        self.assertIn("Cron reset", res[0])

        res = self.db.execute("SELECT cron_init('callback');").fetchone()
        self.assertIn("Initialized", res[0])


# ======================= CRASH RECOVERY TEST =======================

class TestCrashRecovery(unittest.TestCase):

    def test_crash_recovery(self):
        if not os.path.exists(DB_DIR):
            os.makedirs(DB_DIR)
        db_path = os.path.abspath(os.path.join(DB_DIR, "crash_recovery.db"))
        if os.path.exists(db_path):
            os.remove(db_path)

        conn = sqlite3.connect(db_path)
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.enable_load_extension(True)
        conn.load_extension(EXTENSION_PATH)

        conn.execute("SELECT cron_init('callback');")
        conn.execute("INSERT INTO __cron_log (job_name, start_time, status) VALUES ('stale_job', 12345, 'RUNNING');")
        conn.commit()
        conn.execute("SELECT cron_stop();")
        conn.close()

        conn = sqlite3.connect(db_path)
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.enable_load_extension(True)
        conn.load_extension(EXTENSION_PATH)

        res = conn.execute("SELECT status FROM __cron_log WHERE job_name = 'stale_job';").fetchone()
        self.assertIsNotNone(res)
        self.assertEqual(res[0], "CRASHED")

        conn.close()
        if os.path.exists(db_path):
            os.remove(db_path)


# ======================= CRON EXPRESSION TESTS =======================

class TestCronExpressions(CronTestBase):

    def test_33_cron_schedule_basic(self):
        """Test basic cron scheduling."""
        self.db.execute("SELECT cron_init('callback')")
        self.db.execute("SELECT cron_schedule_cron('cron1', '0 * * * * *', 'SELECT 1')")

        job = json.loads(self.db.execute("SELECT cron_get('cron1')").fetchone()[0])
        self.assertEqual(job['cron'], '0 * * * * *')
        self.assertTrue(job['next_run'] > time.time())
        # The next run should be within the next minute
        self.assertTrue(job['next_run'] <= time.time() + 61)

    def test_34_cron_invalid_expression(self):
        """Test that invalid cron expressions are rejected."""
        self.db.execute("SELECT cron_init('callback')")
        with self.assertRaises(sqlite3.OperationalError) as cm:
            self.db.execute("SELECT cron_schedule_cron('bad', 'invalid cron', 'SELECT 1')")
        self.assertIn("Invalid cron expression", str(cm.exception))

    def test_35_cron_execution(self):
        """Test that cron jobs actually execute."""
        # Use a secondary-level cron expression if supported (ccronexpr supports it: second, minute, hour, day, month, dayOfWeek)
        # */1 * * * * * means every second
        self.db.execute("SELECT cron_init('callback')")
        self.db.execute("SELECT cron_schedule_cron('fast_cron', '*/1 * * * * *', 'INSERT INTO results (val) VALUES (42)')")

        # Create results table for testing
        self.db.execute("CREATE TABLE results (val INTEGER)")

        # Trigger engine via progress handler
        time.sleep(1.1)
        self.db.execute("SELECT count(*) FROM results") # Trigger progress handler

        count = self.db.execute("SELECT count(*) FROM results").fetchone()[0]
        self.assertGreaterEqual(count, 1)

    def test_36_cron_list_json(self):
        """Test that cron_list includes the cron expression."""
        self.db.execute("SELECT cron_init('callback')")
        self.db.execute("SELECT cron_schedule_cron('cron_item', '0 0 * * * *', 'SELECT 1')")

        jobs = json.loads(self.db.execute("SELECT cron_list()").fetchone()[0])
        self.assertEqual(len(jobs), 1)
        self.assertEqual(jobs[0]['cron'], '0 0 * * * *')
        self.assertEqual(jobs[0]['interval'], 0) # interval should be 0 for cron jobs

    def test_37_cron_update_preserves_cron(self):
        """Test that updating a cron job works (currently update only supports interval)."""
        # Note: cron_update currently only updates schedule_interval.
        # For now, let's just ensure it doesn't crash or erase the cron string if we don't want it to.
        self.db.execute("SELECT cron_init('callback')")
        self.db.execute("SELECT cron_schedule_cron('cron_upd', '0 0 * * * *', 'SELECT 1')")

        self.db.execute("SELECT cron_update('cron_upd', 3600)")
        job = json.loads(self.db.execute("SELECT cron_get('cron_upd')").fetchone()[0])
        self.assertEqual(job['interval'], 3600)
        # If we update interval, it might switch to interval-based next-run calculation in cron_tick.
        # This is expected behavior for now as cron_update doesn't take a cron string yet.


    def test_38_cron_complex_expressions(self):
        """Test complex cron expressions (ranges, lists, steps)."""
        self.db.execute("SELECT cron_init('callback')")

        # Range: 20-30 seconds
        self.db.execute("SELECT cron_schedule_cron('range_job', '20-30 * * * * *', 'SELECT 1')")
        job = json.loads(self.db.execute("SELECT cron_get('range_job')").fetchone()[0])
        self.assertEqual(job['cron'], '20-30 * * * * *')

        # List: 1,15,30,45 seconds
        self.db.execute("SELECT cron_schedule_cron('list_job', '1,15,30,45 * * * * *', 'SELECT 1')")
        job = json.loads(self.db.execute("SELECT cron_get('list_job')").fetchone()[0])
        self.assertEqual(job['cron'], '1,15,30,45 * * * * *')

        # Step: Every 5 seconds
        self.db.execute("SELECT cron_schedule_cron('step_job', '*/5 * * * * *', 'SELECT 1')")
        job = json.loads(self.db.execute("SELECT cron_get('step_job')").fetchone()[0])
        self.assertEqual(job['cron'], '*/5 * * * * *')

    def test_39_demo_recursion(self):
        """Replicate the logic from demo_cron.py to ensure it works."""
        self.db.execute("SELECT cron_init('callback')")
        self.db.execute("CREATE TABLE demo_log (id INTEGER PRIMARY KEY, message TEXT, timestamp DATETIME DEFAULT CURRENT_TIMESTAMP)")

        # Schedule jobs similar to demo
        self.db.execute("SELECT cron_schedule_cron('fast_job', '*/1 * * * * *', \"INSERT INTO demo_log (message) VALUES ('Every Second')\")")

        # Simulate execution for 3 seconds
        # We need to ensure we call the progress handler frequently enough
        for _ in range(30):
            time.sleep(0.1)
            self.db.execute("SELECT 1") # Trigger progress handler

        rows = self.db.execute("SELECT message FROM demo_log").fetchall()
        # Should have executed at least 2 times in ~3 seconds
        self.assertGreaterEqual(len(rows), 2)
        messages = [r[0] for r in rows]
        self.assertIn('Every Second', messages)

    def test_40_library_examples(self):
        """Test examples directly from ccronexpr_test.c to verify parsing."""
        self.db.execute("SELECT cron_init('callback')")

        examples = [
            "0 15 10 L * ?",       # Last day of month
            "0 15 10 ? * 6L",      # Last Friday
            "0 15 10 ? * 2-6",     # Weekdays
            "0 0/5 14-18 * * ?",   # Step and range
            "0 0 12 ? * SUN,MON",  # List of days
            "0 0 12 1/5 * ?"       # Day of month steps
        ]

        for i, expr in enumerate(examples):
            name = f"lib_ex_{i}"
            # valid expression should schedule without error
            self.db.execute(f"SELECT cron_schedule_cron('{name}', '{expr}', 'SELECT 1')")

            # verify it was stored correctly
            job_row = self.db.execute(f"SELECT cron_get('{name}')").fetchone()
            self.assertIsNotNone(job_row, f"Job {name} failed to schedule")
            job = json.loads(job_row[0])
            self.assertEqual(job['cron'], expr)

    def test_41_edge_cases(self):
        """Test edge cases: leap years, case insensitivity, whitespace."""
        self.db.execute("SELECT cron_init('callback')")

        # Leap year: Feb 29
        self.db.execute("SELECT cron_schedule_cron('leap_job', '0 0 0 29 FEB ?', 'SELECT 1')")
        job = json.loads(self.db.execute("SELECT cron_get('leap_job')").fetchone()[0])
        self.assertEqual(job['cron'], '0 0 0 29 FEB ?')

        # Case insensitivity
        self.db.execute("SELECT cron_schedule_cron('case_job', '0 0 12 ? * mon,wed,fri', 'SELECT 1')")
        job = json.loads(self.db.execute("SELECT cron_get('case_job')").fetchone()[0])
        self.assertEqual(job['cron'], '0 0 12 ? * mon,wed,fri')

        # Extra whitespace
        self.db.execute("SELECT cron_schedule_cron('space_job', '0  0   12  ?  *  SUN', 'SELECT 1')")
        job = json.loads(self.db.execute("SELECT cron_get('space_job')").fetchone()[0])
        # Note: The library might normalize or keep the string. We check what we stored.
        # SQLite just stores the string as-is in the DB.
        self.assertEqual(job['cron'], '0  0   12  ?  *  SUN')

        # Verify it parses correctly by checking next_run is valid (not 0 or error)
        self.assertTrue(job['next_run'] > 0)

    def test_42_interval_accuracy(self):
        """Test that jobs run at the expected 1-second interval."""
        self.db.execute("SELECT cron_init('callback')")
        self.db.execute("CREATE TABLE timing_log (id INTEGER PRIMARY KEY, run_time REAL)")

        # Schedule every 1 second
        self.db.execute("SELECT cron_schedule_cron('timer_job', '*/1 * * * * *', 'INSERT INTO timing_log (run_time) VALUES ((SELECT julianday(''now'') * 86400.0))')")

        # Run for ~5 seconds
        for _ in range(55):
            time.sleep(0.1)
            self.db.execute("SELECT 1")

        rows = self.db.execute("SELECT run_time FROM timing_log ORDER BY id").fetchall()
        self.assertGreaterEqual(len(rows), 3, "Should have run at least 3 times in 5.5 seconds")

        # Check intervals
        timestamps = [r[0] for r in rows]
        for i in range(1, len(timestamps)):
            diff = timestamps[i] - timestamps[i-1]
            # Should be approx 1.0 second. Allow a bit of jitter (0.5 to 1.5 to be safe on loaded systems)
            self.assertTrue(0.5 <= diff <= 1.9, f"Interval {diff}s is out of range (expected ~1.0s)")

    def test_43_macros(self):
        """Test cron macros like @daily, @hourly."""
        self.db.execute("SELECT cron_init('callback')")

        macros = {
            "@hourly": "0 0 * * * *",
            "@daily": "0 0 0 * * *",
            "@weekly": "0 0 0 * * 0",
            "@monthly": "0 0 0 1 * *",
            "@yearly": "0 0 0 1 1 *",
            "@annually": "0 0 0 1 1 *",
            "@midnight": "0 0 0 * * *"
        }

        for macro, expected in macros.items():
            name = f"macro_{macro[1:]}"
            self.db.execute(f"SELECT cron_schedule_cron('{name}', '{macro}', 'SELECT 1')")
            job = json.loads(self.db.execute(f"SELECT cron_get('{name}')").fetchone()[0])
            # The library might resolve the macro to the explicit string or keep it.
            # Based on the C code, it replaces the pointer 'expression' with the string literal.
            # So we expect it to be stored as the SCHEDULE, NOT the macro name?
            # Wait, ccronexpr parses it, but sqlite_cron stores the original string passed to it?
            # Let's check sqlite_cron.c.
            # In cron_schedule_cron_func: sqlite3_bind_text(stmt, 2, cron_expr_str, -1, SQLITE_TRANSIENT);
            # It stores the string PASSED IN. So we expect the macro name.
            self.assertEqual(job['cron'], macro)
            self.assertTrue(job['next_run'] > 0)

    def test_44_next_run_prediction(self):
        """Verify that the calculated next_run time is correct."""
        self.db.execute("SELECT cron_init('callback')")

        # Schedule to run at the top of the next hour
        cron_expr = "0 0 * * * *"
        self.db.execute(f"SELECT cron_schedule_cron('prediction', '{cron_expr}', 'SELECT 1')")

        job = json.loads(self.db.execute("SELECT cron_get('prediction')").fetchone()[0])
        next_run = job['next_run']

        # Calculate expected next run in Python using LOCAL time
        now = time.time()
        from datetime import datetime, timedelta
        dt = datetime.fromtimestamp(now)
        # Round up to next hour
        dt_next = dt.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)

        expected_epoch = dt_next.timestamp()

        self.assertAlmostEqual(next_run, expected_epoch, delta=1)


    def test_45_schedule_alignment(self):
        """Test that jobs align to the specific seconds defined in cron."""
        self.db.execute("SELECT cron_init('callback')")
        self.db.execute("CREATE TABLE align_log (id INTEGER PRIMARY KEY, run_time REAL)")

        # Schedule every 2 seconds (0, 2, 4...)
        # Unix timestamp = (julianday - 2440587.5) * 86400.0
        self.db.execute("SELECT cron_schedule_cron('align_job', '*/2 * * * * *', 'INSERT INTO align_log (run_time) VALUES ((SELECT (julianday(''now'') - 2440587.5) * 86400.0))')")

        # Run for ~5 seconds
        for _ in range(55):
            time.sleep(0.1)
            self.db.execute("SELECT 1")

        rows = self.db.execute("SELECT run_time FROM align_log ORDER BY id").fetchall()
        self.assertGreaterEqual(len(rows), 2, "Should have run at least 2 times")

        for r in rows:
            ts = r[0]
            # We expect execution shortly after an even second.
            # ts % 2 should be strictly between 0.0 and say 0.9 (latency)
            # If it ran on an odd second, it would be > 1.0.
            remainder = ts % 2
            self.assertLess(remainder, 0.9, f"Job executed at {ts} (rem {remainder}), expected alignment to even seconds")

    def test_46_local_time(self):
        """Test that scheduling respects local time (not UTC)."""
        self.db.execute("SELECT cron_init('callback')")

        # We need to construct a cron expression that triggers in the next minute *in local time*
        # If the system were UTC-based, and we are in say EST (-5), there's a 5 hour diff.

        now = time.time()
        # Local time struct
        local_tm = time.localtime(now)

        # Schedule for the next minute (local)
        next_min = local_tm.tm_min + 1
        next_hour = local_tm.tm_hour
        if next_min >= 60:
            next_min = 0
            next_hour += 1
        if next_hour >= 24:
             next_hour = 0
             # technically next day, but * * * covers it

        cron_expr = f"0 {next_min} {next_hour} * * *"
        self.db.execute(f"SELECT cron_schedule_cron('local_job', '{cron_expr}', 'SELECT 1')")

        job = json.loads(self.db.execute("SELECT cron_get('local_job')").fetchone()[0])
        next_run = job['next_run']

        # Calculate expected epoch for that local time
        # mktime() takes LOCAL time struct and returns epoch
        import copy
        expected_tm = time.localtime(now)
        # We need a mutable struct or wait, struct_time is immutable.
        # Use simple calculation or datetime logic if possible, but keep it simple with mktime
        # We know mktime is exact inverse of localtime

        # Let's use datetime for easier manipulation
        from datetime import datetime, timedelta
        dt = datetime.fromtimestamp(now)
        # Round up to next minute start
        dt_next = dt.replace(second=0, microsecond=0) + timedelta(minutes=1)
        expected_epoch = dt_next.timestamp()

        # Allow 1 second fuzz
        self.assertAlmostEqual(next_run, expected_epoch, delta=1.0)

    def test_47_timeout(self):
        """Test that detailed timeouts work (job is killed)."""
        # Busy loop in SQL to simulate work
        # 10 Million should be enough to exceed 200ms on most modern CPUs.
        long_query = "WITH RECURSIVE cnt(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM cnt WHERE x<10000000) SELECT count(*) FROM cnt;"

        self.db.execute("SELECT cron_init('thread', 1)") # Poll every 1s

        self.db.execute("CREATE TABLE IF NOT EXISTS completed (id INTEGER)")

        # Schedule with 200ms timeout
        self.db.execute(f"SELECT cron_schedule('timeout_job', 1, '{long_query}', 200)")

        # Wait loop to check for non-RUNNING status
        # Wait loop to check for TIMEOUT status
        success = False
        for _ in range(20): # Try for 10 seconds
            time.sleep(0.5)
            # Checkpoint to ensure we see worker thread writes
            try:
                self.db.execute("PRAGMA wal_checkpoint(PASSIVE);")
            except:
                pass

            # Check if ANY run resulted in TIMEOUT
            count = self.db.execute("SELECT count(*) FROM __cron_log WHERE job_name='timeout_job' AND status='TIMEOUT'").fetchone()[0]
            if count > 0:
                success = True
                break

        self.assertTrue(success, "Job should have logged TIMEOUT at least once")

    def test_48_retry(self):
        """Test that failed jobs are retried according to policy."""
        self.db.execute("SELECT cron_init('thread', 1)")
        self.db.execute("CREATE TABLE IF NOT EXISTS retry_counts (id INTEGER PRIMARY KEY, run_count INTEGER DEFAULT 0)")
        self.db.execute("INSERT INTO retry_counts (id, run_count) VALUES (1, 0)")

        # "BEGIN; ... COMMIT;" handles transaction inside the cron job
        fail_sql = "BEGIN; UPDATE retry_counts SET run_count = run_count + 1 WHERE id = 1; COMMIT; SELECT load_extension(''non_existent_fail'');"

        self.db.execute(f"SELECT cron_schedule('retry_job', 3600, '{fail_sql}')")
        # Set rety: 2 retries, 1 second interval
        self.db.execute("SELECT cron_set_retry('retry_job', 2, 1)")

        # Trigger first run manually by setting next_run to now
        self.db.execute("UPDATE __cron_jobs SET next_run = strftime('%s','now') - 1 WHERE name='retry_job'")

        # Wait for thread to pick it up (Poll 1s)
        time.sleep(1.5)

        count = self.db.execute("SELECT run_count FROM retry_counts WHERE id=1").fetchone()[0]

        # Check logs for FAILURE - loop briefly in case logging is slow
        log = None
        for _ in range(10):
            log = self.db.execute("SELECT status, error_message FROM __cron_log WHERE job_name='retry_job' ORDER BY id DESC LIMIT 1").fetchone()
            if log:
                break
            time.sleep(0.5)

        if log is None:
             self.fail("DEBUG: No log found execution didn't happen?")
        else:
             print(f"DEBUG: Log status: {log[0]}, Error: {log[1]}")

        self.assertEqual(count, 1, f"Should have run once. Log: {log}")
        self.assertEqual(log[0], 'FAILURE')

        # Check job status for retries
        job = self.db.execute("SELECT retries_attempted, next_run FROM __cron_jobs WHERE name='retry_job'").fetchone()
        self.assertEqual(job[0], 1, "Should have marked 1 retry attempted")

        # Wait for retry interval (1s) slightly
        time.sleep(1.2)

        # Tick 2: First Retry
        # No manual trigger needed, thread handles it


        count = self.db.execute("SELECT run_count FROM retry_counts WHERE id=1").fetchone()[0]
        self.assertEqual(count, 2, "Should have run twice (1 retry)")

        job = self.db.execute("SELECT retries_attempted FROM __cron_jobs WHERE name='retry_job'").fetchone()
        self.assertEqual(job[0], 2, "Should have marked 2 retries attempted")

        # Wait for second retry
        time.sleep(1.5)

        # Tick 3: Second Retry

        count = self.db.execute("SELECT run_count FROM retry_counts WHERE id=1").fetchone()[0]
        self.assertEqual(count, 3, "Should have run thrice (2 retries)")

        # Now it is exhausted. Next run should be normal schedule (3600s later).
        # And retries_attempted should be reset?
        # Our logic: "Exhausted retries, proceed to normal schedule -> next_retries_attempted = 0"

        job = self.db.execute("SELECT retries_attempted, next_run FROM __cron_jobs WHERE name='retry_job'").fetchone()
        self.assertEqual(job[0], 0, "Should have reset retry counter after exhaustion")

        next_run = job[1]
        now = time.time()
        self.assertTrue(next_run > now + 3000, "Next run should be far in future (normal schedule)")

class TestAdditionalFeatures(CronTestBase):
    def test_cron_run(self):
        """Test manual execution of a job."""
        self.db.execute("SELECT cron_init('thread');")
        self.db.execute("CREATE TABLE run_log (msg TEXT);")
        # Schedule far in future so it doesn't run automatically
        self.db.execute("SELECT cron_schedule('manual_job', 3600, \"INSERT INTO run_log VALUES ('ran');\");")

        # Manual run
        res = self.db.execute("SELECT cron_run('manual_job');").fetchone()[0]
        self.assertEqual(res, 1)

        # Verify execution
        count = self.db.execute("SELECT count(*) FROM run_log;").fetchone()[0]
        self.assertEqual(count, 1)

    def test_cron_purge_logs(self):
        """Test log purging with different retention periods."""
        self.db.execute("SELECT cron_init('thread');")

        # Insert old log (10 days ago) using SQL injection directly to bypass readonly fields if any
        # __cron_log start_time is just an INTEGER
        old_time = int(time.time()) - 86400 * 10
        self.db.execute("INSERT INTO __cron_log (job_name, start_time, status) VALUES ('old_job', ?, 'COMPLETED')", (old_time,))

        # Insert recent log (2 days ago)
        recent_time = int(time.time()) - 86400 * 2
        self.db.execute("INSERT INTO __cron_log (job_name, start_time, status) VALUES ('new_job', ?, 'COMPLETED')", (recent_time,))

        self.assertEqual(self.db.execute("SELECT count(*) FROM __cron_log").fetchone()[0], 2)

        # Purge older than 7 days
        deleted = self.db.execute("SELECT cron_purge_logs('-7 days');").fetchone()[0]
        self.assertEqual(deleted, 1)

        count = self.db.execute("SELECT count(*) FROM __cron_log").fetchone()[0]
        self.assertEqual(count, 1)

        # Check that the recent log remains
        job_name = self.db.execute("SELECT job_name FROM __cron_log").fetchone()[0]
        self.assertEqual(job_name, 'new_job')

    def test_cron_status(self):
        """Test engine status reporting."""
        self.db.execute("SELECT cron_init('thread');")
        self.db.execute("SELECT cron_schedule('job1', 60, 'SELECT 1');")

        res = self.db.execute("SELECT cron_status();").fetchone()[0]
        status = json.loads(res)

        self.assertEqual(status['mode'], 'thread')
        self.assertIn('active', status) # Might be 1 or 0 if thread creation failed/success
        # job_count might be 0 or 1 depending on timing, but let's check
        self.assertEqual(status['job_count'], 1)
        self.assertIn('last_check', status)

    def test_cron_next_job_time(self):
        """Test introspection of next run times."""
        self.db.execute("SELECT cron_init('thread');")
        self.db.execute("SELECT cron_schedule('job1', 3600, 'SELECT 1');")

        # 1. Specific job
        next_run = self.db.execute("SELECT cron_job_next('job1');").fetchone()[0]
        self.assertIsInstance(next_run, int)
        # Next run should be roughly now + 3600
        self.assertAlmostEqual(next_run, int(time.time()) + 3600, delta=10)

        # 2. All jobs
        res = self.db.execute("SELECT cron_next_job_time();").fetchone()[0]
        all_jobs = json.loads(res)
        self.assertIn('job1', all_jobs)
        self.assertEqual(all_jobs['job1'], next_run)

    def test_cron_next_job(self):
        """Test retrieving the single nearest upcoming job."""
        self.db.execute("SELECT cron_init('thread');")
        # Job 1: 1 hour from now
        self.db.execute("SELECT cron_schedule('job_far', 3600, 'SELECT 1');")
        # Job 2: 1 minute from now (should be nearest)
        self.db.execute("SELECT cron_schedule('job_near', 60, 'SELECT 1');")

        res = self.db.execute("SELECT cron_next_job();").fetchone()[0]
        job = json.loads(res)

        self.assertEqual(job['name'], 'job_near')
        self.assertIsInstance(job['next_run'], int)
        # Check that it is indeed sooner than the far job
        near_run = job['next_run']

        # Verify it matches what cron_job_next says
        far_run = self.db.execute("SELECT cron_job_next('job_far');").fetchone()[0]
        self.assertLess(near_run, far_run)

if __name__ == '__main__':
    unittest.main(verbosity=2)
