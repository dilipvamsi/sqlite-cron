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
        """Create a fresh database with the extension loaded."""
        if not os.path.exists(DB_DIR):
            os.makedirs(DB_DIR)
        self.db_path = os.path.abspath(os.path.join(DB_DIR, "test_cron.db"))
        self._cleanup_db_files()
        
        self.db = sqlite3.connect(self.db_path)
        self.db.execute("PRAGMA journal_mode=WAL;")
        self.db.execute("PRAGMA synchronous=NORMAL;")
        self.db.execute("PRAGMA cache_size=0;")
        self.db.enable_load_extension(True)
        self.db.load_extension(EXTENSION_PATH)
    
    def tearDown(self):
        """Clean up the database."""
        try:
            self.db.execute("SELECT cron_reset();")
        except:
            pass
        try:
            self.db.close()
        except:
            pass
        self._cleanup_db_files()
    
    def _cleanup_db_files(self):
        """Remove database files."""
        for f in [self.db_path, self.db_path + "-wal", self.db_path + "-shm"]:
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
        
        time.sleep(1.2)
        self.db.execute("WITH RECURSIVE cnt(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM cnt WHERE x<100) SELECT count(*) FROM cnt;").fetchone()
        self.db.execute("WITH RECURSIVE cnt(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM cnt WHERE x<100) SELECT count(*) FROM cnt;").fetchone()
        
        res = self.db.execute("SELECT count(*) FROM __cron_log WHERE job_name = 'rate_job';").fetchone()
        self.assertEqual(res[0], 1)
    
    def test_reentrancy_guard(self):
        self.db.execute("SELECT cron_init('callback');")
        self.db.execute("CREATE TABLE IF NOT EXISTS counter (val INTEGER);")
        self.db.execute("INSERT INTO counter VALUES (0);")
        self.db.execute("SELECT cron_schedule('reentrancy_job', 1, 'UPDATE counter SET val = val + 1');")
        
        time.sleep(1.2)
        for _ in range(10):
            self.db.execute("WITH RECURSIVE cnt(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM cnt WHERE x<50) SELECT count(*) FROM cnt;").fetchone()
        
        res = self.db.execute("SELECT val FROM counter;").fetchone()
        self.assertEqual(res[0], 1)
    
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
        
        new_conn = sqlite3.connect(self.db_path)
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
        res = self.db.execute("SELECT val FROM results;").fetchone()
        self.assertIsNotNone(res)
        self.assertEqual(res[0], 2000000)


class TestCloseDuringJob(CronTestBase):
    
    def test_close_during_job(self):
        self.db.execute("SELECT cron_init('thread');")
        self.db.execute("CREATE TABLE IF NOT EXISTS results (val INTEGER);")
        long_query = "WITH RECURSIVE cnt(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM cnt WHERE x<2000000) INSERT INTO results SELECT count(*) FROM cnt"
        self.db.execute("SELECT cron_schedule('long_job', 1, ?);", (long_query,))
        
        time.sleep(1.2)
        self.db.close()
        
        new_conn = sqlite3.connect(self.db_path)
        res = new_conn.execute("SELECT val FROM results;").fetchone()
        self.assertIsNotNone(res)
        self.assertEqual(res[0], 2000000)
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
            new_conn = sqlite3.connect(self.db_path)
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
        
        conn2 = sqlite3.connect(self.db_path)
        conn2.enable_load_extension(True)
        conn2.load_extension(EXTENSION_PATH)
        res = conn2.execute("SELECT cron_init('thread');").fetchone()[0]
        self.assertIn("Ref count increased", res)
        
        self.db.close()
        time.sleep(0.5)
        
        conn2.execute("SELECT cron_schedule('still_alive', 1, 'SELECT 1');")
        time.sleep(1.2)
        res = conn2.execute("SELECT status FROM __cron_log WHERE job_name = 'still_alive';").fetchone()
        self.assertIsNotNone(res)
        
        conn2.close()
        time.sleep(1.2)
        
        new_conn = sqlite3.connect(self.db_path)
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


if __name__ == '__main__':
    unittest.main(verbosity=2)
