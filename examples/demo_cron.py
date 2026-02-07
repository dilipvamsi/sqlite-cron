import sqlite3
import time
import os
import sys

# Determine extension path based on OS
script_dir = os.path.dirname(os.path.abspath(__file__))
root_dir = os.path.dirname(script_dir)
build_dir = os.path.join(root_dir, "build")

if sys.platform == 'win32' or sys.platform == 'cygwin':
    ext_filename = "sqlite_cron.dll"
elif sys.platform == 'darwin':
    ext_filename = "sqlite_cron.dylib"
else:
    ext_filename = "sqlite_cron.so"

ext_path = os.path.join(build_dir, ext_filename)



if not os.path.exists(ext_path):
    print(f"Error: Extension not found at {ext_path}")
    print("Please run 'make' first.")
    sys.exit(1)

db_path = "demo_cron.db"
if os.path.exists(db_path):
    os.remove(db_path)

print(f"Creating database: {db_path}")
conn = sqlite3.connect(db_path)
conn.enable_load_extension(True)
conn.load_extension(ext_path)

# Initialize in callback mode for this demo
print("Initializing sqlite-cron in callback mode...")
conn.execute("SELECT cron_init('callback')")

# Create a table to log results
conn.execute("CREATE TABLE demo_log (id INTEGER PRIMARY KEY, message TEXT, timestamp DATETIME DEFAULT CURRENT_TIMESTAMP)")

# Schedule jobs using cron expressions
print("\nScheduling jobs...")
# Run every second
conn.execute("SELECT cron_schedule_cron('fast_job', '*/1 * * * * *', \"INSERT INTO demo_log (message) VALUES ('Every Second')\")")
# Run every 5 seconds
conn.execute("SELECT cron_schedule_cron('slow_job', '*/5 * * * * *', \"INSERT INTO demo_log (message) VALUES ('Every 5 Seconds')\")")

print("Jobs scheduled. Waiting for execution (Press Ctrl+C to stop)...")

try:
    for i in range(15):
        time.sleep(1)
        # Trigger callback by running a query
        conn.execute("SELECT 1")

        # Check results
        rows = conn.execute("SELECT timestamp, message FROM demo_log ORDER BY id DESC LIMIT 5").fetchall()
        print(f"\n--- Time: {time.ctime()} ---")
        if not rows:
            print("No logs yet.")
        else:
            for row in rows:
                print(f"[{row[0]}] {row[1]}")

except KeyboardInterrupt:
    print("\nStopping...")

print("\nStopping cron engine...")
conn.execute("SELECT cron_stop()")
conn.close()
os.remove(db_path)
print("Done.")
