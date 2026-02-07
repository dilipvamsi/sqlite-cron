# Simple Makefile for sqlite-cron
CC = gcc
CC_WIN = x86_64-w64-mingw32-gcc
CFLAGS = -g -fPIC -O3 -DTEST_MODE -Iheaders

LDFLAGS_LINUX = -shared -lpthread
LDFLAGS_MAC = -dynamiclib
LDFLAGS_WIN = -shared
BUILD_DIR = build

all:
	@echo "Usage:"
	@echo "  make linux"
	@echo "  make macos"
	@echo "  make windows"

$(BUILD_DIR):
	mkdir -p $(BUILD_DIR)

linux: $(BUILD_DIR)
	$(CC) $(CFLAGS) $(COVERAGE) src/sqlite_cron.c -o $(BUILD_DIR)/sqlite_cron.so $(LDFLAGS_LINUX)

macos: $(BUILD_DIR)
	$(CC) $(CFLAGS) src/sqlite_cron.c -o $(BUILD_DIR)/sqlite_cron.dylib $(LDFLAGS_MAC)

windows: $(BUILD_DIR)
	$(CC) $(CFLAGS) src/sqlite_cron.c -o $(BUILD_DIR)/sqlite_cron.dll $(LDFLAGS_WIN)

# Cross-compile Windows DLL on Linux
windows-cross: $(BUILD_DIR) download-headers
	$(CC_WIN) $(CFLAGS) -DTEST_MODE src/sqlite_cron.c -o $(BUILD_DIR)/sqlite_cron.dll $(LDFLAGS_WIN)

download-headers:
	@if [ -f headers/sqlite3.h ] && [ -f headers/sqlite3ext.h ]; then \
		echo "SQLite headers already exist, skipping download."; \
	else \
		echo "Downloading SQLite headers..."; \
		mkdir -p headers; \
		curl -L https://www.sqlite.org/2023/sqlite-amalgamation-3420000.zip -o sqlite.zip; \
		unzip -o sqlite.zip; \
		mv sqlite-amalgamation-3420000/sqlite3.h sqlite-amalgamation-3420000/sqlite3ext.h headers/; \
		rm -rf sqlite-amalgamation-3420000 sqlite.zip; \
		echo "SQLite headers downloaded."; \
	fi

test: linux
	python -m unittest tests.test_cron -v

# Use pytest if preferred (requires pip install pytest)
test-pytest: linux
	pytest tests/test_cron.py

test-windows: windows-cross
	python -m unittest tests.test_cron -v


leak-check: linux download-headers
	$(CC) $(CFLAGS) tests/leak_check.c -o $(BUILD_DIR)/leak_check -lsqlite3

	valgrind --leak-check=full --show-leak-kinds=all --error-exitcode=1 ./$(BUILD_DIR)/leak_check
	rm -f $(BUILD_DIR)/leak_check

leak-check-windows: windows-cross
	$(CC_WIN) $(CFLAGS) -Isrc tests/leak_check.c -o $(BUILD_DIR)/leak_check.exe

	wine ./$(BUILD_DIR)/leak_check.exe
	rm -f $(BUILD_DIR)/leak_check.exe

coverage: clean $(BUILD_DIR) download-headers
	$(CC) $(CFLAGS) -DTEST_MODE -fprofile-arcs -ftest-coverage -c src/sqlite_cron.c -o $(BUILD_DIR)/sqlite_cron.o
	$(CC) $(BUILD_DIR)/sqlite_cron.o -o $(BUILD_DIR)/sqlite_cron.so $(LDFLAGS_LINUX) -lgcov
	python -m unittest tests.test_cron -v
	gcov -o $(BUILD_DIR) src/sqlite_cron.c
	mv sqlite_cron.c.gcov $(BUILD_DIR)/
	@echo "Coverage report generated in $(BUILD_DIR)/sqlite_cron.c.gcov"

clean:
	rm -rf $(BUILD_DIR) databases
	rm -f *.gcda *.gcno *.gcov test_cron.db* leak_test.db*

# Wine-based Windows testing (requires wine and mingw)
test-wine: windows-cross
	@echo "Testing Windows DLL under Wine..."
	cd $(BUILD_DIR) && wine sqlite3.exe :memory: ".load sqlite_cron" \
		"SELECT cron_init('callback');" \
		"SELECT cron_schedule('wine_test', 60, 'SELECT 1');" \
		"SELECT * FROM __cron_jobs;" \
		"SELECT cron_stop();" 2>&1 | grep -v "libEGL\|pci id\|fixme"
	@echo "Wine test passed!"

# Install Windows Python in Wine (one-time setup)
PYTHON_WIN_URL = https://www.python.org/ftp/python/3.14.3/python-3.14.3-amd64.exe
install-wine-python:
	@if wine python --version 2>/dev/null | grep -q "Python 3.14"; then \
		echo "Python 3.14 is already installed in Wine:"; \
		wine python --version; \
		echo "Run 'make uninstall-wine-python' first to reinstall."; \
	else \
		echo "Downloading Windows Python 3.14..."; \
		curl -L -o /tmp/python-win.exe $(PYTHON_WIN_URL); \
		echo "Installing Python in Wine (this may take a few minutes)..."; \
		wine /tmp/python-win.exe /quiet InstallAllUsers=1 PrependPath=1; \
		echo "Verifying Python installation..."; \
		wine python --version; \
		echo "Windows Python installed successfully!"; \
	fi

# Uninstall Windows Python from Wine
uninstall-wine-python:
	@echo "Uninstalling Windows Python from Wine..."
	@if [ -d "$$HOME/.wine/drive_c/Program Files/Python314" ]; then \
		rm -rf "$$HOME/.wine/drive_c/Program Files/Python314"; \
		echo "Removed Python 3.14 installation"; \
	fi
	@if [ -d "$$HOME/.wine/drive_c/users/$$USER/AppData/Local/Programs/Python" ]; then \
		rm -rf "$$HOME/.wine/drive_c/users/$$USER/AppData/Local/Programs/Python"; \
		echo "Removed user Python installation"; \
	fi
	@echo "Windows Python uninstalled from Wine!"

# Download Windows SQLite tools for Wine testing
download-sqlite-win:
	@if [ -f $(BUILD_DIR)/sqlite3.exe ]; then \
		echo "Windows SQLite tools already exist, skipping download."; \
	else \
		echo "Downloading Windows SQLite tools..."; \
		curl -L -o /tmp/sqlite-tools.zip "https://www.sqlite.org/2024/sqlite-tools-win-x64-3470200.zip"; \
		unzip -o /tmp/sqlite-tools.zip -d $(BUILD_DIR)/; \
		echo "Windows SQLite tools installed!"; \
	fi

# Full Python test suite under Wine (requires install-wine-python first)
test-wine-full: windows-cross
	@echo "Running full Python test suite under Wine..."
	@if wine python --version 2>/dev/null | grep -q Python; then \
		WINEDEBUG=-all wine python -m unittest tests.test_cron -v; \
	else \
		echo "ERROR: Windows Python not installed. Run 'make install-wine-python' first."; \
		exit 1; \
	fi

# Quick Wine smoke test (no Python required, uses sqlite3.exe)
test-wine-quick: windows-cross download-sqlite-win
	@echo "Running quick Wine smoke tests..."
	@echo "=== Testing Callback Mode ==="
	cd $(BUILD_DIR) && WINEDEBUG=-all wine sqlite3.exe test_callback.db \
		".load sqlite_cron" \
		"SELECT cron_init('callback');" \
		"SELECT cron_schedule('job1', 60, 'SELECT 1');" \
		"SELECT cron_list();" \
		"SELECT cron_pause('job1');" \
		"SELECT cron_resume('job1');" \
		"SELECT cron_update('job1', 120);" \
		"SELECT cron_get('job1');" \
		"SELECT cron_delete('job1');" \
		"SELECT cron_stop();"
	@echo "=== Testing Thread Mode ==="
	@cd $(BUILD_DIR) && WINEDEBUG=-all wine sqlite3.exe test_thread.db \
		".load sqlite_cron" \
		"SELECT cron_init('thread');" \
		"SELECT cron_stop();" && echo "Thread mode test passed!"

	@echo "=== Testing Invalid Mode Error ==="
	cd $(BUILD_DIR) && WINEDEBUG=-all wine sqlite3.exe :memory: \
		".load sqlite_cron" \
		"SELECT cron_init('invalid');" 2>&1 | grep -q "Unknown mode" && echo "Invalid mode correctly rejected!"
	@echo "All Wine smoke tests passed!"

# Comprehensive Wine coverage (uses Python if available, falls back to sqlite3.exe)
coverage-wine: windows-cross download-sqlite-win
	@echo "Running Windows coverage tests under Wine..."
	@if wine python --version 2>/dev/null | grep -q Python; then \
		echo "Using Windows Python for full test coverage..."; \
		WINEDEBUG=-all wine python -m unittest tests.test_cron -v; \
	else \
		echo "Windows Python not installed. Using sqlite3.exe for basic coverage..."; \
		$(MAKE) test-wine-quick; \
	fi
	@echo "Windows coverage tests completed!"
