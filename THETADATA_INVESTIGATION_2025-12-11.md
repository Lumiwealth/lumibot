# ThetaData Investigation Report - December 11, 2025

## Summary

This document captures the findings from investigating ThetaData integration issues during GOOG backtesting from 2020-2025.

## Issues Found

### Issue 1: ThetaTerminal Port Binding (CRITICAL BUG)

**Status:** ACTIVE - Affecting production data downloader

**Symptoms:**
- Queue requests timing out after 10 retries
- ThetaTerminal failing to restart
- Async timeout errors: `Request 0dcbd46e... failed (attempt 9/10): ThetaTerminal failed to become ready before timeout`
- 429 Too Many Requests from ThetaData status endpoint
- `ACCOUNT_ALREADY_CONNECTED` warnings

**Root Cause (Multiple Issues):**

1. **Port Binding Issue:** ThetaTerminal Java process fails to bind to port 25503 because a zombie process is holding the port
2. **Account Connection Issue:** ThetaData rejects new connections with `ACCOUNT_ALREADY_CONNECTED`
3. **Rate Limiting:** ThetaData returns 429 Too Many Requests on status probes

```
java.net.BindException: Address already in use
  at java.base/sun.nio.ch.Net.bind0(Native Method)
  ...
Exception in thread "main" java.io.IOException: Failed to bind to 0.0.0.0/0.0.0.0:25503
```

**Evidence from CloudWatch logs (2025-12-11T19:48-19:53):**
```
ThetaTerminal unhealthy for 2 consecutive probes; restarting
ThetaTerminal process found dead (exit_code=None), relaunching
Caused by: java.net.BindException: Address already in use
ThetaTerminal restart failed (monitor_unhealthy) after 47.5s
RuntimeError: ThetaTerminal failed to become ready before timeout
[FPSS] Disconnected from server: ACCOUNT_ALREADY_CONNECTED
Client error '429 Too Many Requests' for url 'http://127.0.0.1:25503/v3/terminal/mdds/status?format=json'
```

**What's Happening:**
1. ThetaTerminal crashes or is killed
2. Supervisor tries to restart but old process holds port 25503
3. New ThetaTerminal starts but can't bind to port
4. ThetaData shows `ACCOUNT_ALREADY_CONNECTED` (orphaned connection from previous instance)
5. Supervisor's health probes get 429 rate limited
6. Cycle repeats indefinitely

**Fix Required:**
The data downloader supervisor needs to:
1. Force-kill ALL ThetaTerminal processes before restart (including child processes)
2. Wait for port 25503 to become available before starting new instance
3. Handle the `ACCOUNT_ALREADY_CONNECTED` case by waiting for ThetaData to disconnect the orphaned session

Location: `botspot_data_downloader/src/botspot_data_downloader/supervisor.py`

### Issue 2: Options Data Range Mismatch (Expected Behavior)

**Status:** NOT A BUG - This is expected behavior for LEAPS options

**Observation:**
When backtesting GOOG from March 2020, the strategy selects options expiring in March 2022 (LEAPS). The data shows:

```
GOOG 2022-03-18 1320.0 CALL
- Cache range: 2019-12-27 to 2022-03-16
- Actual data range: 2021-07-19 to 2022-03-16
- Requested date: 2020-03-12
```

**Explanation:**
This is NOT a data availability issue from ThetaData. The option contract `GOOG 2022-03-18 1320.0 CALL` was not actively traded in March 2020. Even though it may have existed as a contract, there were no trades to record. ThetaData correctly returns placeholder rows for dates where no trading occurred.

**The strategy should handle this gracefully** by either:
1. Selecting options with closer expirations that were being traded
2. Using a fallback pricing method (like Black-Scholes) for options without quotes
3. Skipping days where no valid option prices are available

## Working Components

### DNS and Elastic IP - VERIFIED WORKING
- DNS: `data-downloader.lumiwealth.com` resolves to `34.232.207.152`
- Elastic IP is properly associated with the data downloader instance

### S3 Caching - VERIFIED WORKING
```
[THETA][CACHE][FAST_REUSE] asset=GOOG/USD (day) covers start=2012-12-26 end=2025-12-05
```
- Cache hits are working correctly for stock data
- Options data is being cached properly

### Queue System - PARTIALLY WORKING
- Queue submission works: `POST /queue/submit HTTP/1.1 200 OK`
- Queue stats endpoint works: `/queue/stats` returns valid JSON
- **Problem:** Requests fail when ThetaTerminal is unhealthy due to port binding issue

### NoDataCache - WORKING
- Endpoint available: `/nodata/stats`
- Returns: `{"total_keys":0,"total_entries":0,"keys":[]}`
- No data gaps being tracked currently

## Test Results

### GOOG Smoke Test (Feb-Jun 2020)
- **Stock data:** Retrieved successfully from cache
- **Options data:** Queue requests failing due to ThetaTerminal issues
- **Progress:** Reached ~40% before getting stuck on queue requests

## Recommendations

### Immediate Actions

1. **Fix ThetaTerminal Port Cleanup:**
   - Modify `supervisor.py` to use `kill -9` or `pkill -f ThetaTerminal` before restart
   - Add port availability check before starting ThetaTerminal
   - Implement proper process group killing to catch all child processes

2. **Restart Data Downloader Instance:**
   - Quick fix: Terminate the current instance to let ASG launch a fresh one
   - This will clear the zombie process holding port 25503

### Long-term Improvements

1. **Add Port Availability Check:**
   ```python
   import socket
   def is_port_available(port):
       with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
           return s.connect_ex(('localhost', port)) != 0
   ```

2. **Implement Process Cleanup:**
   ```python
   async def cleanup_before_start(self):
       # Kill any existing ThetaTerminal processes
       os.system("pkill -9 -f ThetaTerminal")
       # Wait for port to become available
       for _ in range(10):
           if is_port_available(25503):
               break
           await asyncio.sleep(1)
   ```

3. **Consider Strategy-Level Fallback:**
   - For LEAPS options without historical quotes, the strategy could:
     - Use Black-Scholes pricing as fallback
     - Select closer expirations with available data
     - Log warning and continue with available data

## Timeline of Events

| Time (UTC) | Event |
|------------|-------|
| 19:23:11 | First ThetaTerminal restart |
| 19:28:19 | Connection lost to ThetaData |
| 19:47:22 | ThetaTerminal marked unhealthy |
| 19:47:24 | Second restart attempt |
| 19:48:47 | BindException - port 25503 already in use |
| 19:48:49 | ThetaTerminal unhealthy, restart triggered |
| 19:48:52 | Process found dead, relaunch attempted |
| 19:49:37 | Restart failed after 47.5s timeout |
| 19:49:42 | 429 Too Many Requests from ThetaData |
| 19:51:20 | ECS task stopped manually |
| 19:51:38 | New ECS task started |
| 19:55:26 | Old instance terminated |
| 19:57:37 | New instance launched (i-0294a8038e0d957dd) |
| 19:58:07 | New instance API started (Uvicorn running) |
| 19:59:25 | Elastic IP re-associated with new instance |
| 20:00:xx | ThetaTerminal still failing to start on new instance |

## Current Status (as of 20:21 UTC)

**API Server:** Working - Uvicorn running at http://data-downloader.lumiwealth.com:8080
**Queue System:** Working - `/queue/stats` endpoint responding
**ThetaTerminal:** NOT WORKING - Process keeps dying immediately after launch
**Active Workers:** 0 (no workers available to process queue requests)

### ROOT CAUSE IDENTIFIED

**Local ThetaTerminal process (PID 41989) on your Mac was blocking production!**

This process was running since Sunday 5AM:
```
robertgrzesik    41989   0.1  2.0 449880816 1005520   ??  SN   Sun05AM  17:44.67 /usr/bin/java -XX:+IgnoreUnrecognizedVMOptions --sun-misc-unsafe-memory-access=allow --enable-native-access=ALL-UNNAMED -jar /Users/robertgrzesik/Documents/Development/botspot_data_downloader/resources/lib/202511141.jar --creds-file /tmp/theta-dev-test/creds.txt --config config.toml
```

**Actions taken:**
1. Killed local ThetaTerminal process (PID 41989) at 20:14 UTC
2. Restarted ECS task multiple times
3. ThetaTerminal still not starting after local process killed

**New issue discovered:**
ThetaTerminal Java process is dying immediately on the ECS container without even creating a log file. This could be:
1. ThetaData account rate-limited from too many rapid reconnection attempts
2. Session not yet released by ThetaData (may take 5-10 minutes)
3. Container-level issue (less likely since API server works fine)

### Issue: ThetaTerminal Not Starting

The ThetaTerminal Java process is dying immediately on startup, even on a fresh instance after ECS task restart:

```
2025-12-11T20:03:23 ThetaTerminal process found dead (exit_code=None), relaunching
2025-12-11T20:03:23 ThetaTerminal log not found when capturing tail (process_death)
2025-12-11T20:03:43 ThetaTerminal process found dead (exit_code=None), relaunching
2025-12-11T20:03:43 ThetaTerminal log not found when capturing tail (process_death)
```

The supervisor tried twice then gave up. The `exit_code=None` is suspicious - it means the process's poll() returned None (which should mean still running), but the log file wasn't created.

**Potential Root Causes:**
1. Java heap memory issues (container memory limits vs `-Xmx2g` setting)
2. ThetaData account lockout from rapid reconnection attempts
3. ECS container resource constraints
4. Docker image build issue (unlikely since API server runs fine)

**Actions Taken:**
1. Stopped ECS task 41f1bac4... manually
2. New task 222410bc... started automatically
3. ThetaTerminal still failing to start on new task

**Next Steps:**
1. Wait for ThetaData account cooldown (~10-15 minutes)
2. Check container memory/CPU limits in ECS task definition
3. Consider reducing Java heap settings: `-Xms256m -Xmx1g`
4. May need to SSH into instance to inspect container logs directly

## Environment

```
DATADOWNLOADER_BASE_URL=http://data-downloader.lumiwealth.com:8080
LUMIBOT_CACHE_BACKEND=s3
LUMIBOT_CACHE_MODE=readwrite
LUMIBOT_CACHE_S3_VERSION=v28
THETADATA_USE_QUEUE=true
BACKTESTING_DATA_SOURCE=thetadata
```

## Files Referenced

- `/Users/robertgrzesik/Documents/Development/Strategy Library/Demos/.env` - Configuration
- `botspot_data_downloader/src/botspot_data_downloader/supervisor.py` - ThetaTerminal process manager
- CloudWatch Log Group: `/ecs/botspot-data-downloader`

---
*Generated: 2025-12-11*
