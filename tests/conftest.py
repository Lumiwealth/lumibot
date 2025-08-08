"""
Pytest configuration and fixtures for LumiBot tests.
Includes global cleanup for APScheduler instances to prevent CI hangs.
"""

import pytest
import gc
import atexit
import threading


def cleanup_all_schedulers():
    """Emergency cleanup for any remaining scheduler instances"""
    try:
        # Force garbage collection to trigger __del__ methods
        gc.collect()
        
        # Try to find and shutdown any remaining APScheduler instances
        for obj in gc.get_objects():
            if hasattr(obj, '__class__') and 'scheduler' in str(obj.__class__).lower():
                if hasattr(obj, 'shutdown') and hasattr(obj, 'running'):
                    try:
                        if obj.running:
                            if hasattr(obj, 'remove_all_jobs'):
                                obj.remove_all_jobs()
                            obj.shutdown(wait=False)
                    except Exception:
                        pass
    except Exception:
        pass


def cleanup_all_threads():
    """Clean up any remaining threads that might be hanging"""
    try:
        # Get all active threads
        active_threads = threading.enumerate()
        main_thread = threading.main_thread()
        
        for thread in active_threads:
            if thread != main_thread and thread.is_alive():
                # Try to stop threads that have a stop method or event
                if hasattr(thread, 'stop'):
                    try:
                        thread.stop()
                    except Exception:
                        pass
                elif hasattr(thread, '_stop_event'):
                    try:
                        thread._stop_event.set()
                    except Exception:
                        pass
    except Exception:
        pass


@pytest.fixture(scope="session", autouse=True)
def global_cleanup():
    """Global cleanup fixture that runs at session start and end"""
    
    # Cleanup before tests start
    cleanup_all_schedulers()
    cleanup_all_threads()
    
    yield
    
    # Cleanup after all tests complete
    cleanup_all_schedulers()
    cleanup_all_threads()
    
    # Force final garbage collection
    gc.collect()


@pytest.fixture(autouse=True)
def test_cleanup():
    """Per-test cleanup to prevent scheduler leaks between tests"""
    yield
    
    # Minimal cleanup to avoid CI deadlocks
    # Only force gc collection, don't do aggressive scheduler cleanup per-test
    gc.collect()


# Register cleanup functions to run on exit
atexit.register(cleanup_all_schedulers)
atexit.register(cleanup_all_threads)