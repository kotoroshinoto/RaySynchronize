import time
import threading
from beartype import beartype
from beartype.typing import List

from raysynchronize.local_interface.rlock import RayRLock
from .rlock_test_thread import RLockTestThread


@beartype
def test_concurrent_rlock_threads():
    num_threads = 10  # Number of threads to run concurrently
    duration = 60  # Duration in seconds for which the test should run

    rlock = RayRLock()
    threads: List[RLockTestThread] = []

    def start_thread():
        _thread = RLockTestThread(rlock)
        _thread.start()
        threads.append(_thread)

    # Start all threads
    for _ in range(num_threads):
        start_thread()

    try:
        # Run the test for the specified duration
        time.sleep(duration)
    finally:
        # Stop all threads
        for thread in threads:
            thread.stop()

        # Wait for all threads to finish
        for thread in threads:
            thread.join()

    # Check for any exceptions or deadlocks
    for thread in threads:
        if not thread.stopped():
            raise threading.ThreadError(f"Thread {thread.ident} did not stop properly")
