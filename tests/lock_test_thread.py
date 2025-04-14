import threading
import time

import numpy as np
from beartype import beartype
from beartype.typing import Optional

from raysynchronize.local_interface.lock import RayLock


@beartype
class LockTestThread(threading.Thread):
    _random: np.random.Generator = np.random.default_rng(int(time.time() * 1000) % 2**32)
    def __init__(
        self,
        lock: RayLock,
        sleep_time: Optional[float]=None
    ):
        super().__init__()
        self._lock_to_test: RayLock = lock
        if sleep_time is not None:
            if sleep_time <= 0:
                raise ValueError(f"sleep_time must be greater than 0. received: {sleep_time}")
            self._sleep_time: float = sleep_time
        else:
            inclusive_high = np.nextafter(5.0, float('inf'))
            self._sleep_time = self._random.uniform(low=0.1, high=inclusive_high)
        self._stop_event: threading.Event = threading.Event()
        self._we_have_the_lock = False

    def attempt_acquire(self):
        self._we_have_the_lock = self._lock_to_test.acquire()
        if self._we_have_the_lock:
            print(f"Thread: {self.ident} acquired the lock")

    def attempt_release(self):
        self._we_have_the_lock = not self._lock_to_test.release()
        if not self._we_have_the_lock:
            print(f"Thread: {self.ident} released the lock")

    def toggle_lock(self):
        if not self._we_have_the_lock:
            self.attempt_acquire()
        else:
            self.attempt_release()

    def run(self):
        while not self._stop_event.is_set():
            time.sleep(self._sleep_time)
            self.toggle_lock()
        while self._we_have_the_lock:
            self.attempt_release()

    def stop(self):
        self._stop_event.set()

    def stopped(self):
        return self._stop_event.is_set() and not self.is_alive()
