import threading
import time

import numpy as np
from beartype import beartype
from beartype.typing import Optional

from raysynchronize.local_interface.rlock import RayRLock


@beartype
class RLockTestThread(threading.Thread):
    _random: np.random.Generator = np.random.default_rng(int(time.time() * 1000) % 2**32)
    def __init__(
        self,
        rlock: RayRLock,
        acquire_depth: Optional[int] = None,
        sleep_time: Optional[float] = None
    ):
        super().__init__()
        self._lock_to_test: RayRLock = rlock
        if acquire_depth is not None:
            if acquire_depth <= 0:
                raise ValueError("acquire_depth must be greater than 0")
            self._target_acquire_depth: int = acquire_depth
        else:
            self._target_acquire_depth: int = self._random.integers(low=1, high=5, endpoint=True)
        self._current_acquire_depth: int = 0
        if sleep_time is not None:
            if sleep_time <= 0:
                raise ValueError(f"sleep_time must be greater than 0. received: {sleep_time}")
            self._sleep_time: float = sleep_time
        else:
            inclusive_high = np.nextafter(5.0, float('inf'))
            self._sleep_time = self._random.uniform(low=0.1, high=inclusive_high)
        self._stop_event: threading.Event = threading.Event()

    def attempt_acquire(self):
        failed_attempts = 0
        def do_the_acquire()->None:
            nonlocal failed_attempts
            success = self._lock_to_test.acquire()
            if success:
                self._current_acquire_depth += 1
            else:
                failed_attempts += 1
        while self._current_acquire_depth < self._target_acquire_depth and failed_attempts < 3:
            do_the_acquire()
        if self._current_acquire_depth > 0:
            print(f"Thread: {self.ident} acquired the lock {self._current_acquire_depth} times")

    def attempt_release(self):
        initial_depth = self._current_acquire_depth
        failed_attempts = 0
        def do_the_release()->None:
            nonlocal failed_attempts
            success = self._lock_to_test.release()
            if success:
                self._current_acquire_depth -= 1
            else:
                failed_attempts += 1
        while self._current_acquire_depth > 0:
            do_the_release()
        if self._current_acquire_depth < initial_depth:
            depth_change = initial_depth - self._current_acquire_depth
            print(f"Thread: {self.ident} released the lock {depth_change} times")

    def toggle_lock(self):
        if self._current_acquire_depth == 0:
            self.attempt_acquire()
        else:
            self.attempt_release()

    def run(self):
        while not self._stop_event.is_set():
            time.sleep(self._sleep_time)
            self.toggle_lock()
        while self._current_acquire_depth > 0:
            self.attempt_release()

    def stop(self):
        self._stop_event.set()

    def stopped(self):
        return self._stop_event.is_set() and not self.is_alive()
