"""Reentrant Lock Actor for Ray Synchronization.

This module defines an actor that provides a distributed reentrant
lock mechanism using Ray. The reentrant lock allows the same actor
to acquire the lock multiple times without causing a deadlock.

"""
from beartype import beartype
from beartype.typing import Union
import ray

from raysynchronize.ray_actors.lock_actor import BaseLockActor

@beartype
class BaseRlockActor(BaseLockActor):
    """Distributed Reentrant Lock Actor.

    This class implements a distributed reentrant lock using Ray's remote capabilities.
    It allows actors and tasks to coordinate access to shared resources by acquiring
    and releasing the lock, with support for reentrancy.
    
    """
    def __init__(self) -> None:
        """Initialize the ReentrantLockActor.

        Sets up the initial state of the lock, including the lock count and the owner actor ID.
        """
        super().__init__()
        self._acquire_count: int = 0

    async def try_acquire(
            self,
            caller_id: str
    ) -> Union[bool, Exception]:
        """Acquire the lock.

        If the lock is not held by any actor, or if it is held by
        the same actor that is trying to acquire it (reentrancy),
        then the lock is acquired. Blocks until the lock is available
        and then acquires it. Returns True if the lock was
        successfully acquired.

        Returns:
            bool: True if the lock was acquired, False otherwise.
        """
        try:
            if not self._locked:
                self._locked = True
                self._owner_id = caller_id
                self._acquire_count += 1
                return True
            if self._owner_id == caller_id:
                self._acquire_count += 1
                return True
            return False
        except Exception as e:  # pylint: disable=W0703
            return e  # Return the exception

    async def release(
        self,
        caller_id: str
    ) -> Union[bool, Exception]:
        """Release the lock.

        Decreases the lock count. If the lock count reaches zero, the lock
        is fully released and other actors and tasks can acquire it.
        
        Parameters:
            caller_id (str): The unique id of the process that owns the lock.
        
        Returns:
            bool: True if the lock was released, False otherwise.
        """
        try:
            if not self._locked:
                return RuntimeError("Cannot release an unlocked lock")
            if not self._owner_id == caller_id:
                return RuntimeError("Cannot release lock you do not own")
            self._acquire_count -= 1
            if self._acquire_count == 0:
                self._locked = False
                self._owner_id = None
            return True
        except Exception as e:  # pylint: disable=W0703
            return e  # Return the exception


RayRLockActor = ray.remote(BaseRlockActor)
