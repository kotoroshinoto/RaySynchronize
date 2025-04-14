"""Lock Actor for Ray Synchronization.

This module defines an actor that provides a distributed lock mechanism using Ray.
The lock can be used to synchronize access to shared resources across multiple Ray actors and tasks.

"""
import asyncio
from abc import ABC

from beartype import beartype
from beartype.typing import Optional, Union
import ray


@beartype
class BaseLockActor(ABC):
    """Distributed Lock Actor.

    This class implements a distributed lock using Ray's remote capabilities.
    It allows multiple actors and tasks to coordinate access to shared resources by
    acquiring and releasing the lock.
    
    """
    def __init__(self):
        """Initialize the LockActor.

        Sets up the initial state of the lock, which is not acquired.
        """
        self._locked: bool = False
        self._owner_id: Optional[str] = None

    async def acquire(
            self,
            caller_id: str,
            blocking:bool=True,
            timeout:Optional[float]=None
    ) -> Union[bool, Exception]:
        """Acquire the lock.
        
        Blocks until the lock is available and then acquires it. Returns True if the lock was
        successfully acquired.
        
        Parameters:
            caller_id: The id of the calling actor.
            blocking: If True, block the acquisition until the lock is available.
            timeout: If specified, raise an exception if the lock cannot be acquired.

        Returns:
            Union[bool, Exception]: True if the lock was acquired, False otherwise.
                If an exception is raised, the exception will be returned for ray.get or await to
                raise.
        """
        try:
            if not blocking:
                return await self.try_acquire(caller_id=caller_id)
            loop = asyncio.get_event_loop()
            start = loop.time()
            while True:
                if await self.try_acquire(caller_id=caller_id):
                    return True
                if timeout is not None and (loop.time() - start) >= timeout:
                    return False
                await asyncio.sleep(0.05)

        except Exception as e:  # pylint: disable=W0703
            return e  # Return the exception

    async def try_acquire(self, caller_id: str) -> Union[bool, Exception]:
        """Non-blocking attempt to acquire the lock."""
        try:
            if not self._locked:
                self._locked = True
                self._owner_id = caller_id
                return True
            return False
        except Exception as e:  # pylint: disable=W0703
            return e  # Return the exception

    async def release(self, caller_id: str) -> Union[bool, Exception]:
        """Release the lock with exception handling."""
        try:
            if not self._locked:
                return RuntimeError("Cannot release an unlocked lock")
            if not self._owner_id == caller_id:
                return RuntimeError("Cannot release lock you do not own")
            self._locked = False
            self._owner_id = None
            return True
        except Exception as e:  # pylint: disable=W0703
            return e  # Return the exception

    async def locked(self) -> Union[bool, Exception]:
        """Check if the lock is currently held."""
        try:
            return self._locked
        except Exception as e:  # pylint: disable=W0703
            return e  # Return the exception


RayLockActor = ray.remote(BaseLockActor)
