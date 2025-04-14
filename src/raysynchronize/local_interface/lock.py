"""Local Interface Lock Utility for Ray Synchronization.

This module provides utility functions and classes for implementing
local locks that can be used to synchronize access to shared resources within a
single process or thread. The primary class defined here is a simple lock that can be
used to manage concurrent access.

"""
import warnings

import ray
from beartype import beartype
from beartype.typing import Optional
from ray.actor import ActorHandle

from raysynchronize.ray_actors.lock_actor import RayLockActor
from raysynchronize.ray_utils.unique_id import get_unique_ray_id


@beartype
class RayLock:
    """
    A local lock wrapper for Ray synchronization.

    This class provides a simple interface to acquire, release, and
    check the status of a distributed lock using Ray's remote capabilities.
    It can be used to synchronize access to shared resources within a single
    process or thread.

    Args:
        poll_interval (float): The interval at which the lock should be polled for
            availability. Defaults to 0.1 seconds.
        timeout (Optional[float]): The maximum time to wait for acquiring the lock.
            If None, it will block indefinitely. Defaults to None.
        actor_handle (Optional[ActorHandle]): An optional pre-existing Ray actor handle.
            Use this only if you are sure about what you're doing. Defaults to None.
            
    """

    def __init__(
        self,
        poll_interval: float=0.1,
        timeout:Optional[float]=None,
        actor_handle: Optional[ActorHandle] = None,
    ) -> None:
        """Initialize the local lock wrapper."""
        if actor_handle is None:
            self._actor: ActorHandle = RayLockActor.remote()
        else:
            warnings.warn(
                "You should only provide the actor handle if you're "
                "absolutely sure you know what you're doing.",
                category=UserWarning
            )
            self._actor: ActorHandle = actor_handle
        self._poll = poll_interval
        self._timeout = timeout

    def acquire(
        self,
        blocking: bool=True,
        timeout: Optional[float]=None
    ) -> bool:
        """Acquire the lock synchronously.

        This method attempts to acquire the lock. If `blocking` is set to True,
        it will block until the lock is acquired or the specified timeout expires.
        If `blocking` is False, it will return immediately with a boolean indicating
        whether the lock was acquired.

        Args:
            blocking (bool): Whether to wait for the lock if it's not available. Defaults to True.
            timeout (Optional[float]): The maximum time to wait for acquiring the lock.
                If None, it will block indefinitely. Defaults to None.

        Returns:
            bool: True if the lock was acquired, False otherwise.
            
        """
        caller_id = get_unique_ray_id()
        object_ref = self._actor.acquire.remote(
            caller_id=caller_id,
            blocking=blocking,
            timeout=timeout
        )
        return ray.get(object_ref)

    def try_acquire(self) -> bool:
        """Try to acquire the lock without waiting.
        
        This method attempts to acquire the lock immediately.
        If the lock is not available, it returns False.

        Returns:
            bool: True if the lock was acquired, False otherwise.
            
        """
        caller_id = get_unique_ray_id()
        object_ref = self._actor.try_acquire.remote(
            caller_id=caller_id
        )
        return ray.get(object_ref)

    def release(self) -> bool:
        """Release the lock synchronously.

        This method releases the lock, allowing other threads to acquire it.

        Returns:
            bool: True if the lock was released successfully, False otherwise.
            
        """
        caller_id = get_unique_ray_id()
        object_ref = self._actor.release.remote(
            caller_id=caller_id
        )
        return ray.get(object_ref)

    def locked(self) -> bool:
        """Check if the lock is currently held.

        This method checks whether the lock is currently held by any thread.

        Returns:
            bool: True if the lock is held, False otherwise.
            
        """
        object_ref = self._actor.locked.remote()
        return ray.get(object_ref)

    async def async_acquire(
        self,
        blocking: bool=True,
        timeout: Optional[float]=None
    ) -> bool:
        """Acquire the lock asynchronously.

        This method attempts to acquire the lock asynchronously.
        If `blocking` is set to True, it will wait until the lock is acquired or
        the specified timeout expires. If `blocking` is False, it will return
        immediately with a boolean indicating whether the lock was acquired.

        Args:
            blocking (bool): Whether to wait for the lock if it's not available. Defaults to True.
            timeout (Optional[float]): The maximum time to wait for acquiring the lock.
                If None, it will block indefinitely. Defaults to None.

        Returns:
            bool: True if the lock was acquired, False otherwise.
            
        """
        caller_id = get_unique_ray_id()
        object_ref = self._actor.acquire.remote(
            caller_id=caller_id,
            blocking=blocking,
            timeout=timeout
        )
        return await object_ref

    async def async_try_acquire(self) -> bool:
        """Try to acquire the lock asynchronously without waiting.

        This method attempts to acquire the lock immediately asynchronously.
        If the lock is not available, it returns False.

        Returns:
            bool: True if the lock was acquired, False otherwise.
            
        """
        caller_id = get_unique_ray_id()
        object_ref = self._actor.try_acquire.remote(
                caller_id=caller_id
        )
        return await object_ref

    async def async_release(self) -> bool:
        """Release the lock asynchronously.

        This method releases the lock asynchronously, allowing other threads to acquire it.

        Returns:
            bool: True if the lock was released successfully, False otherwise.
            
        """
        caller_id = get_unique_ray_id()
        object_ref = self._actor.release.remote(
            caller_id=caller_id
        )
        return await object_ref

    async def async_locked(self) -> bool:
        """Check if the lock is currently held asynchronously.

        This method checks whether the lock is currently held by any thread asynchronously.

        Returns:
            bool: True if the lock is held, False otherwise.
        
        """
        object_ref = self._actor.locked.remote()
        return await object_ref
