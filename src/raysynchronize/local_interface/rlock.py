"""Local Interface Reentrant Lock Utility for Ray Synchronization.

This module provides utility functions and classes for implementing
reentrant local locks that can be used to synchronize access to shared resources within a
single process or thread. The primary class defined here is a reentrant lock that can be
used to manage concurrent access.
"""
import warnings

from beartype import beartype
from beartype.typing import Optional
from ray.actor import ActorHandle

from raysynchronize.ray_actors.rlock_actor import RayRLockActor
from raysynchronize.local_interface.lock import RayLock


@beartype
class RayRLock(RayLock):
    """A local reentrant lock wrapper for Ray synchronization.

    This class provides a simple interface to acquire, release, and check the
    status of a distributed reentrant lock using Ray's remote capabilities.
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
        """Initialize the local reentrant lock wrapper."""
        if actor_handle is None:
            actor_handle = RayRLockActor.remote()
        else:
            warnings.warn(
        "You should only provide the actor handle if you're "
                "absolutely sure you know what you're doing.",
                category=UserWarning
            )
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", category=UserWarning)
            super().__init__(
                poll_interval=poll_interval,
                timeout=timeout,
                actor_handle=actor_handle,
            )
