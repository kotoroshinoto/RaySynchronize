"""Utility Functions for Ray Synchronization.

This module provides utility functions that assist in managing and generating
unique identifiers within the Ray environment. The primary function defined here
is `get_unique_ray_id`, which generates a unique identifier string combining
various contextual information.

"""
import threading

import ray
try:
    # noinspection PyUnresolvedReferences
    import ray.serve
    from ray.serve.exceptions import RayServeException
    HAVE_RAY_SERVE = True
except ImportError:
    RayServeException = None
    HAVE_RAY_SERVE = False

from raysynchronize.ray_utils.ray_logging_filter import suppress_ray_runtime_context_warning_logs


def get_unique_ray_id() -> str:
    """Generate a unique identifier string for the current Ray context.

    This function generates a unique identifier string that combines various
    contextual information from the Ray environment, including node ID, worker ID,
    job ID, task ID, actor name and ID, deployment name and replica tag (if available),
    and thread identifier. The purpose of this identifier is to provide a comprehensive
    and unique representation of the current execution context within the Ray system.

    Returns:
        str: A unique identifier string combining various contextual information from the
            Ray environment.

    Raises:
        RuntimeError: If Ray is not initialized when trying to get the unique Ray ID.
    
    """
    with suppress_ray_runtime_context_warning_logs():
        if HAVE_RAY_SERVE:
            try:
                ray_serve_context = ray.serve.get_replica_context()
                deployment_uid = (
                    f"{ray_serve_context.app_name}_"
                    f"{ray_serve_context.deployment}_"
                    f"{ray_serve_context.replica_tag}"
                )
            except RayServeException:
                deployment_uid = str(None)
        else:
            deployment_uid = str(None)
        try:
            context = ray.get_runtime_context()
        except AssertionError as e:
            raise RuntimeError("Tried to get unique Ray ID, but Ray is not initialized") from e
        try:
            actor_id = context.get_actor_id()
            actor_name = context.get_actor_name()
            actor_uid = f"{actor_name}_{actor_id}"
        except AssertionError:
            actor_uid = str(None)
        try:
            task_id = context.get_task_id()
            task_name = context.get_task_name()
            task_uid = f"{task_name}_{task_id}"
        except AssertionError:
            task_uid = str(None)
        try:
            job_uid = context.get_job_id()
        except AssertionError:
            job_uid = str(None)
        try:
            node_uid = context.get_node_id()
        except AssertionError:
            node_uid = str(None)
        try:
            worker_uid = context.get_worker_id()
        except AssertionError:
            worker_uid = str(None)
        # noinspection PyBroadException
        try:
            thread_uid = threading.get_ident()
        except Exception:  # pylint: disable=W0703
            thread_uid = str(None)
        return (
            f"{node_uid}_{worker_uid}_{job_uid}_{task_uid}_"
            f"{actor_uid}_{deployment_uid}_{thread_uid}"
        )
