"""
Thread pool manager for pipeline task execution.
Prevents unlimited thread spawning which can exhaust system resources.
"""

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Callable, Tuple

from config import Config

logger = logging.getLogger(__name__)


class PipelineThreadPool:
    """
    Manages thread pool for executing pipeline tasks.
    Limits concurrent execution to prevent resource exhaustion.
    """

    def __init__(self, max_workers: int = None):
        """
        Initialize thread pool.

        Args:
            max_workers: Maximum number of concurrent threads.
                        Defaults to Config.THREAD_POOL_MAX_WORKERS (default: 3)
        """
        self.max_workers = max_workers or Config.THREAD_POOL_MAX_WORKERS
        self.executor = ThreadPoolExecutor(max_workers=self.max_workers)
        logger.info(f"🚀 ThreadPoolExecutor initialized with {self.max_workers} workers")

    def submit_task(self, func: Callable, *args, **kwargs) -> Tuple:
        """
        Submit a task to the thread pool.

        Args:
            func: Callable function to execute
            *args: Positional arguments for func
            **kwargs: Keyword arguments for func

        Returns:
            Future object
        """
        future = self.executor.submit(func, *args, **kwargs)
        logger.info(f"Task submitted: {func.__name__}")
        return future

    def shutdown(self, wait: bool = True):
        """
        Shutdown the thread pool.

        Args:
            wait: If True, wait for all tasks to complete before shutting down
        """
        logger.info(f"Shutting down ThreadPoolExecutor (wait={wait})")
        self.executor.shutdown(wait=wait)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.shutdown(wait=True)


# Global thread pool instance
_pipeline_thread_pool = None


def get_thread_pool() -> PipelineThreadPool:
    """Get or create the global thread pool instance."""
    global _pipeline_thread_pool
    if _pipeline_thread_pool is None:
        _pipeline_thread_pool = PipelineThreadPool()
    return _pipeline_thread_pool


def shutdown_thread_pool():
    """Shutdown the global thread pool."""
    global _pipeline_thread_pool
    if _pipeline_thread_pool is not None:
        _pipeline_thread_pool.shutdown(wait=True)
        _pipeline_thread_pool = None
