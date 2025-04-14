import asyncio
from collections import deque


class AsyncLifoQueue:
    def __init__(self):
        self._queue = deque()
        self._get_event = asyncio.Event()

    async def put(self, item):
        """Put an item in the queue (LIFO order)."""
        self._queue.appendleft(item)
        if len(self._queue) == 1:
            self._get_event.set()

    async def get(self):
        """Get the item from the queue (LIFO order)."""
        if not self._queue:
            await self._get_event.wait()
        item = self._queue.popleft()
        if not self._queue:
            self._get_event.clear()
        return item

    def qsize(self):
        """Returns the current size of the queue."""
        return len(self._queue)

    async def empty(self):
        """Checks if the queue is empty."""
        return len(self._queue) == 0
