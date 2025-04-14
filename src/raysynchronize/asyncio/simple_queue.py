import asyncio
from collections import deque

class AsyncSimpleQueue:
    def __init__(self):
        self._queue = deque()
        self._put_event = asyncio.Event()
        self._get_event = asyncio.Event()

    async def put(self, item):
        """Put an item in the queue."""
        self._queue.append(item)
        # Signal that an item has been added
        if len(self._queue) == 1:
            self._get_event.set()

    async def get(self):
        """Get an item from the queue."""
        # Wait until an item is available
        if not self._queue:
            await self._get_event.wait()
        item = self._queue.popleft()  # O(1) for deque
        # If the queue is empty after popping, reset the get event
        if not self._queue:
            self._get_event.clear()
        return item

    def qsize(self):
        """Returns the current size of the queue."""
        return len(self._queue)

    async def empty(self):
        """Checks if the queue is empty."""
        return len(self._queue) == 0
