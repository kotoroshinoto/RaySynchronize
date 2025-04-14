import asyncio
import heapq


class AsyncPriorityQueue:
    def __init__(self):
        self._queue = []
        self._get_event = asyncio.Event()

    async def put(self, item, priority):
        """Put an item in the queue with the given priority."""
        # The heapq module uses a min-heap, so we use
        # negative priority to simulate max-heap behavior if needed
        heapq.heappush(self._queue, (priority, item))
        # Signal that an item is available to get
        if len(self._queue) == 1:
            self._get_event.set()

    async def get(self):
        """Get the item with the highest priority (lowest number)."""
        if not self._queue:
            await self._get_event.wait()
        _, item = heapq.heappop(self._queue)
        if not self._queue:
            self._get_event.clear()
        return item

    def qsize(self):
        """Returns the current size of the queue."""
        return len(self._queue)

    async def empty(self):
        """Checks if the queue is empty."""
        return len(self._queue) == 0
