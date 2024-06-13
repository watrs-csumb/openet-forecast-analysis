from collections import deque

class Queue:
    def __init__(self) -> None:
        self.items = deque();

    def enqueue(self, value):
        self.items.appendleft(value)