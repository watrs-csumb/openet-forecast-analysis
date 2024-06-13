from collections import deque

 # Similar to internet examples but with additional C++ std::queue features
class Queue:
    # Constructor
    def __init__(self) -> None:
        self.items = deque();

	# Modifiers
    def enqueue(self, value):
        self.items.appendleft(value)
        
    def dequeue(self):
        return self.items.pop()
    
    # Capacity
    def is_empty(self):
        return len(self.items) == 0
    
    def size(self):
        return len(self.items)
    
    # Element access
    def front(self):
        return self.items[0]
    
    def back(self):
        return self.items[-1]