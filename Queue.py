from collections import deque

 # Similar to internet examples but with additional C++ std::queue features
class Queue:
    # Constructor
    def __init__(self) -> None:
        self.items = deque()
    
    def __init__(self, values: list) -> None:
        self.items = deque()
        self.enqueue(values)

	# Modifiers
    def enqueue(self, value: any) -> None:
        if type(value) is not list:
            self.items.appendleft(value)
        else:
            for item in value:
                self.enqueue(item)
        
    def dequeue(self) -> any:
        return self.items.popleft()
    
    # Capacity
    def is_empty(self) -> bool:
        return len(self.items) == 0
    
    def size(self) -> int:
        return len(self.items)
    
    # Element access
    def front(self) -> any:
        return self.items[0]
    
    def back(self) -> any:
        return self.items[-1]