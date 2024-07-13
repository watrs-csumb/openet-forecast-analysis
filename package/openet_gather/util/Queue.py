from collections import deque

 # Similar to internet examples but with additional C++ std::queue features
class Queue:
    # Constructor
    def __init__(self) -> None:
        self.items = deque()
    
    def __init__(self, values: list) -> None:
        '''Creates Queue object and adds each item from the list to the queue individually.'''
        self.items = deque()
        self.enqueue(values)

	# Modifiers
    def enqueue(self, value: any) -> None:
        '''Adds value to queue. If the value is a list, it will add each item individually to the queue.'''
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