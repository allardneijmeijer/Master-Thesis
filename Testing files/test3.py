import heapq
from sortedcontainers import SortedSet
from transitions import Machine

class Matter(Machine):
    def say_hello(self): print "hello, new state!"
    def say_goodbye(self): print "goodbye, old state!"

    def __init__(self):
        states = ['solid', 'liquid', 'gas']
        Machine.__init__(self, states, initial='solid')
        self.add_transition('melt', 'solid', 'liquid')

lump = Matter()
lump.state