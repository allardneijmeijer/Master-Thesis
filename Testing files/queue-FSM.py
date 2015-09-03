import heapq
from sortedcontainers import SortedSet
from transitions import Machine

class FSM(Machine):
    def __init__(self):
        Machine.__init__(self)
        Machine.add_states(self,['operational', 'unscheduled', 'scheduled'])
        Machine.set_state(self,'operational')
        self.add_transition('repair', '*', 'operational')
        self.add_transition('breaku','operational','unscheduled')
        self.add_transition('breaks','operational','scheduled')


class Job(object):
    __slots__ = ['Id', 'a', 's', 't', 'p']
    def __init__(self, Id, a, s, t, p):
        self.Id = Id # id 
        self.a  = a # arrival time
        self.s = s # service time
        self.t = t # job type
        self.p = p # priority

#from collections import namedtuple
#event = namedtuple('Event', ['t', 'type', 'j'])

class Server(object):       # class not used yet
    def __init__(self):
        busy = False
        self.machine = FSM()

class Event(object):
    __slots__ = ['t', 'type', 'j', 'q', 'b']
    def __init__(self, t, type, j):
        self.t = t  # time
        self.type = type  #valid types: a arrrival, d departure, m maintenance, r repair, j is job
        self.j =  j # job
        self.q = 0 # queue length
        self.b = 0 # number of busy servers

class Queue(object):
    def __init__(self):
        self.numServers = None
        self.busyServers = 0
        self.queue = None
        self.logbook = SortedSet(key = lambda e: e.t)
        #self.events = SortedSet(key = lambda e: e.t)
        self.events = []
        self.machine = FSM()    #define state machine for queue server

    def addJobs(self, jobs):
        for j in jobs:
            #self.events.add( Event(t = j.a, type = 'a', j = j))
            heapq.heappush(self.events, (j.a, Event(t = j.a, type = 'a', j = j) ) )
            
    def addMaintenance(self, time):
        Id = len(jobs) + 1
        job = Job(Id,time,1,m,1)
        heapqq.heappush(self.events, (time, Event(t=time, type = 'm', j = job) ) )

    def addRepair(self, t):
        Id = len(jobs) + 1
        job = Job(Id,time,1,r,1)
        heapqq.heappush(self.events, (time, Event(t=time, type = 'r', j = job) ) )
        
    # def completeRepair(self,t)
        

    def run(self):
        while self.events:
            #e = self.events.pop(0)
            e = heapq.heappop(self.events)[1]
            now, job = e.t, e.j  # save the time and the job
            if e.type == 'a': # job arrival
                if self.busyServers < self.numServers: # a server is free
                    #self.events.add( Event(t = now + job.s, type = 'd', j = job ))
                    heapq.heappush(self.events, \
                                   (now + job.s, Event(t = now + job.s, type = 'd', j = job )))
                    self.busyServers += 1 # server becomes busy
                else: # server is occupied
                    self.queue.add(job)
            elif e.type == 'd': # job departure
                if self.queue: # queue not empty
                    j = self.queue.pop(0) # get  next job to work on
                    #self.events.add(Event(t = now+j.s, type = 'd', j = j))
                    heapq.heappush(self.events, (now+j.s, Event(t = now+j.s, type = 'd', j = j)))
                else: # server becomes free as there is no job in queue
                    self.busyServers -= 1 # server is empty
            elif e.type == 'm': # maintenance action
                a = 1 # random crap
            elif e.type == 'r': # repair action
                a = 1 # random crap
            self.log(e)

    # The rest is statistics collection
    def log(self, e):
        e.q = len(self.queue)
        e.b = self.busyServers
        self.logbook.add(e)

    def queueAtArrivalMoments(self):
        return [e.q for e in self.logbook if e.type == "a"]

    def queueAtDepartureMoments(self):
        return [e.q for e in self.logbook if e.type == "d"]

    def arrivalTimes(self):
        return [e.t for e in self.logbook if e.type == "a"]
    
    def departureTimes(self):
        return [e.t for e in self.logbook if e.type == "d"]
        
test = Queue()
print(test.machine.state)
test.machine.breaku()
print(test.machine.state)
test.machine.repair()
print(test.machine.state)
print("piet")

