
import heapq
from sortedcontainers import SortedSet

class Job(object):
    __slots__ = ['Id', 'a', 's', 'p']
    
    def __init__(self, Id, a, s, p):
        self.Id = Id # id 
        self.a  = a # arrival time
        self.s = s # service time
        self.p = p # priority

#from collections import namedtuple
#event = namedtuple('Event', ['t', 'type', 'j'])

class Event(object):
    __slots__ = ['t', 'type', 'j', 'q', 'b']
    def __init__(self, t, type, j):
        self.t = t  # time
        self.type = type  #valid types: a arrrival, d departure, j is job
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

    def addJobs(self, jobs):
        for j in jobs:
            #self.events.add( Event(t = j.a, type = 'a', j = j))
            heapq.heappush(self.events, (j.a, Event(t = j.a, type = 'a', j = j) ) )
            print(j.a)

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

class Fifo(Queue):
    def __init__(self, numServers = 1):
        super(Fifo, self).__init__()
        self.numServers = numServers
        self.queue = SortedSet(key = lambda job: job.a)

class Lifo(Queue):
    def __init__(self, numServers = 1):
        super(Lifo, self).__init__()
        self.numServers = numServers
        self.queue = SortedSet(key = lambda job: -job.a)

class SPTF(Queue):
    def __init__(self, numServers = 1):
        super(SPTF, self).__init__()
        self.numServers = numServers
        self.queue = SortedSet(key = lambda job: job.s)

class LPTF(Queue):
    def __init__(self, numServers = 1):
        super(LPFT, self).__init__()
        self.numServers = numServers
        self.queue = SortedSet(key = lambda job: -job.s)

class Priority(Queue):
    def __init__(self, numServers = 1):
        super(Priority, self).__init__()
        self.numServers = numServers
        self.queue = SortedSet(key = lambda job: job.p)


    import  numpy as np
    from scipy.stats import expon
    import matplotlib.pylab as plt

    np.random.seed(3)

    def makeJobs(num, F, G):
        jobs = set()
        A = 0 # arrival times
        for i in range(num):
            A += F.rvs()
            s = G.rvs()
            jobs.add( Job(i, A, s, p=1) )
        return jobs

    labda = 2.
    mu = 2.1
    F = expon(scale = 1./labda)
    G = expon(scale = 1./mu)

    print(F.mean())
    print(G.mean())

    fifo = Fifo()

    numJobs = 400
    jobs = makeJobs(numJobs, F, G)
    fifo.addJobs(jobs)

    fifo.run()


    qa = fifo.arrivalTimes()
    qd = fifo.departureTimes()
    
    plt.plot(qa)
    plt.plot(qd)
    plt.show()

    qa = fifo.queueAtArrivalMoments()
    qd = fifo.queueAtDepartureMoments()

    print(np.average(qa))
    print(np.average(qd))
    
    plt.plot(qa)
    plt.plot(qd)
    plt.show()

