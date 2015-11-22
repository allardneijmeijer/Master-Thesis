from collections import defaultdict
from sortedcontainers import SortedSet
import numpy as np

class Event:
    __slots__ = ['f', 't', 'time', 'job', 'event']
    
    def __init__(self, f, t, time, job = None, event = ""):
        self.f = f  # from node
        self.t = t  # to node
        self.time = time # time to deliver the message
        self.job = job 
        self.event = event

    def __repr__(self):
        return "%10s %10s %7.3f\t%s".format(self.f, self.t, self.time, self.event)


class Scheduler(SortedSet):
    def __init__(self):
        super().__init__(key = lambda m: m.time)
        self.endOfSimultationTime = np.inf
        self._now = 0.

    def now(self):
        return self._now

    def add(self, m):
        if self._now < self.endOfSimultationTime:
            super().add(m)
        
    def pop(self):
        m = super().pop(0)
        self._now = m.time
        m.t.receive(m)
        return m

    def run(self):
        while len(self):
            self.pop()

    def printSelf(self):
        print(self._now)
        for s in self:
            print(s.f, s.t, s.time)

    def register(self, *args, **kwargs):
        for node in args:
            node.scheduler = self


class Job:
    def __init__(self, name = ""):
        self.name = name
        self.arrivalTime = 0
        self.serviceTime = 0
        self.logging = []

    def log(self, *args):
        self.logging.append(args)

    def setArrivalTime(self, time):
        self.arrivalTime = time

    def setServiceTime(self, time):
        self.serviceTime = time


class Node:
    def __init__(self, name = ""):
        self.name = name
        self.In = None
        self.Out = None

    def __repr__(self):
        return self.name

    def receive(self, m = None):
        pass

    def send(self, m = None):
        pass


class Sender(Node):
    def __init__(self):
        super().__init__(name = "Sender")

        self.interarrivaltime = None
        self.totalJobs = 0
        self.numSentJobs = 0
        self.scheduler = None

    def receive(self, m):
        if m == self.generateNewJob:
            self.send()
            if self.numSentJobs < self.totalJobs:
                self.generateNewJob.time = self.scheduler.now() + self.timeBetweenConsecutiveJobs.rvs()
                self.scheduler.add(self.generateNewJob)

    def send(self):
        self.numSentJobs += 1
        job = Job(name = "job: %d".format(self.numSentJobs))
        m = Event(self, self.Out, self.scheduler.now(), job, event = "job")
        self.scheduler.add( m )

    def setTimeBetweenConsecutiveJobs(self, distribution):
        self.timeBetweenConsecutiveJobs = distribution

    def setTotalJobs(self, tot):
        # total number of jobs to be generated
        self.totalJobs = tot
        
    def start(self):
        self.generateNewJob = Event(self, self, 0, "Generate new job")
        self.scheduler.add(self.generateNewJob)


class Observer:
    def __init__(self):
        self.observers = []

    def register(self, observer):
        if not observer in self.observers:
            self.observers.append(observer)

    def unregister(self, observer):
        if observer in self.observers:
            self.observers.remove(observer)

    def unregister_all(self):
        if self.observers:
            del self.observers[:]

    def update_observers(self, *args, **kwargs):
        for observer in self.observers:
            observer.update(*args, **kwargs)

    def update(self, arg):
        pass


class Server:
    _ids = 0

    def __init__(self):
        self.queue = SortedSet(key = lambda job: job.arrivalTime)
        self.numServers = 1
        Server._ids +=1
        self.busyServers = 0
        self.serviceTimeDistribution = None
        self.name = "Server {}".format(Server._ids)
        self.In = None
        self.Out = None
        self.scheduler = None

    def receive(self, m):
        if m.event == "end":  # end of service
            self.send(m.job)  
            if len(self.queue) > 0:
                assert self.busyServers == self.numServers
                job = self.queue.pop(0)
                self.startService(job)
            else:
                assert self.busyServers > 0
                self.busyServers -= 1
            #self.departureStats()
        else: # receive new job
            assert "job" in m.event
            job = m.job
            job.setArrivalTime(self.scheduler.now())
            serviceTime =  self.serviceTimeDistribution.rvs()
            job.setServiceTime(serviceTime)
            job.log(self.scheduler.now(), "a", self.busyServers + len(self.queue))
            if self.busyServers < self.numServers:
                self.busyServers += 1
                self.startService(job)
            else:
                self.queue.add(job)

    def startService(self, job):
        job.log(self.scheduler.now(), "s", len(self.queue))
        t = self.scheduler.now() + job.serviceTime
        m = Event(self, self, t, job = job, event = "end")
        self.scheduler.add(m)
                
    def send(self, job):  # job departure
        job.log(self.scheduler.now(), "d", len(self.queue))
        m = Event(self, self.Out, self.scheduler.now(), job = job, event = "job")
        self.scheduler.add(m)

    def setServiceTimeDistribution(self, distribution):
        self.serviceTimeDistribution = distribution


class Sink:
    def __init__(self):
        self.jobs = set()
        self.name = ("Sink")
        self.In = None
        self.Out = None
        self.scheduler = None

    def receive(self, m):
        self.jobs.add(m.job)

    def send(self):
        pass

    def stats2(self, t):
        count = defaultdict(int)
        for j in self.jobs:
            for l in j.logging:
                if l[1] == t:
                    count[l[2]] += 1
        return count

    def stats(self, t):
        for j in self.jobs:
            for l in j.logging:
                time, type, queue = l[:]
                if type == t:
                    yield (time, queue)

    def arrivalStats(self):
        count = defaultdict(int)
        for t, q, in self.stats("a"):
            count[q] += 1
        return count

    def departureStats(self):
        count = defaultdict(int)
        for t, q, in self.stats("d"):
            count[q] += 1
        return count

    def queueAtArrivalTimes(self):
        T, Q = list(), list()
        for t, q in sorted(self.stats("a")):
            T.append(t)
            Q.append(q)
        return T, Q