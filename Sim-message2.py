
from functools import partial
from collections import defaultdict
from sortedcontainers import SortedSet
import numpy as np
from scipy.stats import expon
np.random.seed(1)

class Message:
    __slots__ = ['f', 't', 'time', 'job', 'message']
    
    def __init__(self, f, t, time, job = None, message = ""):
        self.f = f  # from node
        self.t = t  # to node
        self.time = time # time to deliver the message
        self.job = job 
        self.message = message

    def __repr__(self):
        return "{:>10} {:>10} {:>7.3f} {}".format(self.f, self.t, self.time, self.message)
        #return "%10s %10s %7.3f\t%s"%(self.f, self.t, self.time, self.message)

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

    def Run(self):
        while len(self):
            m = self.pop()

    def printSelf(self):
        print(self._now)
        for s in self:
            print(s.f, s.t, s.time)

scheduler = Scheduler()

schedule = scheduler.add
now = scheduler.now

class Job:
    def __init__(self, name = "", priority = 1):
        self.name = name
        self.arrivalTime = 0
        self.serviceTime = 0
        self.priority = priority
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

    def receive(self, m):
        if m == self.generateNewJob:
            self.send()
            if self.numSentJobs < self.totalJobs:
                self.generateNewJob.time = now() + self.timeBetweenConsecutiveJobs.rvs()
                schedule(self.generateNewJob)

    def send(self):
        self.numSentJobs += 1
        job = Job(name = "job: %d"%self.numSentJobs)
        m = Message(self, self.Out, now(), job, message = "job")
        schedule( m )

    def setTimeBetweenConsecutiveJobs(self, distribution):
        self.timeBetweenConsecutiveJobs = distribution

    def setTotalJobs(self, tot):
        # total number of jobs to be generated
        self.totalJobs = tot
        
    def start(self):
        self.generateNewJob = Message(self, self, 0, "Generate new job")
        schedule(self.generateNewJob)

class Queue(Node):
    def __init__(self):
        super().__init__("Queue")
        self.numServers = 0
        self.busyServers = 0
        self.queue = None
        self.serviceTimeDistribution = None

    def receive(self, m):
        if m.message == "end":  # end of service
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
            assert "job" in m.message
            job = m.job
            job.setArrivalTime(now())
            serviceTime =  self.serviceTimeDistribution.rvs()
            job.setServiceTime(serviceTime)
            job.log(now(), "a", self.busyServers + len(self.queue))
            if self.busyServers < self.numServers:
                self.busyServers += 1
                self.startService(job)
            else:
                self.queue.add(job)

    def startService(self, job):
        job.log(now(), "s", len(self.queue))
        t = now() + job.serviceTime
        m = Message(self, self, t, job = job, message = "end")
        schedule(m)
                
    def send(self, job):  # job departure
        job.log(now(), "d", len(self.queue))
        m = Message(self, self.Out, now(), job = job, message = "job")
        schedule(m)

    def setNumberOfServers(self, numServers):
        self.numServers = numServers

    def setServiceTimeDistribution(self, distribution):
        self.serviceTimeDistribution = distribution

class Sink(Node):
    def __init__(self):
        super().__init__("Sink")
        self.jobs = set()

    def receive(self, m):
        self.jobs.add(m.job)

    def send(self):
        pass

    def stats(self, t):
        count = defaultdict(int)
        for j in self.jobs:
            for l in j.logging:
                if l[1] == t:
                    count[l[2]] += 1
        return count

    def arrivalStats(self):
        return self.stats("a")

    def departureStats(self):
        return self.stats("d")

class Fifo(Queue):
    def __init__(self):
        super().__init__()
        self.queue = SortedSet(key = lambda job: job.arrivalTime)

class Lifo(Queue):
    def __init__(self):
        super().__init__()
        self.queue = SortedSet(key = lambda job: -job.arrivalTime)

class SPTF(Queue): # shortest processing time first
    def __init__(self):
        super().__init__()
        self.queue = SortedSet(key = lambda job: job.serviceTime)

class LPTF(Queue): # longest processing time first
    def __init__(self):
        super().__init__()
        self.queue = SortedSet(key = lambda job: -job.serviceTime)

class Priority(Queue): # a priority queue
    def __init__(self, numServers = 1):
        super().__init__()
        self.queue = SortedSet(key = lambda job: job.p)

labda, mu = 1., 1.3

sender = Sender()
sender.setTotalJobs( 1e4 )
sender.setTimeBetweenConsecutiveJobs(expon(scale = 1./labda))

queue = Fifo()
#queue = SPTF()
queue.setNumberOfServers(1)
queue.setServiceTimeDistribution(expon(scale = 1./mu))

sink = Sink()

if __name__ == "__main__":
    sender.Out = queue
    queue.In = sender
    queue.Out = sink
    sink.In = queue

sender.start()
scheduler.Run()

stats = sink.arrivalStats()

rho = labda/mu
S = sum(stats.values())
for i, v in  stats.items():
     print(i, v*1./S, (1.-rho)*rho**i)
