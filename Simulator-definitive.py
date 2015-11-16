
from collections import defaultdict
from sortedcontainers import SortedSet
import numpy as np
from scipy.stats import expon
import matplotlib.pylab as plt
from transitions import Machine

np.random.seed(1)


class Event:
    __slots__ = ['f', 't', 'time', 'job', 'event']
    
    def __init__(self, f, t, time, job=None, event=""):
        self.f = f  # from node
        self.t = t  # to node
        self.time = time  # time to deliver the message
        self.job = job 
        self.event = event

    def __repr__(self):
        return "%10s %10s %7.3f\t%s".format(self.f, self.t, self.time, self.event)


class Scheduler(SortedSet):
    def __init__(self):
        super().__init__(key=lambda m: m.time)
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

scheduler = Scheduler()

schedule = scheduler.add
now = scheduler.now


class Job:
    def __init__(self, name="", priority=1):
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
    def __init__(self, name=""):
        self.name = name
        self.In = None
        self.Out = None

    def __repr__(self):
        return self.name

    def receive(self, m=None):
        pass

    def send(self, m=None):
        pass


class Source(Node):
    def __init__(self):
        super().__init__(name="Source")

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
        job = Job(name="job: {}".format(self.numSentJobs))
        m = Event(self, self.Out, now(), job, event="job")
        schedule(m)

    def setTimeBetweenConsecutiveJobs(self, distribution):
        self.timeBetweenConsecutiveJobs = distribution

    def setTotalJobs(self, tot):
        # total number of jobs to be generated
        self.totalJobs = tot
        
    def start(self):
        self.generateNewJob = Event(self, self, 0, "Generate new job")
        schedule(self.generateNewJob)


class Server(Node, Machine):
    def __init__(self, name):
        super().__init__(name)
        state = ['Processing', 'Idle', 'Maintenance', 'Failure', 'Blocked']
        Machine.__init__(self, states=state, initial='Idle')
        self.add_transition('repair', 'Failure', 'Idle')
        self.add_transition('repairproc', 'Failure', 'Processing')
        self.add_transition('fail', ['Processing', 'Idle'], 'Failure')
        self.add_transition('start', 'Idle', 'Processing', after='startService')
        self.add_transition('stop', ['Processing','Blocked'], 'Idle')
        self.add_transition('maintain', 'Idle', 'Maintenance')
        self.add_transition('finish', 'Maintenance', 'Idle')
        self.add_transition('block', ['Processing', 'Idle', 'Maintenance', 'Failure'], 'Blocked')
        self.add_transition('unblocki', 'Blocked', 'Idle')
        self.add_transition('unblockp', 'Blocked', 'Processing')
        self.activeJob = None
        self.maxQueue = 5
        self.numServers = 1
        self.busyServers = 0
        self.queue = None
        self.serviceTimeDistribution = None
        self.observers = []
        self.jobsprocessed = 0
        self.jobsReceived = 0
        self.blocked = False

    def receive(self, m):
        # print("event = {}".format(m.event))
        # print(self.activeJob)
        if m.event == "end":  # end of service
            self.send(m.job)
            self.activeJob = None
            self.stop()
            if len(self.queue) > 0 and not(self.state == 'Blocked'):
                assert self.busyServers == self.numServers
                job = self.queue.pop(0)
                self.jobsprocessed +=1
                self.start(job)
                self.unblock()
                #self.startService(job)
            else:
                assert self.busyServers > 0
                self.busyServers -= 1
            # self.departureStats()
        else: # receive new job
            assert "job" in m.event
            job = m.job
            self.jobsReceived += 1
            job.setArrivalTime(now())
            serviceTime =  self.serviceTimeDistribution.rvs()
            job.setServiceTime(serviceTime)
            job.log(now(), "a", self.busyServers + len(self.queue))
            if self.busyServers < self.numServers and not(self.state == 'Blocked'):
                self.busyServers += 1
                self.jobsprocessed +=1
                self.start(job)
            else:
                if len(self.queue) >= self.maxQueue:
                    self.update_observers("Block")
                    self.queue.add(job)
                    # blocking!
                else:
                    self.queue.add(job)

    def unblock(self):
        self.update_observers("Unblock")

    def startService(self, job):
        # print('started!')
        # print(self.state)
        self.activeJob = job
        job.log(now(), "s", len(self.queue))
        t = now() + job.serviceTime
        m = Event(self, self, t, job=job, event="end")
        schedule(m)
                
    def send(self, job):  # job departure
        job.log(now(), "d", len(self.queue))
        m = Event(self, self.Out, now(), job=job, event="job")
        schedule(m)

    def setNumberOfServers(self, numServers):
        self.numServers = numServers

    def setServiceTimeDistribution(self, distribution):
        self.serviceTimeDistribution = distribution

    # Code for Observer
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
        if "Block" in arg:
            #print("{} {}".format(self, arg))
            self.block()
        elif "Unblock" in arg and self.state == 'Blocked':
            #print("{} {}, {}".format(self, arg, self.activeJob))
            if self.activeJob:
                self.unblockp()
            else:
                print('idle')
        else:
            print('Not Blocked')


class Sink(Node):
    def __init__(self):
        super().__init__("Sink")
        self.jobs = SortedSet(key= lambda job: job.name)
        self.jobsreceived = 0

    def receive(self, m):
        self.jobsreceived +=1
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
    

class Fifo(Server):
    def __init__(self, name):
        super().__init__(name)
        self.queue = SortedSet(key=lambda job: job.arrivalTime)


class Lifo(Server):
    def __init__(self):
        super().__init__()
        self.queue = SortedSet(key=lambda job: -job.arrivalTime)


class SPTF(Server): # shortest processing time first
    def __init__(self):
        super().__init__()
        self.queue = SortedSet(key=lambda job: job.serviceTime)


class LPTF(Server): # longest processing time first
    def __init__(self):
        super().__init__()
        self.queue = SortedSet(key=lambda job: -job.serviceTime)


class Priority(Server): # a priority queue
    def __init__(self, numServers = 1):
        super().__init__()
        self.queue = SortedSet(key=lambda job: job.p)


if __name__ == "__main__":
    labda, mu = 1., 1.3
    
    source = Source()
    source.setTotalJobs( 1e4 )
    source.setTimeBetweenConsecutiveJobs(expon(scale = 1./labda))
    server1 = Fifo('server1')
    server2 = Fifo('server2')
    server3 = Fifo('server3')
    server2.register(server1)       # register observer
    #queue = SPTF()
    server1.setNumberOfServers(1)
    server1.setServiceTimeDistribution(expon(scale = 1./mu))
    server2.setNumberOfServers(1)
    server2.setServiceTimeDistribution(expon(scale = 1./mu))
    server3.setNumberOfServers(1)
    server3.setServiceTimeDistribution(expon(scale = 1./mu))

    sink = Sink()

if __name__ == "__main__":
    source.Out = server1
    server1.In = source
    server1.Out = server2
    server2.In = server1
    server2.Out = server3
    server3.In = server2
    server3.Out = sink
    sink.In = server1

if __name__ == "__main__":
    source.start()
    scheduler.run()

# if __name__ == "__main__":
#     T, Q = sink.queueAtArrivalTimes()
#     plt.plot(T,Q)
#     plt.show()



#    quit()
    stats = sink.arrivalStats()

    rho = labda/mu
    S = sum(stats.values())
    for i, v in  stats.items():
        print(i, v*1./S, (1.-rho)*rho**i)

print(source.numSentJobs)
print(server1.jobsprocessed)
print(server2.jobsprocessed)
print(server1.jobsReceived)
print(server3.jobsprocessed)
print(sink.jobsreceived)

for i in sink.jobs:
    print(sink.jobs.pop(0).name)
