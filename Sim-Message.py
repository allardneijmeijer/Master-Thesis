
from collections import defaultdict
from sortedcontainers import SortedSet
import numpy as np
from scipy.stats import expon
np.random.seed(1)


class Message:
    def __init__(self, f, t, time, job=None, message=""):
        self.f = f  # from node
        self.t = t  # to node
        self.time = time  # time to deliver the message
        self.job = job 
        self.message = message

    def __repr__(self):
        return "%10s %10s %7.3f\t%s"%(self.f, self.t, self.time, self.message)


class Scheduler(SortedSet):
    def __init__(self):
        super().__init__(key=lambda m: m.time)
        self.endOfSimultationTime = np.inf
        self.now = 0.

    def add(self, m):
        if self.now < self.endOfSimultationTime:
            super().add(m)

    def pop(self):
        m = super().pop(0)
        self.now = m.time
        m.t.receive(m)
        return m

    def run(self):
        while len(self):
            self.pop()

    def printSelf(self):
        print(self.now)
        for s in self:
            print(s.f, s.t, s.time)


def now():
    return scheduler.now


def schedule(m):
    scheduler.add(m)


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
        m = Message(self, self.Out, now(), message = "job: %d"%self.numSentJobs)
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
        self.serviceTime = None
        self.numServers = 0
        self.busyServers = 0
        self.queue = None

        self.count = defaultdict(int)

    def receive(self, m):
        if m.message == "end":  # end of service
            self.send(m.job)  
            if len(self.queue) > 0:
                assert self.busyServers == self.numServers
                j = self.queue.pop(0)
                t = now() + self.serviceTime.rvs()
                schedule(Message(self, self, t, job = j, message = "end")) 
            else:
                assert self.busyServers > 0
                self.busyServers -= 1
            self.departureStats()
        else: # receive new job
            assert "job" in m.message
            if self.busyServers < self.numServers: 
                self.busyServers += 1
                t = now() + self.serviceTime.rvs()
                schedule(Message(self, self, t, job = m, message = "end"))
            else:
                self.queue.add(m)
            self.arrivalStats()
                
    def send(self, m):  # job departure
        m.f, m.t, m.time = self, self.Out, now()
        schedule(m)

    def setNumberOfServers(self, numServers):
        self.numServers = numServers

    def setServiceTime(self, distribution):
        self.serviceTime = distribution

    def arrivalStats(self):
        self.count[len(self.queue)] += 1

    def departureStats(self):
        pass


class Fifo(Queue):
    def __init__(self):
        super().__init__()
        self.queue = SortedSet(key = lambda job: job.time)


class Sink(Node):
    def __init__(self):
        super(Sink, self).__init__("Sink")

    def receive(self, m):
        pass

    def send(self):
        pass


if __name__ == "__main__":
    labda, mu = 1., 1.3
    
    sender = Sender()
    sender.setTotalJobs( 1e4 )
    sender.setTimeBetweenConsecutiveJobs(expon(scale = 1./labda))
    
    queue = Fifo()
    queue.setNumberOfServers(1)
    queue.setServiceTime(expon(scale = 1./mu))
    
    sink = Sink()

sender.Out = queue
queue.In = sender
queue.Out = sink
sink.In = queue

scheduler = Scheduler()
sender.start()
scheduler.run()

rho = labda/mu
S = sum(queue.count.values())
for i, v in  queue.count.items():
    print(i, v*1./S, (1.-rho)*rho**i)