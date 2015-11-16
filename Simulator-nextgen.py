# Developed using Python 3.4.3
__author__ = 'allardneijmeijer'

from scipy.stats import expon
from collections import defaultdict
from sortedcontainers import SortedSet
import numpy as np
from transitions import Machine
np.random.seed(1)


class Event:
    __slots__ = ['f', 't', 'time', 'job']

    def __init__(self, f, t, time, job=None):
        self.f = f  # from node
        self.t = t  # to node
        self.time = time  # time to deliver the message
        self.job = job

    def __repr__(self):
        return 'This is a event coming from "{}" sent to "{}" at time {}, considering job "{}".'.format(self.f, self.t, self.time, self.job)


class ArrivalEvent(Event):
    def __init__(self, f, t, time, job=None):
        super().__init__(f, t, time, job)


class StartEvent(Event):
    def __init__(self, f, t, time, job=None):
        super().__init__(f, t, time, job)


class StopEvent(Event):
    def __init__(self, f, t, time, job=None):
        super().__init__(f, t, time, job)


class FailureEvent(Event):
    def __init__(self, f, t, time, job=None):
        super().__init__(f, t, time, job)


class MaintenanceEvent(Event):
    def __init__(self, f, t, time, job=None):
        super().__init__(f, t, time, job=None)


class CheckEvent(Event):
    def __init__(self, f, t, time, job=None):
        super().__init__(f, t, time, job=None)


class IdleEvent(Event):
    def __init__(self, f, t, time, job=None):
        super().__init__(f, t, time, job=None)


class Job:
    __slots__ = ['name', 'arrivalTime', 'serviceTime', 'logging']

    def __init__(self, name=""):
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

    def __repr__(self):
        return '{} with arrivaltime {}'.format(self.name, self.arrivalTime)


class Scheduler(SortedSet):
    def __init__(self):
        super().__init__(key=lambda m: m.time)
        self.simEnd = np.inf
        self.now = 0

    def now(self):
        return self.now

    def add(self, m):
        if self.now < self.simEnd:
            super().add(m)

    def first(self):
        m = super().pop(0)
        self.now = m.time
        # m.t.receive(m)
        return m

    def Run(self):
        while len(self):
            self.first()

    def printSelf(self):
        print(self.now)
        for s in self:
            print(s.f, s.t, s.time, s.job)


class Node:
    def __init__(self, name=""):
        self.name = name
        self.In = None
        self.Out = None


class Server(Node, Machine):
    def __init__(self, serviceTime = 1):
        super().__init__("Server")
        state = ['Processing', 'Idle', 'Maintenance', 'Failure', 'Blocked']
        Machine.__init__(self, states=state, initial='Idle')
        self.add_transition('repair', 'Failure', 'Idle')
        self.add_transition('repairproc', 'Failure', 'Processing')
        self.add_transition('fail', ['Processing', 'Idle'], 'Failure')
        self.add_transition('start', 'Idle', 'Processing', 'startproc')
        self.add_transition('stop', 'Processing', 'Idle', 'stopproc')
        self.add_transition('maintain', 'Idle', 'Maintenance')
        self.add_transition('finish', 'Maintenance', 'Idle')
        self.add_transition('block', ['Processing', 'Idle', 'Maintenance', 'Failure'], 'Blocked')
        self.add_transition('unblock', 'Blocked', 'Idle')
        self.activeJob = None
        self.queue = SortedSet
        self.maxQueue = None
        self.serviceTime = serviceTime

    def generateFailure(self):
        pass

    def generateMaintenance(self):
        pass

    def receive(self, m):
        if isinstance(m, StartEvent):
            self.start(m.job)
        elif isinstance(m, StopEvent):
            self.stop(m.job)
        elif isinstance(m, ArrivalEvent):
            m.job.arrivalTime = now
            if len(self.queue):
                    self.addQueue(m.job)
            else:
                b = StartEvent(self, self, now, m.job)
                schedule(b)
        else:
            raise Exception('No valid event!')

    def send(self, m):
        schedule(m)

    def startproc(self,m):
        print('Start!')
        self.activeJob = m
        stoptime = now + self.serviceTime  # randomize!
        signal = StopEvent(self, self, stoptime, m.job)
        schedule(signal)

    def stopproc(self,m):
        print('Stop')
        self.activeJob = None
        schedule(ArrivalEvent(self, self.Out, now, m.job))
        if len(self.queue):
            newjob = self.queue.pop(0)
            schedule(StartEvent(self, self, now, newjob))
        else:
            pass

    def addQueue(self, m):
        self.queue.add(m)


# class Queue(Node):
#     def __init__(self):
#         super().__init__("Queue")
#         self.queue = SortedSet
#         self.maxQueue = None
#
#     def receive(self, m):
#         if isinstance(m, ArrivalEvent):
#             m.job.arrivalTime = now
#             if len(self.queue):
#                     self.add(m.job)
#             else:
#                 b = StartEvent(self, self.Out, now, m.job)
#                 schedule(b)
#         elif isinstance(m, CheckEvent):
#             # Checks if the event is a check from upstream server for jobs in queue
#             if len(self.queue):
#                 b = StartEvent(self, self.Out, now, m.job)
#                 schedule(b)
#             # else:
#             #     b = IdleEvent(self, self.Out, now, None)
#             #     schedule(b)
#         else:
#             raise Exception('No valid event!')
#
#     def add(self, m):
#         self.queue.add(m)


class Sink(Node):
    def __init__(self, arrival, totaljobs):
        super().__init__("Sink")
        self.jobs = set()
        self.numSentJobs = 0
        self.totalJobs = totaljobs
        self.timeBetweenConsecutiveJobs = arrival
        self.generateNewJob = Event(self, self, 0)

    def __str__(self):
        return self.name

    def receive(self, m):
        if m == self.generateNewJob:
            self.send()
            if self.numSentJobs < self.totalJobs:
                self.generateNewJob.time = now + self.timeBetweenConsecutiveJobs.rvs()
                schedule(self.generateNewJob)
        else:
            self.jobs.add(m.job)

    def send(self):
        self.numSentJobs += 1
        job = Job(name="job: {}".format(self.numSentJobs))
        m = ArrivalEvent(self, self.Out, now, job)
        schedule(m)

    def start(self):
        self.generateNewJob = ArrivalEvent(self, self.Out, 0, "Generate new job")
        schedule(self.generateNewJob)


class Fifo(Server):
    def __init__(self, servicetime):
        super().__init__(servicetime)
        self.queue = SortedSet(key = lambda job: job.arrivalTime)

class Lifo(Server):
    def __init__(self, servicetime):
        super().__init__(servicetime)
        self.queue = SortedSet(key = lambda job: -job.arrivalTime)

class SPTF(Server): # shortest processing time first
    def __init__(self, servicetime):
        super().__init__(servicetime)
        self.queue = SortedSet(key = lambda job: job.serviceTime)

class LPTF(Server): # longest processing time first
    def __init__(self, servicetime):
        super().__init__(servicetime)
        self.queue = SortedSet(key = lambda job: -job.serviceTime)

class Priority(Server): # a priority queue
    def __init__(self,servicetime,  numServers = 1):
        super().__init__(servicetime)
        self.queue = SortedSet(key = lambda job: job.p)


#### initializing scheduler ####
scheduler = Scheduler()
schedule = scheduler.add
now = scheduler.now
####

labda, mu = 1., 1.3
arrive=expon(scale = 1./labda)
service=expon(scale = 1./mu)

s = Sink(arrive, 10)
e = Fifo(service)

s.In = e
e.In = s
e.Out= s
s.Out = e

s.start()
scheduler.Run



# j = Job('1')
# j.arrivalTime = 2
# e.addQueue(j)
# srv = Server()
# e.Out = srv
# print(e.queue[0])
# a = ArrivalEvent('a', 'b', 1, Job('2'))
# e.receive(a)
#
# print(e.queue[0], e.queue[1])
