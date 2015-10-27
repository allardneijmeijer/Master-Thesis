# Developed using Python 3.4.3
__author__ = 'allardneijmeijer'

from scipy.stats import expon
from collections import defaultdict
from functools import partial
from sortedcontainers import SortedSet
import numpy as np
from transitions import Machine
np.random.seed(1)


class Event:
    __slots__ = ['f', 't', 'time', 'job']

    def __init__(self, f, t, time, job = None):
        self.f = f  # from node
        self.t = t  # to node
        self.time = time # time to deliver the message
        self.job = job

    def __repr__(self):
        return 'This is a event comming from "{}" sent to "{}" at time {}, considering job "{}".'.format(self.f, self.t, self.time, self.job)

class ArrivalEvent(Event):
    def __init__(self, f, t, time, job=None):
        super().__init__(f, t, time, job=None)

class StartEvent(Event):
    def __init__(self, f, t, time, job=None):
        super().__init__(f, t, time, job=None)


class StopEvent(Event):
    def __init__(self, f, t, time, job=None):
        super().__init__(f, t, time, job=None)


class FailureEvent(Event):
    def __init__(self, f, t, time, job=None):
        super().__init__(f, t, time, job=None)


class MaintenanceEvent(Event):
    def __init__(self, f, t, time, job=None):
        super().__init__(f, t, time, job=None)


class Job:
    __slots__ = ['name', 'arrivalTime', 'serviceTime', 'logging']
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

    def __repr__(self):
        return '{}'.format(self.name)


class Scheduler(SortedSet):
    def __init__(self):
        super().__init__(key = lambda m: m.time)
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
        #m.t.receive(m)
        return m

    def Run(self):
        while len(self):
            m = self.first()

    def printSelf(self):
        print(self.now)
        for s in self:
            print(s.f, s.t, s.time)


class Entry(Node):
    def __init(self):
        super().__init__("Entry")

    def receive(self,m):
        if isinstance(m,ArrivalEvent):
            print('Succes!')
        else:
            raise('Fout!')

    def send(self,m):
        pass


class Node:
    def __init__(self, name = ""):
        self.name = name
        self.In = None
        self.Out = None


class Server(Node, Machine):
    def __init__(self):
        super().__init__("Server")
        state = ['Processing', 'Idle', 'Maintenance', 'Failure']
        Machine.__init__(self, states=state, initial='Idle')
        self.add_transition('repair', 'Failure', 'Idle')
        self.add_transition('repairproc', 'Failure', 'Processing')
        self.add_transition('fail', ['Processing', 'Idle'], 'Failure')
        self.add_transition('start', 'Idle', 'Processing')
        self.add_transition('stop', 'Processing', 'Idle')
        self.add_transition('maintain', 'Idle', 'Maintenance')
        self.add_transition('finish', 'Maintenance', 'Idle')

    def generateFailure(self):
        pass

    def generateMaintenance(self):
        pass

    def receive(self):
        pass

    def send(self,m):
        pass

class Queue(SortedSet):
    def __init__(self, name="Queue"):
        super().__init__(key = lambda m: m.arrivalTime)
        self.name = name
        self.In = None
        self.Out = None

    def receive(self):
        pass

    def send(self,m):
        pass


class Sink(Node):
    def __init__(self, arrival, totaljobs):
        super().__init__("Sink")
        self.jobs = set()
        self.numSentJobs = 0
        self.totalJobs = totaljobs
        self.timeBetweenConsecutiveJobs = arrival

    def __str__(self):
        return 'Sink!'

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
        job = Job(name = "job: {}".format(self.numSentJobs))
        m = ArrivalEvent(self, self.Out, now, job)
        schedule(m)

    def start(self):
        self.generateNewJob = Event(self, self, 0)
        schedule(self.generateNewJob)

#### initializing scheduler ####
scheduler = Scheduler()
schedule = scheduler.add
now = scheduler.now
####

labda, mu = 1., 1.3
arrive=expon(scale = 1./labda)
service=expon(scale = 1./mu)

s = Sink(arrive,1e4)

e=Entry()
a = ArrivalEvent('a','b',1)
e.receive(a)