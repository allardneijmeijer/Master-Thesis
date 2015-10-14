__author__ = 'allardneijmeijer'

from scipy.stats import expon
#from collections import defaultdict
from sortedcontainers import SortedSet
import numpy as np
from transitions import Machine
np.random.seed(1)


class Event(object):
    __slots__ = ['time', 'event', 'job', 'ql', 'qt', 'server']

    def __init__(self, time, event, job):

        self.time = time        # Time of execution of event (for place in stack)
        self.event = event      # Type of event (Arrival, Start processing, Stop processing, failure,....)
        self.job = job          # Use instance of class Job for this event

    def __str__(self):
        return "The event starts at t={}".format(self.time)


class Job(object):              # Jobs
    __slots__ = ['nr', 'arrival', 'service', 'a', 'qt', 's', 'jobtype', 'server']

    def __init__(self, nr, a, s, jobtype, server):

        self.nr = nr
        self.arrival = a
        self.service = s
        self.jobtype = jobtype
        self.server = server
        self.qt = 0

    def __str__(self):
        return "Job is being served"


class Queue(SortedSet):            # Queues are a storage of jobs before a server
    def __init__(self):
        super().__init__()
        self.sup = super(Queue, self)
        self.sup.__init__(key=lambda job: job.arrival)
        self.add = self.sup.add
        self.pop = self.sup.pop


class Server(Machine):           # Server processes the jobs.
    def __init__(self):
        state = ['Processing', 'Idle', 'Maintenance', 'Failure']
        Machine.__init__(self, states=state, initial='Idle')
        self.add_transition('repair', 'Failure', ['Idle', 'Processing'])  # , after='generateFailure',before = '')
        self.add_transition('fail', ['Processing', 'Idle'], 'Failure')
        self.add_transition('start', 'Idle', 'Processing', before='startprocessing')
        self.add_transition('stop', 'Processing', 'Idle', after='stopprocessing')
        self.add_transition('maintain', 'Idle', 'Maintenance')
        self.add_transition('finish', 'Maintenance', 'Idle')
        self.activeJob = None
        self.queue = Queue()        # will be implemented differently in G\G\n

    def startprocessing(self,job):
        self.activeJob = job

    def stopprocessing(self):
        exit = self.activeJob
        self.activejob = None
        return exit

    def generateFailure(self):
        pass

    def generateMaintenance(self):
        pass


# class ProcessingStep(object):
# to be implemented later, when G/G/n
#     #contains the servers and their queue(s)
#     def __init__(self):
#         self.queue = Queue()
#         # For now: one queue, one server:



class Stack(SortedSet):            # Stack stores all events in a sortedset
    def __init__(self):
        super().__init__()
        self.sup = super(Stack, self)
        self.sup.__init__(key=lambda event: event.time)
        self.stackadd = self.sup.add
        self.stackpop = self.sup.pop
        self.endOfSimultationTime = np.inf
        self.now = 0

    def addJobs(self, jobs):
        for nr,job in enumerate(jobs):
            self.stackadd( Event(job.arrival, 'a', job) )

    def addEvent(self,time,event,job):
        self.stackadd( Event(time = time,event = event, job = job) )

    def run(self):
        while self:
            #print(len(self))
            a = self.stackpop(0)
            #print (a.time)
            if a.event == 'a' and a.job.server.is_Idle():
                a.job.server.start(a.job)
            elif a.event == 'a' and not(a.job.server.is_Idle()):
                a.job.server.queue.add(a.job)
            elif a.event == 'm':
                pass
            elif a.event == '':
                pass


def makeJobs(num, F, G,serv):
        jobs = list()
        A = 0 # arrival times
        for i in range(num):
            A += F.rvs()
            s = G.rvs()
            jobs.append( Job(i, A, s,jobtype='a', server=serv) )
        return jobs

labda = 2.
mu = 2.1
F = expon(scale = 1./labda)
G = expon(scale = 1./mu)
serv = Server()
jobs = makeJobs(10,F,G,serv)

s = Stack()
s.addJobs(jobs)
#s.stackadd(Event(-1, 1, 1))

s.run()

#To be used to inspect list!

# b = list(iter(s))
#
# for i,item in enumerate(b):
#     print(b[i].event)
