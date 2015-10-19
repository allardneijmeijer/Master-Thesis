from scipy.stats import expon
#from collections import defaultdict
from sortedcontainers import SortedSet
import numpy as np
from transitions import Machine
np.random.seed(1)


class Event(object):
    """
    The Event class is used to represent all events happening in the simulator. The events are stored in the event stack
    and are sorted on time of execution. All events have a Job associated with it. Events can be of several different kinds:
    Arrivals, Start of processing, stop of processing, failures, etc....
    """
    __slots__ = ['time', 'event', 'job', 'ql', 'qt', 'server']

    def __init__(self, time, event, job):

        self.time = time        # Time of execution of event (for place in stack)
        self.event = event      # Type of event (Arrival, Start processing, Stop processing, failure,....)
        self.job = job          # Use instance of class Job for this event

    def __str__(self):
        return "The event is a '{}', concerned with job {} ".format(self.event, self.job.time)


class Job(object):
    """
    The Job class describes every job that traverses the simulated system. The jobs have several parameters (to be described later)
    """
    __slots__ = ['nr', 'arrival', 'service', 'a', 'qt', 's', 'jobtype', 'server']

    def __init__(self, nr, a, s, jobtype, server):

        self.nr = nr                # job nr
        self.arrival = a            # arrival time of job
        self.service = s            # service time of job
        self.jobtype = jobtype      # typ of job (processing, maintenance, repair)
        self.server = server        # server where the job needs processing ( may change to become a list/dictionary later for more servers)
        self.qt = 0                 # total queueing time of job

    def __str__(self):
        return "Job nr. {} is being served".format(self.nr)


class Queue(SortedSet):
    """
    The Queue class is used to represent the queues for all servers. Queues contain Jobs that have arrived at a certain server, but cannot be processed immediately.
    The jobs are stored in the order of their arrival (later, different queueing algortithms will be implemented)
    """
    def __init__(self):
        super().__init__()
        self.sup = super(Queue, self)
        self.sup.__init__(key=lambda job: job.arrival)
        self.add = self.sup.add
        self.pop = self.sup.pop


class Server(Machine):
    """
    Class server is describing the servers of the system. The servers are modelled using finite state machines. Each server
    has associated a current state, and a fixed set of possible state transitions. The state transitions trigger functions
    that keep the simulator running.
    """
    def __init__(self):
        state = ['Processing', 'Idle', 'Maintenance', 'Failure']
        Machine.__init__(self, states=state, initial='Idle')
        self.add_transition('repair', 'Failure', ['Idle', 'Processing'])
        self.add_transition('fail', ['Processing', 'Idle'], 'Failure')
        self.add_transition('start', 'Idle', 'Processing', before='startprocessing')
        self.add_transition('stop', 'Processing', 'Idle', after='stopprocessing')
        self.add_transition('maintain', 'Idle', 'Maintenance')
        self.add_transition('finish', 'Maintenance', 'Idle')
        self.activeJob = None
        self.queue = Queue()        # will be implemented differently in G\G\n

    def startprocessing(self, job):
        """
        This function starts processing of the specified job. It sets the current activejob of the server to this job.
        :param job:
        :return:
        """
        self.activeJob = job

    def stopprocessing(self):
        """
        This function sets the current activejob to None. Next, it checks if the queue is non-empty, and if this is true
        it triggers a state-transition of the machine to process the first job in the queue.
        :return:
        """
        self.activeJob = None
        if self.queue:
            self.start(self.queue.pop(0))

    def generateFailure(self):
        """
        To be implemented
        :return:
        """
        pass

    def generateMaintenance(self):
        """
        To be implemented
        :return:
        """
        pass


class Stack(SortedSet):
    """
    The Stack class is the core of the simulator. The event stack is implemented by using a sortedset, sorting all events
    on their execution time. The event stack also contains the simulation time.
    """
    def __init__(self):
        super().__init__()
        self.sup = super(Stack, self)
        self.sup.__init__(key=lambda event: event.time)
        self.stackadd = self.sup.add
        self.stackpop = self.sup.pop
        self.endOfSimultationTime = np.inf
        self.now = 0

    def addJobs(self, jobs):
        """
        This function adds the specified list of jobs to the event stack. This function is used to initialize the simulation.
        :param jobs:
        :return:
        """
        for nr,job in enumerate(jobs):
            self.stackadd( Event(job.arrival, 'a', job) )

    def addEvent(self, time, event, job):
        """
        Create a new event to be executed at the specified time, concerning the specified job.
        :param time:
        :param event:
        :param job:
        :return:
        """
        self.stackadd( Event(time = time, event = event, job = job) )

    def run(self):
        """
        Performs the actual simulation. While the eventstack is nonempty, this function takes the first event on the stack
        and executes this.
        :return:
        """
        while self:
            a = self.stackpop(0)

            #debugging
            #print(a.job.server.state)
            #print(a.event)
            #print(self.now)

            self.now = a.time
            if a.event == 'a' and a.job.server.is_Idle():
                a.job.server.start(a.job)
                tstop = a.job.service + self.now
                self.addEvent(tstop, 'f', a.job)
                self.printCont()
            elif a.event == 'a' and not(a.job.server.is_Idle()):
                a.job.server.queue.add(a.job)
                #print(len(a.job.server.queue))
            elif a.event == 'f':
                a.job.server.stop()
            elif a.event == 'm':
                pass
            elif a.event == '':
                pass

    def printCont(self):
        b = list(iter(self))
        for i,item in enumerate(b):
            print(b[i].time, b[i].job.service, b[i].event)

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
