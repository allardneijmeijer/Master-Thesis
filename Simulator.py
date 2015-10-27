# Developed using Python 3.4.3

from scipy.stats import expon
# from collections import defaultdict
from sortedcontainers import SortedSet
import numpy as np
from transitions import Machine
np.random.seed(1)


class Event(object):
    """
    The Event class is used to represent all events happening in the simulator. The events are stored in the event stack
    and are sorted on time of execution. All events have a Job associated with it. Events can be of several different ki
    nds:
    Arrivals, Start of processing, stop of processing, failures, etc....
    """
    __slots__ = ['time', 'event', 'job', 'ql', 'qt', 'server']

    def __init__(self, time, event, job):

        self.time = time        # Time of execution of event (for place in stack)
        self.event = event      # Type of event (Arrival, Start processing, Stop processing, failure,....)
        self.job = job          # Use instance of class Job for this event

    def __str__(self):
        return "The event is a '{}', concerned with job {} ".format(self.event, self.job.nr)


class Job(object):
    """
    The Job class describes every job that traverses the simulated system. The jobs have several parameters (to be descr
    ibed later)
    """
    __slots__ = ['nr', 'arrival', 'service', 'a', 'qt', 's', 'jobtype', 'server', 'qstart', 'qstop', 'event']

    def __init__(self, nr, a, s, jobtype, server):

        self.nr = nr                # job nr
        self.arrival = a            # arrival time of job
        self.service = s            # service time of job
        self.jobtype = jobtype      # typ of job (processing, maintenance, repair)
        self.server = server        # server where the job needs processing ( may change to become a list/dictionary lat
        # er for more servers)
        self.qt = 0                 # total queueing time of job
        self.qstart = 0             # used to compute queueing time
        self.qstop = 0              # used to compute queueing time
        self.event = None           # event associated with job
        self.routing = []

    def __str__(self):
        return "Job nr. {} is being served".format(self.nr)


class Queue(SortedSet):
    """
    The Queue class is used to represent the queues for all servers. Queues contain Jobs that have arrived at a certain
    server, but cannot be processed immediately.
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
    Class server is describing the servers of the system. The servers are modelled using finite state machines. Each
    server has associated a current state, and a fixed set of possible state transitions. The state transitions trigger
    functions that keep the simulator running.
    """
    def __init__(self, stack):
        state = ['Processing', 'Idle', 'Maintenance', 'Failure']
        Machine.__init__(self, states=state, initial='Idle')
        self.add_transition('repair', 'Failure', 'Idle')
        self.add_transition('repairproc', 'Failure', 'Processing', after='repaired')
        self.add_transition('fail', ['Processing', 'Idle'], 'Failure', before='failure')
        self.add_transition('start', 'Idle', 'Processing', before='startprocessing')
        self.add_transition('stop', 'Processing', 'Idle', after='stopprocessing')
        self.add_transition('maintain', 'Idle', 'Maintenance', before='startprocessing')
        self.add_transition('finish', 'Maintenance', 'Idle', after='stopprocessing')

        self.activeJob = None
        self.eventStack = stack
        self.queue = Queue()        # will be implemented differently in G\G\n
        self.generateMaintenance()
        self.maintenanceScheduled = None
        self.delayedJob = None
        self.generateFailure()

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
        if self.maintenanceScheduled:
            plan = self.maintenanceScheduled
            print('maintenance planned')
            self.maintenanceScheduled = None
            tstop = self.eventStack.now + plan.job.service
            self.eventStack.addEvent(tstop, 'sm', plan.job)
            self.maintain(plan)
        elif self.queue:
            job = self.queue.pop(0)
            job.qt += self.eventStack.now - job.qstart
            tstop = self.eventStack.now + job.service
            self.eventStack.addEvent(tstop, 'f', job)
            self.start(job)

        # debugging output
        self.eventStack.printCont()
        print('-------')

    def failure(self, job):
        """

        :param job:
        :return:
        """
        self.delayedJob = self.activeJob
        self.activeJob = job
        self.eventStack.remove(self.delayedJob.event)
        trepair = 1
        self.delayedJob.event.time += trepair             # replace 1 with repairtime!
        self.eventStack.add(self.delayedJob.event)
        job = Job(-1, 1, 1, 'repair', self)         # randomize later!!
        self.eventStack.addEvent(trepair, 'repair', job)
        self.startprocessing(job)

    def repaired(self):
        """

        :return:
        """
        self.activeJob = self.delayedJob
        self.delayedJob = None

    def generateFailure(self):
        """
        To be implemented
        :return:
        """
        tfail = self.eventStack.now + 1
        job = Job(-1, 1, 1, 'failure', self)        # randomize later!!
        self.eventStack.addEvent(tfail, 'fail', job)

    def generateMaintenance(self):
        """
        To be implemented
        :return:
        """
        tmaint = self.eventStack.now + 1        # randomize later!!
        job = Job(-1, 1, 1, 'maint', self)
        self.eventStack.addEvent(tmaint, 'm', job)


class Stack(SortedSet):
    """
    The Stack class is the core of the simulator. The event stack is implemented by using a sortedset, sorting all
    events on their execution time. The event stack also contains the simulation time.
    """
    def __init__(self):
        super().__init__()
        self.sup = super(Stack, self)
        self.sup.__init__(key=lambda event: event.time)
        self.stackadd = self.sup.add
        self.stackpop = self.sup.pop
        self.endOfSimultationTime = np.inf
        self.now = 0
        # initialize the jobs and a server. For development only!
        labda = 2.
        mu = 2.1
        F = expon(scale = 1./labda)
        G = expon(scale = 1./mu)
        self.serv = Server(self)
        jobs = makeJobs(10, F, G, self.serv)
        self.addJobs(jobs)

    def addJobs(self, jobs):
        """
        This function adds the specified list of jobs to the event stack. This function is used to initialize the
        simulation.
        :param jobs:
        :return:
        """
        for nr, job in enumerate(jobs):
            self.stackadd(Event(job.arrival, 'a', job))

    def addEvent(self, time, event, job):
        """
        Create a new event to be executed at the specified time, concerning the specified job.
        :param time:
        :param event:
        :param job:
        :return:
        """
        evt = Event(time = time, event = event, job = job)
        job.event = evt
        self.stackadd(evt)

    def run(self):
        """
        Performs the actual simulation. While the eventstack is nonempty, this function takes the first event on the stack
        and executes this.
        :return:
        """
        while self:
            self.printCont()
            print("--------")
            a = self.stackpop(0)

            # debugging output
            #print(a.job.server.state)
            #print(a.event)
            #print(self.now)

            self.now = a.time
            if a.event == 'a' and a.job.server.is_Idle():
                a.job.server.start(a.job)
                tstop = a.job.service + self.now
                self.addEvent(tstop, 'f', a.job)
                #debugging output
                #self.printCont()
                #print("-------")
            elif a.event == 'a' and not(a.job.server.is_Idle()):
                a.job.qstart = self.now
                a.job.server.queue.add(a.job)
                #print(len(a.job.server.queue))
            elif a.event == 'f':
                a.job.server.stop()
            elif a.event == 'm':
                print('maintenance!')
                self.serv.maintenanceScheduled = a
            elif a.event == 'sm':
                a.job.server.finish()
            elif a.event == 'fail':
                print('fail!')
                #self.printCont()
                a.job.server.fail(a.job)
            elif a.event == 'repair':
                if a.job.server.delayedJob:
                    print(a.job.server.state)
                    a.job.server.repairproc()
                else:
                    print(a.job.server.state)
                    a.job.server.repair()

    def printCont(self):
        """
        For debugging purposes. Lists all events in the event-stack
        :return:
        """
        b = list(iter(self))
        for i, item in enumerate(b):
            print(b[i].time, b[i].job.service, b[i].event)
        print("Time= {}".format(self.now))


def makeJobs(num, F, G, serv):
        jobs = list()
        A = 0 # arrival times
        for i in range(num):
            A += F.rvs()
            s = G.rvs()
            jobs.append( Job(i, A, s,jobtype='a', server=serv) )
        return jobs

s = Stack()

s.run()
