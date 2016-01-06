from collections import defaultdict
from sortedcontainers import SortedSet
from scipy.stats import expon
from transitions import Machine
# import matplotlib.pylab as plt
import numpy as np


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
        self.completed = False

    def now(self):
        return self._now

    def add(self, m):
        if self._now < self.endOfSimultationTime:
            super().add(m)
        
    def pop(self):
        m = super().pop(0)
        self._now = m.time
        m.t.receive(m)
        #self.printSelf()
        return m

    def run(self):
        while len(self):
            self.pop()
            if self._now > np.inf:
                break

    def delete(self, job):
        for index, value in enumerate(self):
            if value.job == job:
                del self[index]
                break

    def printSelf(self):
        print(self._now)
        for s in self:
            print(s.time, s.event)
        print('-----')

    def register(self, *args):
        for node in args:
            node.scheduler = self


class Job:
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


class Sender(Node):
    def __init__(self):
        super().__init__(name="Sender")

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
        job = Job(name="job: %d".format(self.numSentJobs))
        job.startime = self.scheduler.now()
        m = Event(self, self.Out, self.scheduler.now(), job, event="arrive")
        self.scheduler.add(m)

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
        if observer not in self.observers:
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


class Server(Machine):
    _ids = 0

    def __init__(self):
        state = ['Up', 'Failed', 'Maintenance', 'Blocked']
        Machine.__init__(self, states=state, initial='Up')
        self.add_transition('start', 'Up', 'Up', after='startJob')
        self.add_transition('fail', 'Up', 'Failed', after='startFail')
        self.add_transition('repair', 'Failed', 'Up', after='rep')
        self.add_transition('maintain', 'Up', 'Maintenance')
        self.add_transition('maintcpl', 'Maintenance', 'Up')
        self.add_transition('interrep', 'Failed', 'Maintenance')
        self.add_transition('block', 'Up', 'Blocked')
        self.add_transition('unblock', 'Blocked', 'Up')

        self.queue = SortedSet(key=lambda job: job.arrivalTime)
        self.numServers = 1
        self.busyServers = 0
        Server._ids += 1
        self.serviceTimeDistribution = None
        self.name = 'Server {}'.format(Server._ids)
        self.In, self.Out, self.scheduler, self.activejob, self.interuptjob = None, None, None, None, None

        # debugging
        self.jobsarrived = 0
        self.jobsprocessed = 0
        self.jobsprocessed = 0
        self.numfailures = 0

    #####  Process logic #####

    # starting and ending jobs #
    def startJob(self, job):
        # logging
        job.log(self.scheduler.now(), 's', len(self.queue))
        self.activejob = job
        # schedule job end
        t = self.scheduler.now() + job.serviceTime
        m = Event(self, self, t, job=job, event='end')
        self.scheduler.add(m)

    def end(self, m):
        self.send(m.job)
        self.jobsprocessed += 1
        self.activejob = None
        if len(self.queue) > 0 and self.state == 'Up':
            job = self.queue.pop(0)
            self.start(job)
        else:
            self.busyServers -= 1
        #self.departureStats()

    # starting and ending jobs #

    # Failures #
    def rep(self, temp):
        if self.interuptjob:
            self.resumejob()
        if not self.scheduler.completed:
            self.generateFailure()

    def startFail(self, temp):
        if self.activejob:
            self.interuptJob()
        self.numfailures += 1
        t = self.scheduler.now() + 5
        m = Event(self, self, t, job=None, event="repair")
        self.scheduler.add(m)

    def generateFailure(self):
        t = self.scheduler.now() + 100
        m = Event(self, self, t, job=None, event="fail")
        self.scheduler.add(m)
    # Failures #

    def arrive(self, m):
        self.arrivalInit(m)
        if self.busyServers < self.numServers and self.state == 'Up':
            self.busyServers += 1
            self.start(m.job)
        else:
            self.queue.add(m.job)

    ##### End process logic #####

    def receive(self, m):
        result = getattr(self, m.event)(m)

    def interuptJob(self):
        self.interuptjob = self.activejob
        self.activejob = None
        self.scheduler.delete(self.interuptjob)

    def resumejob(self):
        self.activejob = self.interuptjob
        self.interuptjob = None
        t = self.scheduler.now() + self.activejob.serviceTime
        m = Event(self, self, t, job=self.activejob, event="end")
        self.scheduler.add(m)

    def arrivalInit(self, m):
        self.jobsarrived += 1
        job = m.job
        job.setArrivalTime(self.scheduler.now())
        serviceTime = self.serviceTimeDistribution.rvs()
        job.setServiceTime(serviceTime)
        job.log(self.scheduler.now(), "a", self.busyServers + len(self.queue))
                
    def send(self, job):  # job departure
        job.log(self.scheduler.now(), "d", len(self.queue))
        m = Event(self, self.Out, self.scheduler.now(), job=job, event="arrive")
        self.scheduler.add(m)

    def setServiceTimeDistribution(self, distribution):
        self.serviceTimeDistribution = distribution


class Sink:
    def __init__(self):
        self.jobs = []
        self.name = "Sink"
        self.In = None
        self.Out = None
        self.scheduler = None
        self.sender = None
        self.tpTimes = []

    def receive(self, m):
        m.job.finishTime = self.scheduler.now()
        self.jobs.append(m.job)
        m.job.ctime = m.job.finishTime - m.job.startime
        self.tpTimes.append(m.job.ctime)
        #print(len(self.jobs))
        if len(self.jobs) == self.sender.totalJobs:
            self.scheduler.completed = True

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

    def totaltime(self):
        return self.jobs[-1].finishTime

    def throughput(self):
        avg = np.mean(self.tpTimes)
        return avg

class Simulator:
    def __init__(self, totaljobs, numServers, labda, mu, failurerate, repairtime,  maintenance):
        np.random.seed(1)
        self.rho = labda/mu
        self.scheduler = Scheduler()
        now = self.scheduler.now
        self.sender = Sender()
        arrival = expon(scale=1./labda)
        service = expon(scale=1./mu)
        self.sender.setTotalJobs(totaljobs)
        self.sender.setTimeBetweenConsecutiveJobs(arrival)
        self.queue = Server()
        self.queue.setServiceTimeDistribution(service)
        self.sink = Sink()
        self.sender.Out = self.queue
        self.queue.In = self.sender
        self.queue.Out = self.sink
        self.sink.In = self.queue
        self.sink.sender = self.sender
        self.scheduler.register(self.sender, self.queue, self.sink)
        self.sender.start()
        self.queue.generateFailure()

    def run(self):
        self.scheduler.run()
        stats = self.sink.arrivalStats()
        S = sum(stats.values())
        for i, v in stats.items():
            print(i, v*1./S, (1.-self.rho)*self.rho**i)

        print('Processed:', self.queue.jobsprocessed, 'Arrived:', self.queue.jobsarrived, self.queue.numfailures)

        print("Total time taken: {0:.2f} seconds".format(self.sink.totaltime()))

        print(self.sink.throughput())

        # T, Q = self.sink.queueAtArrivalTimes()
        # plt.plot(T,Q)
        # plt.show()
