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
        return "{} {} {} {}".format(self.f, self.t, self.time, self.event)


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
        return m

    def run(self):
        while len(self):
            self.pop()
            if self._now > np.inf:
                break

    def deletejob(self, job):
        for index, value in enumerate(self):
            if value.job == job:
                var = self[index]
                del self[index]
                #print('deleted', var.time, self.now())
                break

    def deleteevent(self, event):
        for index, value in enumerate(self):
            if value.event == event:
                var = self[index]
                del self[index]
                #print('deleted', var)
                break

    def printSelf(self):
        print(self._now)
        for s in self:
            print(s.f, s.t, s.time)

    def register(self, *args):
        for node in args:
            node.scheduler = self


class Job:
    def __init__(self, name=""):
        self.name = name
        self.arrivalTime = 0
        self.serviceTime = 0
        self.sentTime = 0
        self.finishTime = 0
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
        job.sentTime = self.scheduler.now()
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

    def __init__(self, service, mtbf, mttr, mInt, mTime):
        state = ['Up', 'Failed', 'Maintenance', 'Blocked']
        Machine.__init__(self, states=state, initial='Up')
        self.add_transition('start', 'Up', 'Up', after='startJob')
        self.add_transition('fail', 'Up', 'Failed', after='startFail')
        self.add_transition('repair', 'Failed', 'Up', after='rep')
        self.add_transition('maintain', 'Up', 'Maintenance', after='startMaint')
        self.add_transition('maintcpl', 'Maintenance', 'Up', after='stopMaint')
        self.add_transition('interrep', 'Failed', 'Maintenance', after='startMaint')
        self.add_transition('block', 'Up', 'Blocked')
        self.add_transition('unblock', 'Blocked', 'Up')

        self.queue = SortedSet(key=lambda job: job.arrivalTime)
        self.numServers = 1
        self.busyServers = 0
        self.mtbf = mtbf
        self.mttr = mttr
        self.maintInt = mInt
        self.maintTime = mTime
        Server._ids += 1
        self.serviceTimeDistribution = service
        self.name = 'Server {}'.format(Server._ids)
        self.In, self.Out, self.scheduler, self.activejob, self.interuptjob = None, None, None, None, None

        # debugging
        self.jobsarrived = 0
        self.jobsprocessed = 0
        self.jobsprocessed = 0
        self.numfailures = 0
        self.numMaint = 0


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
        t = self.scheduler.now() + self.mttr
        m = Event(self, self, t, job=None, event="repair")
        self.scheduler.add(m)

    def generateFailure(self):
        t = self.scheduler.now() + self.mtbf
        m = Event(self, self, t, job=None, event="fail")
        self.scheduler.add(m)
    # Failures #

    # Maintenance #
    def stopMaint(self, temp):
        if self.interuptjob:
            #self.resumejob()
            self.startJob(self.interuptjob)
            self.interuptjob = None
        if not self.scheduler.completed:
            self.generateFailure()
            self.generateMaintenance()

    def startMaint(self, *args):
        if self.activejob:
            self.interuptJob()
        self.scheduler.deleteevent('fail')
        self.numMaint += 1
        t = self.scheduler.now() + self.maintTime
        m = Event(self, self, t, job=None, event="maintcpl")
        self.scheduler.add(m)

    def generateMaintenance(self):
        t = self.scheduler.now() + self.maintInt
        m = Event(self, self, t, job=None, event="triggerMaintenance")
        self.scheduler.add(m)

    def triggerMaintenance(self, temp):
        if self.state == "Up":
            self.maintain()
        elif self.state == "Failed":
            self.scheduler.deleteevent('repair')
            self.interrep()
        elif self.state == "Blocked":
            pass
        else:
            print('Wrong state')

    # Maintenance #

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
        self.scheduler.deletejob(self.interuptjob)

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

    # def setServiceTimeDistribution(self, distribution):
    #     self.serviceTimeDistribution = distribution

    # def setIntervals(self, mtbf, mttr, mInt, mTime):
    #     if isinstance(mtbf, int):
    #         self.mtbf = mtbf
    #     else:
    #         self.mtbf = expon(scale=1/mtbf)
    #
    #     if isinstance(mttr, int):
    #         self.mttr = mttr
    #     else:
    #         self.mttr = expon(scale=1/mttr)
    #
    #     if isinstance(mInt, int):
    #         self.maintInt = mInt
    #     else:
    #         self.maintInt = expon(scale=1/mInt)
    #
    #     if isinstance(mTime, int):
    #         self.maintTime = mTime
    #     else:
    #         self.maintTime = expon(scale=1/mTime)


class Sink:
    def __init__(self):
        self.jobs = set()
        self.name = "Sink"
        self.In = None
        self.Out = None
        self.scheduler = None
        self.sender = None
        self.tpTimes = []
        self.lastarrivaltime = 0

    def receive(self, m):
        self.jobs.add(m.job)
        #print(len(self.jobs))
        m.job.finishTime = self.scheduler.now()
        tp = m.job.finishTime - m.job.sentTime
        self.tpTimes.append(tp)
        self.lastarrivaltime = self.scheduler.now()
        if len(self.jobs) == self.sender.totalJobs:
            self.scheduler.completed = True
            self.scheduler.clear()

    def send(self):
        pass

    def stats2(self, t):
        count = defaultdict(int)
        for j in self.jobs:
            for l in j.logging:
                if l[1] == t:
                    count[l[2]] += 1
        return count

    def throughput(self):
        avg = np.mean(self.tpTimes)
        return avg

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


class Simulator:
    def __init__(self, totaljobs, numServers, labda, mu, mtbf, mttr,  mInt, mTime):
        np.random.seed(1)
        # initialize values
        self.rho = labda/mu
        arrival = expon(scale=1./labda)
        service = expon(scale=1./mu)

        # initializations
        self.sender = Sender()
        self.scheduler = Scheduler()
        self.queue = Server(service, mtbf=mtbf, mttr=mttr, mInt=mInt, mTime=mTime)
        self.sink = Sink()
        now = self.scheduler.now

        # establish relations between nodes
        self.sender.Out = self.queue
        self.queue.In = self.sender
        self.queue.Out = self.sink
        self.sink.In = self.queue
        self.sink.sender = self.sender
        self.scheduler.register(self.sender, self.queue, self.sink)

        # set parameters
        self.sender.setTotalJobs(totaljobs)
        self.sender.setTimeBetweenConsecutiveJobs(arrival)

        # self.queue.setServiceTimeDistribution(service)
        # self.queue.setIntervals(mtbf, mttr, mInt, mTime)
        self.queue.generateFailure()
        self.queue.generateMaintenance()

        self.sender.start()

    def runDebug(self):
        self.scheduler.run()
        stats = self.sink.arrivalStats()
        S = sum(stats.values())
        for i, v in stats.items():
            print(i, v*1./S, (1.-self.rho)*self.rho**i)

        print('Processed:', self.queue.jobsprocessed, 'Arrived:', self.queue.jobsarrived, self.queue.numfailures, self.queue.numMaint)
        print(self.sink.throughput())
        print(self.sink.lastarrivaltime)

        # T, Q = self.sink.queueAtArrivalTimes()
        # plt.plot(T,Q)
        # plt.show()

    def run(self):
        self.scheduler.run()
        return self.sink.throughput()