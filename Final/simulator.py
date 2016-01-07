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
        
    def pop(self, index=0):
        m = super().pop(0)
        self._now = m.time
        m.t.receive(m)
        return m

    def run(self):
        while len(self):
            self.pop()
            if self._now > np.inf:
                break

    def deletejob(self, server, job):
        for index, value in enumerate(self):
            if value.job == job and value.t == server:
                # var = self[index]
                del self[index]
                # print('deleted', var.time, self.now())
                # break

    def deleteevent(self, server, event):
        for index, value in enumerate(self):
            if value.event == event and value.t == server:
                # var = self[index]
                del self[index]
                # print('deleted', var)
                break

    def printSelf(self):
        print(self._now)
        for s in self:
            print(s.f, s.t, s.time, s.event)

    def printEvents(self, machine):
        for s in self:
            if s.t == machine:
                print(s.time, s.event)

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
        self.interupted = False

    def log(self, *args):
        self.logging.append(args)

    def setArrivalTime(self, time):
        self.arrivalTime = time

    def setServiceTime(self, time):
        self.serviceTime = time


class Sender:
    def __init__(self, jobs, distrib):
        self.name="Sender"
        self.totalJobs = jobs
        self.numSentJobs = 0
        self.scheduler = None
        self.timeBetweenConsecutiveJobs = distrib

    def receive(self, m):
        if m == self.generateNewJob:
            self.send()
            if self.numSentJobs < self.totalJobs:
                self.generateNewJob.time = self.scheduler.now() + self.timeBetweenConsecutiveJobs.rvs()
                self.scheduler.add(self.generateNewJob)

    def send(self):
        self.numSentJobs += 1
        job = Job(name="job: {}".format(self.numSentJobs))
        job.sentTime = self.scheduler.now()
        m = Event(self, self.Out, self.scheduler.now(), job, event="arrive")
        self.scheduler.add(m)
        
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


class Queue(SortedSet, Observer):
    def __init__(self, maxqueue):
        super().__init__(key=lambda job: job.arrivalTime)
        self.observers = []
        self.maxqueue = maxqueue

    def add(self, job):
        super(Queue, self).add(job)
        if len(self) >= self.maxqueue:
            self.update_observers('block')

    def pop(self, index=-1):
        value = self._list.pop(index)
        self._set.remove(value)
        if len(self) < self.maxqueue:
            self.update_observers('unblock')
        return value


class Server(Machine):
    _ids = 0

    def __init__(self, service, maxqueue, mtbf, mttr, mInt, mTime):
        super().__init__()

        # initialize state machine
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

        # initialize all kind of variables
        self.queue = Queue(maxqueue) # = SortedSet(key=lambda job: job.arrivalTime)
        self.numServers = 1
        self.busyServers = 0
        self.mtbf = mtbf
        self.mttr = mttr
        self.maintInt = mInt
        self.maintTime = mTime
        self.serviceTimeDistribution = service
        self.blocked = False

        Server._ids += 1
        self.name = 'Server {}'.format(Server._ids)
        self.In, self.Out, self.scheduler, self.activejob, self.interuptjob = None, None, None, None, None
        self.blocked = False

        # logging variables
        self.jobsarrived = 0
        self.jobsprocessed = 0
        self.jobsprocessed = 0
        self.numfailures = 0
        self.numMaint = 0
        self.ctime = []
        self.logging = []
        self.idletime = 0
        self.previdle = 0
        self.startidle = 0

    # Process logic

    def idleCount(self, var):
        if var == "start":
            self.startidle = self.scheduler.now()
        elif var == "stop":
            self.idletime += self.scheduler.now() - self.startidle

    def receive(self, m):
        result = getattr(self, m.event)(m)

    def send(self, job):  # job departure
        job.log(self.scheduler.now(), "d", len(self.queue))
        self.log(self.scheduler.now(), "d", len(self.queue))
        m = Event(self, self.Out, self.scheduler.now(), job=job, event="arrive")
        self.scheduler.add(m)

    def arrive(self, m):
        self.jobsarrived += 1
        job = m.job
        job.setArrivalTime(self.scheduler.now())
        serviceTime = self.serviceTimeDistribution.rvs()
        job.setServiceTime(serviceTime)
        job.log(self.scheduler.now(), "a", self.busyServers + len(self.queue))
        self.log(self.scheduler.now(), "a", len(self.queue))
        self.queue.add(m.job)
        self.trystart()

    def trystart(self):
        #print(self.name, self.blocked, len(self.queue))
        if len(self.queue) and self.busyServers < self.numServers and self.state == 'Up' and self.blocked == False:
            job = self.queue.pop(0)
            self.busyServers += 1
            self.start(job)
            self.idleCount('stop')
            return True
        else:
            return False

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
        self.busyServers -= 1
        self.idleCount('start')
        self.activejob = None
        self.departureStats(m)
        self.trystart()

    def departureStats(self, m):
        ctime = self.scheduler.now() - m.job.arrivalTime
        self.ctime.append(ctime)

    # Blocking
    def update(self, arg):
        #print(self.name, len(self.queue), self.blocked)
        if arg == 'block':
            self.blocked = True
        elif arg == 'unblock':
            self.blocked = False
            if self.busyServers == 0:
                self.trystart()

    # Failures
    def rep(self, temp):
        if self.interuptjob:
            self.resumejob()
        else:
            self.trystart()
            self.idleCount('start')
        if not self.scheduler.completed:
            self.generateFailure()

    def startFail(self, temp):
        if self.activejob:
            self.interuptJob()
        else:
            self.idleCount('stop')
        self.numfailures += 1
        #self.scheduler.printSelf()
        t = self.scheduler.now() + self.mttr #self.mttr.rvs()
        m = Event(self, self, t, job=None, event="repair")
        self.scheduler.add(m)

    def generateFailure(self):
        t = self.scheduler.now() + self.mtbf #self.mtbf.rvs()
        m = Event(self, self, t, job=None, event="fail")
        self.scheduler.add(m)

    # Maintenance
    def stopMaint(self, temp):
        if self.interuptjob:
            self.resumejob()
        else:
            self.trystart()
            self.idleCount('start')
        if not self.scheduler.completed:
            self.generateFailure()
            self.generateMaintenance()

    def startMaint(self, *args):
        if self.activejob:
            self.interuptJob()
        else:
            self.idleCount('stop')
        self.scheduler.deleteevent(self, 'fail')
        self.numMaint += 1
        t = self.scheduler.now() + self.maintTime #self.maintTime.rvs()
        m = Event(self, self, t, job=None, event="maintcpl")
        self.scheduler.add(m)

    def generateMaintenance(self):
        t = self.scheduler.now() + self.maintInt #self.maintInt.rvs()
        m = Event(self, self, t, job=None, event="triggerMaintenance")
        self.scheduler.add(m)

    def triggerMaintenance(self, temp):
        if self.state == "Up":
            self.maintain()
        elif self.state == "Failed":
            self.scheduler.deleteevent(self, 'repair')
            self.interrep()
        elif self.state == "Blocked":
            pass
        else:
            print('Wrong state')

    # Interupting job processing

    def interuptJob(self):
        self.interuptjob = self.activejob
        self.interuptjob.interupted = True
        self.activejob = None
        self.scheduler.deletejob(self, self.interuptjob)

    def resumejob(self):
        self.activejob = self.interuptjob
        self.interuptjob = None
        t = self.scheduler.now() + self.activejob.serviceTime
        m = Event(self, self, t, job=self.activejob, event="end")
        self.scheduler.add(m)

    # Logging and statistics gathering
    def log(self, *args):
        self.logging.append(args)

    def stats(self, t):
        for l in self.logging:
            time, type, queue = l[:]
            if type == t:
                yield (time, queue)

    def queueAtArrivalTimes(self):
        T, Q = list(), list()
        for t, q in sorted(self.stats("a")):
            T.append(t)
            Q.append(q)
        return T, Q

    def arrivalStats(self):
        count = defaultdict(int)
        for t, q, in self.stats("a"):
            count[q] += 1
        return count


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
        self.jobs.append(m.job)
        m.job.finishTime = self.scheduler.now()
        self.tpTimes.append(m.job.finishTime - m.job.sentTime)
        if len(self.jobs) == self.sender.totalJobs:
            self.scheduler.completed = True
            self.scheduler.clear()

    def send(self):
        pass

    def throughput(self):
        avg = np.mean(self.tpTimes)
        return avg

    def totaltime(self):
        return self.jobs[-1].finishTime

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
    def __init__(self, totaljobs, maxqueue, labda, mu, mtbf, mttr,  mInt, mTime):
        np.random.seed(1)
        self.checkInput([mu, mtbf, mttr, mInt, mTime])
        self.numSrv = len(mu)
        arrival = expon(scale=1./labda)

        # initialize sender, scheduler and sink
        self.sender = Sender(totaljobs, expon(scale=1./labda))
        self.scheduler = Scheduler()
        self.servers = []
        # initialize all servers
        for nr in range(self.numSrv):
            service = expon(scale=1./mu[nr])
            # mtb = expon(scale=mtbf[nr])
            # mtt = expon(scale=mttr[nr])
            # mIn = expon(scale=mInt[nr])
            # mTim = expon(scale=mTime[nr])
            mtb = mtbf[nr]
            mtt = mttr[nr]
            mIn = mInt[nr]
            mTim = mTime[nr]
            mq = maxqueue[nr]
            self.servers.append(Server(service, mq, mtbf=mtb, mttr=mtt, mInt=mIn, mTime=mTim))
        self.sink = Sink()
        now = self.scheduler.now

        # establish relations between nodes and scheduler
        self.sender.Out = self.servers[0]
        for i in range(len(self.servers)):
            if i != 0:
                self.servers[i].In = self.servers[i-1]
            if i != (len(self.servers) -1) :
                self.servers[i].Out = self.servers[i+1]
            self.scheduler.register(self.servers[i])
        self.servers[0].In = self.sender
        self.servers[-1].Out = self.sink
        self.sink.In = self.servers[-1]
        self.sink.sender = self.sender
        self.scheduler.register(self.sender, self.sink)

        # register server N as observer for server N-1
        for server in self.servers:
            if isinstance(server.In, Server):
                server.queue.register(server.In)
                #print(server.name,'registers',server.In.name, 'as observer')

        # set parameters for simulation
        #self.sender.setTotalJobs(totaljobs)
        #self.sender.setTimeBetweenConsecutiveJobs(arrival)

        # initialize failures and maintenance for all servers
        for i in range(len(self.servers)):
            self.servers[i].generateFailure()
            self.servers[i].generateMaintenance()

        self.sender.start()

    def runDebug(self):
        self.scheduler.run()

        # create output
        print('Queue length distribution:')
        for i in range(self.numSrv):
            print('Server{}'.format(i))
            stats = self.servers[i].arrivalStats()
            S = sum(stats.values())
            for i, v in stats.items():
                print(i, v*1./S)

        for i in range(self.numSrv):
            print('Processed on server{}:'.format(i), self.servers[i].jobsprocessed, 'Arrived:', self.servers[i].jobsarrived, 'Failures: ',self.servers[i].numfailures, 'Maintenance: ',self.servers[i].numMaint)

        print('Cycle time: {:.5} seconds'.format(self.sink.throughput()))

        for i in range(self.numSrv):
            print("Server {} has CT: {:.5} seconds".format(i,np.mean(self.servers[i].ctime)))

        print("Total time taken: {0:.2f} seconds".format(self.sink.totaltime()))

        total = self.sender.totalJobs / self.sink.totaltime()
        print("Total throughput: {} items per second".format(total))

        print('Idle percentage:')
        for server in self.servers:
            print('{0}: {1:.2f}'.format(server.name, server.idletime/self.sink.totaltime()*100),'%')

        # prints log for last job. Uncomment for use
        # variable = self.sink.jobs[-1].logging
        # for i in range(len(variable)):
        #     print(variable[i])

    # Print job log of job with name
    def printJobLog(self, name):
        for job in self.sink.jobs:
            if job.interupted:
                print('----')
                print(job.name)
                for i in range(len(job.logging)):
                    print(job.logging[i])

    # check for input
    def checkInput(self, values):
        n = len(values[0])
        if all(len(x) == n for x in values):
            pass
        else:
            print('Error in input! Check dimensions of input lists')
            exit()

        # if isinstance(mu, float) and isinstance(mtbf, float) and isinstance(mttr, float) and isinstance(mInt, float) and isinstance(mTime, float):
        #     print('float')
        # elif len(mtbf) == len(mu) and len(mttr) == len(mu) and len(mInt) == len(mu) and len(mTime) == len(mu):
        #     pass
        # else:
        #     print('Error: input not correct')
        #     exit()

    def run(self):
        self.scheduler.run()
        return self.sink.throughput()