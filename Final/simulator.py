from collections import defaultdict
from sortedcontainers import SortedSet
from scipy.stats import expon
from transitions import Machine
# import matplotlib.pylab as plt
import numpy as np


class Event:
    """
    The Event class is used to communicate between nodes in the simulator. All nodes send events considering arrivals of
    jobs, the start of processing of jobs, maintenance tasks and more. All events are stored in the "Scheduler" and
    executed at the right time.
    """
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
    """
    The scheduler handles all events in the simulator. Events are stored in a chronological way, and executed one by one
    The scheduler also contains the simulator clock, and can therefore be seen as the core of the event based simulator.
    """
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

    def delete_job(self, server, job):
        for index, value in enumerate(self):
            if value.job == job and value.t == server:
                # var = self[index]
                del self[index]
                # print('deleted', var.time, self.now())
                # break

    def delete_event(self, server, event):
        for index, value in enumerate(self):
            if value.event == event and value.t == server:
                # var = self[index]
                del self[index]
                # print('deleted', var)
                break

    def print_self(self):
        print(self._now)
        for s in self:
            print(s.f, s.t, s.time, s.event)

    def print_events(self, machine):
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

    def set_arrival_time(self, time):
        self.arrivalTime = time

    def set_service_time(self, time):
        self.serviceTime = time


class Sender:
    """
    The sender generates new jobs according to the arrival rate distribution. It uses a self-sustaining event that
    reinstates itself in the scheduler when it arrives at the sender again.
    """
    def __init__(self, jobs, distrib):
        self.name = "Sender"
        self.totalJobs = jobs
        self.numSentJobs = 0
        self.scheduler = None
        self.timeBetweenConsecutiveJobs = distrib
        self.Out = None
        self.generateNewJob = Event(self, self, 0, "Generate new job")

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
        self.scheduler.add(self.generateNewJob)


class Observer:
    """
    The observer is the base class for using the observer pattern.
    """
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
    """
    The queue is a class that contains all the jobs in queue for its associated server. It behaves like a sortedset
    implementing a FIFO queueing discipline. Further, the queue implements the observer class: it registers the previous
    server in line as its observer in order to be able to block processing in this server when the queue becomes full.
    """
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
    """
    The server is implemented as a state machine. Based on the state of the machine, actions are executed with the
    server or the jobs in the server. Most actions are executed by using state transitions, but some actions have to be
    handled by the scheduler (like ending a job at a certain time).
    """
    _ids = 0

    def __init__(self, service, maxqueue, mtbf, mttr, mInt, mTime):
        super().__init__()

        # initialize state machine
        state = ['Up', 'Failed', 'Maintenance', 'Blocked']
        Machine.__init__(self, states=state, initial='Up')
        self.add_transition('start', 'Up', 'Up', after='start_job')
        self.add_transition('fail', 'Up', 'Failed', after='start_fail')
        self.add_transition('repair', 'Failed', 'Up', after='rep')
        self.add_transition('maintain', 'Up', 'Maintenance', after='start_maint')
        self.add_transition('maintcpl', 'Maintenance', 'Up', after='stop_maint')
        self.add_transition('interrep', 'Failed', 'Maintenance', after='start_maint')
        self.add_transition('block', 'Up', 'Blocked')
        self.add_transition('unblock', 'Blocked', 'Up')

        # initialize all kind of variables
        self.queue = Queue(maxqueue)
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

    def idle_count(self, var):
        if var == "start":
            self.startidle = self.scheduler.now()
        elif var == "stop":
            self.idletime += self.scheduler.now() - self.startidle

    def receive(self, m):
        getattr(self, m.event)(m)

    def send(self, job):  # job departure
        job.log(self.scheduler.now(), "d", len(self.queue))
        self.log(self.scheduler.now(), "d", len(self.queue))
        m = Event(self, self.Out, self.scheduler.now(), job=job, event="arrive")
        self.scheduler.add(m)

    def arrive(self, m):
        self.jobsarrived += 1
        job = m.job
        job.set_arrival_time(self.scheduler.now())
        service_time = self.serviceTimeDistribution.rvs()
        job.set_service_time(service_time)
        job.log(self.scheduler.now(), "a", self.busyServers + len(self.queue))
        self.log(self.scheduler.now(), "a", len(self.queue))
        self.queue.add(m.job)
        self.trystart()

    def trystart(self):
        # print(self.name, self.blocked, len(self.queue))
        if len(self.queue) and self.busyServers < self.numServers and self.state == 'Up' and self.blocked is False:
            job = self.queue.pop(0)
            self.busyServers += 1
            self.start(job)
            self.idle_count('stop')
            return True
        else:
            return False

    # starting and ending jobs #
    def start_job(self, job):
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
        self.idle_count('start')
        self.activejob = None
        self.departure_stats(m)
        self.trystart()

    def departure_stats(self, m):
        ctime = self.scheduler.now() - m.job.arrivalTime
        self.ctime.append(ctime)

    # Blocking
    def update(self, arg):
        # print(self.name, len(self.queue), self.blocked)
        if arg == 'block':
            self.blocked = True
        elif arg == 'unblock':
            self.blocked = False
            if self.busyServers == 0:
                self.trystart()

    # Failures
    def rep(self, *args):
        if self.interuptjob:
            self.resumejob()
        else:
            self.trystart()
            self.idle_count('start')
        if not self.scheduler.completed:
            self.generate_failure()

    def start_fail(self, *args):
        if self.activejob:
            self.interupt_job()
        else:
            self.idle_count('stop')
        self.numfailures += 1
        # self.scheduler.print_Self()
        t = self.scheduler.now() + self.mttr  # self.mttr.rvs()
        m = Event(self, self, t, job=None, event="repair")
        self.scheduler.add(m)

    def generate_failure(self):
        t = self.scheduler.now() + self.mtbf  # self.mtbf.rvs()
        m = Event(self, self, t, job=None, event="fail")
        self.scheduler.add(m)

    # Maintenance
    def stop_maint(self, *args):
        if self.interuptjob:
            self.resumejob()
        else:
            self.trystart()
            self.idle_count('start')
        if not self.scheduler.completed:
            self.generate_failure()
            self.generate_maintenance()

    def start_maint(self, *args):
        if self.activejob:
            self.interupt_job()
        else:
            self.idle_count('stop')
        self.scheduler.delete_event(self, 'fail')
        self.numMaint += 1
        t = self.scheduler.now() + self.maintTime  # self.maintTime.rvs()
        m = Event(self, self, t, job=None, event="maintcpl")
        self.scheduler.add(m)

    def generate_maintenance(self):
        t = self.scheduler.now() + self.maintInt  # self.maintInt.rvs()
        m = Event(self, self, t, job=None, event="trigger_maintenance")
        self.scheduler.add(m)

    def trigger_maintenance(self, temp):
        if self.state == "Up":
            self.maintain()
        elif self.state == "Failed":
            self.scheduler.delete_event(self, 'repair')
            self.interrep()
        elif self.state == "Blocked":
            pass
        else:
            print('Wrong state')

    # Interupting job processing

    def interupt_job(self):
        self.interuptjob = self.activejob
        self.interuptjob.interupted = True
        self.activejob = None
        self.scheduler.delete_job(self, self.interuptjob)

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
            time, action, queue = l[:]
            if action == t:
                yield (time, queue)

    def queue_at_arrival_times(self):
        T, Q = list(), list()
        for t, q in sorted(self.stats("a")):
            T.append(t)
            Q.append(q)
        return T, Q

    def arrival_stats(self):
        count = defaultdict(int)
        for t, q, in self.stats("a"):
            count[q] += 1
        return count


class Sink:
    """
    The sink is the last node in the simulator. All jobs eventually accumulate in the sink, and are processed here for
    statistics gathering.
    """
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
                time, action, queue = l[:]
                if action == t:
                    yield (time, queue)

    def arrival_stats(self):
        count = defaultdict(int)
        for t, q, in self.stats("a"):
            count[q] += 1
        return count

    def departure_stats(self):
        count = defaultdict(int)
        for t, q, in self.stats("d"):
            count[q] += 1
        return count

    def queue_at_arrival_times(self):
        T, Q = list(), list()
        for t, q in sorted(self.stats("a")):
            T.append(t)
            Q.append(q)
        return T, Q


class Simulator:
    """
    This class is used to instantiate all classes and build the actual simulator model. It uses the inputs to determine
    the number of servers needed, the parameters of these servers and the total amount of jobs. Also, relations between
    the nodes are made and the observer pattern is initialized.
    """
    def __init__(self, totaljobs, maxqueue, labda, mu, mtbf, mttr,  mInt, mTime):
        np.random.seed(1)
        self.checkInput([mu, mtbf, mttr, mInt, mTime])
        self.numSrv = len(mu)

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
            ### currently deterministic failures and maintenance.
            mtb = mtbf[nr]
            mtt = mttr[nr]
            mIn = mInt[nr]
            mTim = mTime[nr]
            mq = maxqueue[nr]
            self.servers.append(Server(service, mq, mtbf=mtb, mttr=mtt, mInt=mIn, mTime=mTim))
        self.sink = Sink()

        # establish relations between nodes and scheduler
        self.sender.Out = self.servers[0]
        for i in range(len(self.servers)):
            if i != 0:
                self.servers[i].In = self.servers[i - 1]
            if i != (len(self.servers) -1):
                self.servers[i].Out = self.servers[i + 1]
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
                # print(server.name,'registers',server.In.name, 'as observer')

        # initialize failures and maintenance for all servers
        for i in range(len(self.servers)):
            self.servers[i].generate_failure()
            self.servers[i].generate_maintenance()

        self.sender.start()

    def runDebug(self):
        self.scheduler.run()

        # create output
        print('Queue length distribution:')
        for srv in range(self.numSrv):
            print('Server{}'.format(srv))
            stats = self.servers[srv].arrival_stats()
            S = sum(stats.values())
            for i, v in stats.items():
                print(i, v*1./S)

        for i in range(self.numSrv):
            print('Processed on server{}:'.format(i), self.servers[i].jobsprocessed, 'Arrived:',
                  self.servers[i].jobsarrived, 'Failures: ', self.servers[i].numfailures, 'Maintenance: ',
                  self.servers[i].numMaint)

        print('Cycle time: {:.5} seconds'.format(self.sink.throughput()))

        for i in range(self.numSrv):
            print("Server {} has CT: {:.5} seconds".format(i, np.mean(self.servers[i].ctime)))

        print("Total time taken: {0:.2f} seconds".format(self.sink.totaltime()))

        total = self.sender.totalJobs / self.sink.totaltime()
        print("Total throughput: {} items per second".format(total))

        print('Idle percentage:')
        for server in self.servers:
            print('{0}: {1:.2f}'.format(server.name, server.idletime/self.sink.totaltime()*100), '%')

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

    def run(self):
        self.scheduler.run()
        return self.sink.throughput()
