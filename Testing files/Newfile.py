
from collections import defaultdict
from sortedcontainers import SortedSet
import numpy as np
from scipy.stats import expon
from transitions import Machine
np.random.seed(1)

class Message(object):
    def __init__(self, f, t, time, job = None, message = ""):
        self.f = f  # from node
        self.t = t  # to node
        self.time = time
        self.job = job
        self.message = message

    def __repr__(self):
        return "%10s %10s %7.3f\t%s"%(self.f, self.t, self.time, self.message)

class Scheduler(SortedSet):
    def __init__(self):
        self.sup = super(Scheduler, self)
        self.sup.__init__(key = lambda m: m.time)
        self.supadd = self.sup.add
        self.suppop = self.sup.pop
        self.endOfSimultationTime = np.inf
        self.now = 0

    def add(self, m):
        if now() < self.endOfSimultationTime:
            self.supadd(m)

    def pop(self):
        m = self.suppop(0)
        self.now = m.time
        m.t.receive(m)
        return m

    def Run(self):
        while len(self) and now()<10000:
            m = self.pop()

    def printSelf(self):
        print(self.now)
        for s in self:
            print(s.f, s.t, s.time)

def now():
    return scheduler.now

def schedule(m):
    scheduler.add(m)

class Node(Machine):
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
    def __init__(self, interarrivaltime, totalJobs = 0):
        super(Sender, self).__init__(name = "Sender")

        self.interarrivaltime = interarrivaltime
        self.totalJobs = totalJobs  # total number of jobs
        self.numSentJobs = 0

        self.generateNewJob = Message(self, self, 0, "Generate new job")
        schedule(self.generateNewJob)

    def receive(self, m):
        if m == self.generateNewJob:
            self.send()
            if self.numSentJobs < self.totalJobs:
                self.generateNewJob.time = now() + self.interarrivaltime.rvs()
                schedule(self.generateNewJob)

    def send(self):
        self.numSentJobs += 1
        m = Message(self, self.Out, now(), message = "job: %d"%self.numSentJobs)
        schedule( m )

class FSM(Node):
    def __init__(self, name = ""):
        self.name = name
        state = ['operational', 'unscheduled', 'scheduled']
        Machine.__init__(self,states=state,initial='operational')
        self.add_transition('repair', '*', 'operational',after='generateFailure')
        self.add_transition('breaku','operational','unscheduled')
        self.add_transition('breaks','operational','scheduled')

class Server(FSM):
    def __init__(self, serviceTime, numServers, mtbf, repairtime):
        super(Server, self).__init__("Server")
        self.serviceTime = serviceTime
        self.numServers = numServers
        self.mtbf = mtbf
        self.repairtime = repairtime
        self.numfailures = 0

        self.busyServers = 0

        self.count = defaultdict(int)

    def receive(self, m):
        if "job" in m.message:   # receive new job
            if self.busyServers < self.numServers:
                self.busyServers += 1
                t = now() + self.serviceTime.rvs()
                schedule(Message(self, self, t, job = m, message = "end"))
            else:
                self.queue.add(m)
            self.arrivalStats()
        if "failure" in m.message:
            self.numfailures += 1
            self.breaku()
            t = now() + self.repairtime.rvs()
            schedule(Message(self, self, t, job = m, message = "repair"))
        if "repair" in m.message:
            self.repair()

        elif m.message == "end":
            self.send(m.job)
            if len(self.queue) > 0:
                j = self.queue.pop(0)
                t = now() + self.serviceTime.rvs()
                schedule(Message(self, self, t, job = j, message = "end"))
            else:
                self.busyServers -= 1
            self.departureStats()

    def generateFailure(self, mtbf=expon(scale=1./0.001)):
        tFail = now() + mtbf.rvs()
        m = Message(self, self, tFail, message = "failure")
        schedule( m )
        print('failure',tFail)

    def repair(self):
        self.repair()
        #self.generateFailure(expon(scale = 1./0.1))

    def send(self, m):  # job departure
        m.f, m.t, m.time = self, self.Out, now()
        schedule(m)

    def arrivalStats(self):
        self.count[len(self.queue)] += 1

    def departureStats(self):
        pass

class Fifo(Server):
    def __init__(self, serviceTime, numServers=1, mtbf=1, repairtime=1):
        super(Fifo, self).__init__( serviceTime, numServers, mtbf, repairtime)
        self.queue = SortedSet(key = lambda job: job.time)

class Sink(Node):
    def __init__(self):
        super(Sink, self).__init__("Sink")

    def receive(self, m):
        pass

    def send(self):
        pass

scheduler = Scheduler()

labda, mu = 1., 1.3
interarrivaltime = expon(scale = 1./labda)
serviceTime = expon(scale = 1./mu)
repairtime = expon(scale = 1./0.05)

sender = Sender(interarrivaltime, totalJobs = 1e4)
srv = Fifo(serviceTime, numServers = 1, mtbf = 5, repairtime = repairtime)
sink = Sink()

sender.Out = srv
#queue.In = sender
srv.Out = sink
#sink.In = queue

m = Message(sender, srv, 1, message = "failure")
schedule( m )

scheduler.Run()

rho = labda/mu
S = sum(srv.count.values())
for i, v in  srv.count.items():
    print(i, v*1./S, (1.-rho)*rho**i)
print('failures: ',srv.numfailures)