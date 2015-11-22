import MM1 as sim
from scipy.stats import expon
import matplotlib.pylab as plt
import numpy as np

np.random.seed(1)

scheduler = sim.Scheduler()
now = scheduler.now

labda, mu = 1., 1.3
    
sender = sim.Sender()
sender.setTotalJobs( 1e2 )
sender.setTimeBetweenConsecutiveJobs(expon(scale = 1./labda))
    
queue = sim.Server()
queue.setServiceTimeDistribution(expon(scale = 1./mu))
    
sink = sim.Sink()

sender.Out = queue
queue.In = sender
queue.Out = sink
sink.In = queue

scheduler.register(sender, queue)

sender.start()
scheduler.run()

#T, Q = sink.queueAtArrivalTimes()
#plt.plot(T,Q)
#plt.show()
    



stats = sink.arrivalStats()
    
rho = labda/mu
S = sum(stats.values())
for i, v in  stats.items():
    print(i, v*1./S, (1.-rho)*rho**i)