import simulator as sim
# import profile
# from scipy.stats import expon
# import sys
# import matplotlib.pylab as plt
# import numpy as np


#### Input variables for the simulator ####
# All values are stored in lists, where the number of values in the list determines the number of servers
# in the simulator. Make sure that the dimensions of the lists are equal.
labda = 1
mu = [1.1, 1.1]
mtbf = [50, 10000000]
mttr = [1, 1]
mInt = [100, 10000000]
mTime = [1, 1]
maxqueue = [5, 5]
nrJobs = 1e4

### Use this code for a single debuging run ####
experiment = sim.Simulator(nrJobs, maxqueue, labda, mu, mtbf, mttr, mInt, mTime)
experiment.runDebug()



#### Use this code for comparing experimental results ####
# valuesMTBF = [30, 50, 100, 200, 300, 500]
# valuesMTTR = [1, 2, 3, 5, 10]
#
# results = []
# temp = []
# for j in valuesMTBF:
#     for i in valuesMTTR:
#         print('run ', i)
#         experiment = sim.Simulator(1e4, 10, labda, mu, mtbf=j, mttr=i, mInt=100, mTime=2)
#         temp.append(experiment.run())
#     results.append(temp)
#
# print(results)
