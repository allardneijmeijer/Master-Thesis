import multipleservers as sim
from scipy.stats import expon
import sys
# import matplotlib.pylab as plt
# import numpy as np

#sys.stdout = open('results.txt', 'w')
labda =1
mu = [1.3, 1.3, 1.3, 1.3]
mtbf = [1000, 1000, 1000, 1000]
mttr = [1, 1, 1, 1]
mInt = [1000, 1000, 1000, 1000]
mTime = [1, 1, 1, 1]
maxqueue =[5, 5, 5, 5]

#arrive = expon(scale=1./labda)
#serve = expon(scale=1./mu)
experiment = sim.Simulator(1e4, labda, mu, mtbf, mttr, mInt, mTime)
experiment.runDebug()

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
