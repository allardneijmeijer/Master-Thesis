import mm1sf as sim
from scipy.stats import expon
# import matplotlib.pylab as plt
# import numpy as np

labda, mu = 1., 1.3
arrive = expon(scale=1./labda)
serve = expon(scale=1./mu)
experiment = sim.Simulator(1e4, 10, labda, mu, 10, 1, 0)
experiment.run()
