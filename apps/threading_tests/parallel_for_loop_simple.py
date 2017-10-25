#
# omp_for_loop_simple.py :- test simple parallel for loop construct
#
from sys import path
path.append('../../simian/simian-master/SimianPie')
path.append('../../hardware')
import nodes
import clusters
from simian import Simian 


def run_tasklist(self, msg, *args):
  tasklist = [['iALU', 10], ['fALU', 80],
                ['VECTOR', 20, 24],
                ['MEM_ACCESS', 4, 10,
                1, 1, 1,
                10, 300, True]]
  omp_wrapper = [["parallel_for", 32, 100000000, "static", False, tasklist]]
  time = self.time_compute(omp_wrapper)
  print 'Computations finished at time',time
  

simName, startTime, endTime, minDelay, useMPI = \
  "for_loop_sim", 0.0, 100000000000.0, 0.1, False

simianEngine = Simian(simName, startTime, endTime, minDelay, useMPI)

cluster = clusters.SingleCielo(simianEngine)
simianEngine.attachService(nodes.Node, "run_tasklist" , run_tasklist)
simianEngine.schedService(0.0, "run_tasklist", "", "Node", 0)
  
simianEngine.run()
simianEngine.exit()
