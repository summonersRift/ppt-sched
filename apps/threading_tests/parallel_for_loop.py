#
# omp_for_loop.py :- test parallel for loop construct
#
from sys import path
path.append('../../simian/simian-master/SimianPie')
path.append('../../hardware')
import nodes
import clusters
from simian import Simian 

  
def callback(node, args):
  print("Loop finished executing at time: "+str(node.engine.now))

def run_tasklist(self, msg, *args):
  tasklist1 = ['regular', ['iALU', 10], ['fALU', 40],
                ['VECTOR', 20, 24],
                ['MEM_ACCESS', 4, 10,
                1, 1, 1,
                10, 80, True]]
  critical1 = ['critical',['iALU', 1], ['fALU', 3],
                ['MEM_ACCESS', 1, 2,
                1, 1, 1,
                1, 6, True]]
  tasklist2 = ['regular', ['iALU', 10], ['fALU', 40],
                ['VECTOR', 20, 24],
                ['MEM_ACCESS', 4, 10,
                1, 1, 1,
                10, 80, True]]
  critical2 = ['critical',['iALU', 1], ['fALU', 3],
                ['MEM_ACCESS', 1, 2,
                1, 1, 1,
                1, 6, True]]
  tasklist = [tasklist1, critical1,tasklist2, critical2]
  omp_wrapper = [["parallel_for", 4, 10000000000, "static", True, tasklist,
                  callback, None]]
  time = self.time_compute(omp_wrapper)
  
simName, startTime, endTime, minDelay, useMPI = \
  "for_loop_sim", 0.0, 100000000000.0, 0.1, False

simianEngine = Simian(simName, startTime, endTime, minDelay, useMPI)

cluster = clusters.SingleCielo(simianEngine)
simianEngine.attachService(nodes.Node, "run_tasklist" , run_tasklist)
simianEngine.schedService(0.0, "run_tasklist", "", "Node", 0)
  
simianEngine.run()
print("Simulation finished at time "+str(simianEngine.now))
simianEngine.exit()
