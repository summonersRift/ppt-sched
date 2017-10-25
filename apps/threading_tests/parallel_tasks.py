#
# omp_for_loop_simple.py :- test simple parallel for loop construct
#
from sys import path
path.append('../../simian/simian-master/SimianPie')
path.append('../../hardware')
import nodes
import clusters
from simian import Simian 

def callback(node, args):
  print("Tasks finished executing at time: "+str(node.engine.now))

def run_tasklist(self, msg, *args):
  task1 = ["task" ,['iALU', 10], ['fALU', 800],
                ['VECTOR', 20, 24],
                ['MEM_ACCESS', 4, 10,
                1, 1, 1,
                10, 1600, True]]
  task2 = ["task" ,['iALU', 10], ['fALU', 2400],
                ['VECTOR', 20, 24],
                ['MEM_ACCESS', 4, 10,
                1, 1, 1,
                10, 4800, True]]
  task3 = ["task" ,['iALU', 10], ['fALU', 400],
                ['VECTOR', 20, 24],
                ['MEM_ACCESS', 4, 10,
                1, 1, 1,
                10, 200, True]]
  task4 = ["task" ,['iALU', 10], ['fALU', 1200],
                ['VECTOR', 20, 24],
                ['MEM_ACCESS', 4, 10,
                1, 1, 1,
                10, 2400, True]]
  task5 = ["task" ,['iALU', 10], ['fALU', 6000],
                ['VECTOR', 20, 24],
                ['MEM_ACCESS', 4, 10,
                1, 1, 1,
                10, 12000, True]]
  task6 = ["task" ,['iALU', 10], ['fALU', 8000],
                ['VECTOR', 20, 24],
                ['MEM_ACCESS', 4, 10,
                1, 1, 1,
                10, 16000, True]]
  tasklist = [task1,task2,task3,task4,task5,task6]
  tasklist += tasklist
  tasklist += tasklist
  tasklist += tasklist
  tasklist += tasklist
  tasklist += tasklist
  tasklist += tasklist
  tasklist += tasklist
  print("Starting computations of "+str(len(tasklist))+" tasks")
  wrapper = [["parallel_tasks", 16, tasklist, callback, None]]
  time = self.time_compute(wrapper)
  

simName, startTime, endTime, minDelay, useMPI = \
  "parallel_tasks_sim", 0.0, 100000000000.0, 0.1, False

simianEngine = Simian(simName, startTime, endTime, minDelay, useMPI)

cluster = clusters.SingleCielo(simianEngine)
simianEngine.attachService(nodes.Node, "run_tasklist" , run_tasklist)
simianEngine.schedService(0.0, "run_tasklist", "", "Node", 0)
  
simianEngine.run()
simianEngine.exit()
print 'Computations finished at time', simianEngine.now
