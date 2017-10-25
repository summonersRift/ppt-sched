#
# scheduler.py :- schedules submissions
#
# scheduler software model: is an object created in the JobSchedulerNode(entity)
# job_schedule_proc: schedules jobs and manages timing
#
#
from job_queue import *
from resource_manager import *
from algorithms_mapping import *
from algorithms_scheduling import *
#from application import *

from sys import path
path.append("../")
from ppt import *
import re
import random

#for debugging memory
#import psutil
#from guppy import hpy



class Scheduler (object):
    """Software model for scheduler node"""

    #def __init__(self, hpcsim_dict, sched_dict, apps=[], **params):
    def __init__(self, hpcsim_dict, sched_dict, **params):
        """initializing job scheduler with jobq, defaults"""

        str_me = "  * Scheduler::__init__() --"
        print (str_me+"initializing scheduler")
        self.debug_me = False

        self.hpcsim_dict = hpcsim_dict

        # file to write the final job queue entries
        if "fileroot" not in sched_dict:
            self.fileroot = None
        self.fileroot = sched_dict['fileroot']

        # initiate JobQueue data structure with empty queues
        self.jobq = JobQueue()

        # initiate ResourceManager with all resources as free
        cores_per_node = 1
        if "cores_per_node" in sched_dict:
            cores_per_node = int(sched_dict['cores_per_node'])

        #create resource manger
        self.resource_manager = ResourceManager(hpcsim_dict['intercon'].num_hosts(), cores_per_node)


        # time to process a job job_schedule_proc
        self.job_processing_time = 0.000001             # 1 nano sec here
        self.stats = []
        self.num_finished = 0
        self.sem_scheduler_busy = False              # for appending to terminate_next

        # default service discipline is fsfs
        if 'scheduling' not in sched_dict:
            sched_dict['scheduling'] = "fcfs"
 
        # default service discipline is random-node (worst)
        if 'mapping' not in sched_dict:
            sched_dict['mapping'] = "random-node"

        #initialize job service discipline and mapping/placement algorithm
        self.scheduling = str(sched_dict['scheduling']).lower()
        self.mapping  = str(sched_dict['mapping']).lower()


        # create job_scheduler :- instance of service disciplines
        print ("%s service discipline=%s" % (str_me, self.scheduling))
        if self.scheduling   == 'fcfs' :
            self.job_scheduler = FCFS(sched_dict, self.resource_manager)
        elif self.scheduling == 'sjf':
            self.job_scheduler = SJF(sched_dict, self.resource_manager)
        elif self.scheduling == 'ljf':
            self.job_scheduler = LJF(sched_dict, self.resource_manager)
        elif self.scheduling == 'queue-priority':
            self.job_scheduler = PriorityQueue(sched_dict, self.resource_manager)
        elif self.scheduling == 'score':
            self.job_scheduler = ScoreBased(sched_dict, self.resource_manager)
        else: 
            raise Exception (str_me +" unsupported discipline " + str(self.scheduling))


        # create mapper :- instance of placement algorithm
        print (str_me +" placement algorithm=" + self.mapping)
        if "random" in self.mapping:
            self.mapper = RandomMapping(sched_dict, self.resource_manager)
        elif "round-robin" in self.mapping :
            #self.mapper = RoundRobinMapping(sched_dict, self.resource_manager)
            self.mapper = RoundRobinMapping(sched_dict, hpcsim_dict, self.resource_manager)
        elif "double-ended" in self.mapping :
            self.mapper = DoubleEndedMapping(sched_dict, hpcsim_dict, self.resource_manager)
        elif "machine-learning" in self.mapping :
            self.mapper = MachineLearningMapping(sched_dict,  hpcsim_dict, self.resource_manager)
        else: 
            raise Exception (str_me +" unsupported mapping mapper " + str(self.mapping))
        
        #ssave sched_dict
        self.sched_dict = sched_dict

        # satitizing and submitting applications to jobq.waiting
        #self.jobq.submit_workload(apps) 

        print ( str_me+" finished initializing. Status --\n" + str(self.jobq.status_all())  )


    def schedule_old(self):
        str_me = " * sched_soft::schedule() --"
        processing_delay = 0.0
        
        while True:
            processing_delay += self.job_process_time

            num_free_cores = self.resource_manager.num_free_cores()

            candidates = [d for d in self.jobq.unscheduled_jobs if d[1] <= num_free_cores]
            if  len(candidates) == 0:
                break

            #find best candidate
            candidate = self.job_scheduler.find_next(candidates)
            print  ("%s selected job_no:%d, size: %d" % (str_me, candidate[0], candidate[1]))
        
            #find mapping -- machine learning, finds best of candidate(s)
            hostmap = self.mapper.find(candidate[1], get_attr(candidate, "partitions") )
            if len(hostmap) <1 : #!= get_attr(candidate, "size"):
                raise Exception ("Hostmap empty"+ job_pretty(candidate) )

            #update candidate and jobq
            start = self.sched_node.get_now() + processing_delay
            candidate = set_attr(candidate, "hostmap", hostmap)
            candidate = set_attr(candidate, "starttime", start)
            self.jobq.schedule(candidate)


            # run the applcaition executable on the hostmap
            job_no     = candidate[0]
            walltimef  = get_attr(candidate, "walltimef")
            executable = get_attr(candidate, "executable")
            args       = walltimef, get_attr(candidate, "args")      # pattern/list of *args
            if walltimef < 0:  raise Exception (" * scheduler.py::launch walltimef absent")

            # statistics collection [time, free_cores, num_jobs_running]
            num_free_cores = self.resource_manager.num_free_cores()
            self.stats.append([start, num_free_cores, self.jobq.num_jobs_running])

            # schedule start evt
            for idx in range(len(hostmap)):
                #if 0 <= hostmap[idx] < self.intercon.num_hosts():
                data = {
                        "rank"      : idx,       #obaida --  this is IMPORTANT
                        "args"      : args,
                        "mpiopt"    : self.hpcsim_dict.get('mpiopt',{}),
                        "main_proc" : executable,
                        "hostmap"   : hostmap,
                }
                self.sched_node.reqService(self.job_process_time, "create_mpi_proc", data, "Host", hostmap[idx])

            #schedule timeout evt
            endtime    = start+ walltimef
            
            #main_proc = "%s" % (executable)
            data = {
                    "job_no"    : job_no,
                    "endtime"   : endtime,
                    "hostmap"   : hostmap,
                    "main_proc" : executable["app"],
            }
            self.sched_node.reqService(walltimef+processing_delay, "job_timeout_evt", data,"JobScheduler", 0)


    #def _schedule_jobs(self):
    def schedule(self):
        str_me = " * sched_soft::schedule() --"
        processing_delay = 0.000001
        
        current_time = self.sched_node.get_now()
        free_cores = self.resource_manager.num_free_cores()

        for job in self.jobq.unscheduled_jobs:
            if self.jobq.user_run_time_prev[job.user_id] != None:
                average =  int((self.jobq.user_run_time_last[job.user_id]
                                + self.jobq.user_run_time_prev[job.user_id])/ 2)
                job.predicted_run_time = min (job.user_estimated_run_time, average)


        ## _schedule_head_of_list()  --pyss equivalent
        while True:
            if len(self.jobq.unscheduled_jobs) == 0:
                break
            # first job can't be scheduled
            if self.resource_manager.num_free_cores() < self.jobq.unscheduled_jobs[0].num_required_processors:
                break
            else: # Try to schedule the first job  -- this is FCFS
                job = self.jobq.unscheduled_jobs.pop(0)
                #self.cpu_snapshot.assignJob(job, current_time)
                #print " * schedule() selected job with id: ", job.id
                self.execute(job, current_time + self.job_processing_time)

        ## CALL backfill, save a function call
        if  len(self.jobq.unscheduled_jobs)>0:
            self._backfill_jobs(current_time)

    def _backfill_jobs(self, current_time):
        """BACKFILLING"""

        #print " * attempting to backfill from %d jobs" % len(self.jobq.unscheduled_jobs)
        # if there is no `unscheduled_jobs`
        if  len(self.jobq.unscheduled_jobs)==0:
            return

        raise Exception(" * attempting to backfill")
        ##find BACKFILL window
        first_job = self.jobq.unscheduled_jobs[0]
        num_free_cores = self.resource_manager.num_free_cores()
        need = first_job.num_required_processors - num_free_cores

        ## find earliest time when first_job can start
        jobs_end_time_first_order = list_copy(self.jobq.running)
        jobs_end_time_first_order = sorted(jobs_end_time_first_order, key=end_time_sort_key)
        while need > 0:
            job = jobs_end_time_first_order.pop(0)
            need -= job.num_required_processors
            first_job_predicted_start_time = job.end_time
        del jobs_end_time_first_order

        ## BACKFILLable jobs
        tail = list_copy(self.jobq.unscheduled_jobs[1:])
        tail_of_jobs_by_sjf_order = sorted(tail, key=sjf_sort_key)

        for job in tail_of_jobs_by_sjf_order:
            if current_time+job.predicted_run_time < first_job_predicted_start_time:
                self.jobq.unscheduled_jobs.remove(job)
                self.execute(job, current_time+self.job_processing_time)
                raise Exception (" * just backfilled a job")



    def execute(self, candidate, start_time):
        str_me = " * sched_soft::execute() :-"
        print  ("%s selected job_no-%d, size-%d"
                % (str_me, candidate.id, candidate.num_required_processors))

        #find mapping -- machine learning, finds best of candidate(s)
        candidate.hostmap = self.mapper.find(candidate.num_required_processors)
        if len(candidate.hostmap) <1 :
            raise Exception ("Hostmap empty"+ job_pretty(candidate) )

        #update candidate and jobq
        candidate.start_time =  start_time
        candidate.end_time= start_time + candidate.actual_run_time
        args = candidate.actual_run_time, candidate.args

        self.jobq.schedule(candidate)

        # run the applcaition executable on the hostmap
        if candidate.actual_run_time< 0:  raise Exception (" * scheduler.py::launch walltimef absent")

        # statistics collection [time, free_cores, num_jobs_running]
        num_free_cores = self.resource_manager.num_free_cores()
        self.stats.append([start_time, num_free_cores, self.jobq.num_jobs_running])

        ## run application on the hardware model
        for idx in range(len(candidate.hostmap)):
            #if 0 <= hostmap[idx] < self.intercon.num_hosts():
            data = {
                    "rank"      : idx,       #obaida --  this is IMPORTANT
                    "args"      : args,
                    "mpiopt"    : self.hpcsim_dict.get('mpiopt',{}),
                    "main_proc" : candidate.app,
                    "hostmap"   : candidate.hostmap,
            }
            self.sched_node.reqService(self.job_processing_time, "create_mpi_proc", data, "Host", candidate.hostmap[idx])

        ## KILL event scheule
        #main_proc = "%s" % (executable)
        data = {
                "job_no"    : candidate.id,
                "endtime"   : candidate.end_time,
                "hostmap"   : candidate.hostmap,
                "main_proc" : candidate.app,
        }
        self.sched_node.reqService(candidate.actual_run_time+self.job_processing_time, "job_timeout_evt", data,"JobScheduler", 0)


    #%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
    def terminate(self, endtime, cause, argX):
        """helper method invoked by mpi_terminate to add a node to terminate"""
        
        str_me = "   * scheduler::terminate()) --"
        job_no = -1  #when timout we get job_no
        if cause =="normal":
            job_no = self.jobq.get_job_no_by_root_node(argX)
        elif cause == "timeout":
            job_no = int(argX)
        else:
            raise Exception(str_me+" invalid termination cause:"+cause)
        if job_no == -1:  
            print "%s endtime:%f, now:%f" %(str_me, endtime, self.sched_node.get_now() )
            return #already killed?
            #raise Exception (str_me+" non existent job.") 

        job = self.jobq.get_running_job_by_job_no (job_no)
        if job == -1:
            print "%s endtime:%f, now:%f" %(str_me, endtime, self.sched_node.get_now() )
            raise Exception (" job is not running")
            

        print ("%s job_no:%d, endtime:%f, now:%f" 
               %(str_me, job_no, endtime, self.sched_node.get_now())  ) 
        #deallocate resources
        nodes_used =  []
        for d in job.hostmap:
            if d not in nodes_used:
                nodes_used.append(d)
        self.resource_manager.free(nodes_used)
        self.jobq.terminate(endtime, job_no, cause)
        self.num_finished += 1
        self.schedule()

    
    #%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
    def write_stats(self):
        """write : time, num_free_cores, len_runningQ"""

        if self.fileroot is not None:
            meta_txt  = "#Mapping Algorithm     :"+str(self.mapping) + "\n"
            meta_txt += "#Scheduling Discipline :"+str(self.scheduling) + "\n"
            meta_txt += "#num_finished  :"+str(self.num_finished)+"\n"
            self.jobq.write_all_to_file(self.fileroot, meta_txt)

        outfile = open(self.fileroot+"_scheduler.out", 'w')
        outfile.write(meta_txt)
        outfile.write("#time\tnum_free_cores\tnum_jobs_running\n")
        for record in self.stats:
            line = ""
            for field in record:
                line += str(field) + "\t"
            outfile.write( line.rstrip() + "\n" )
        outfile.close()


#%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
def job_schedule_proc(this, *args):
    if shutdown_bug:                   # stop scheduling when  <500MB memory left
        values = psutil.virtual_memory()
        mem_free_mb = values.available >> 20
        if mem_free_mb < 500L:
            jobq.shutdown()            # move jobs  waiting->finished-error
            print ("%s after shutdown jobs scheduled:%d, mem_free(MB):%d"
                    % (str_me, num_scheduled, mem_free_mb ) )
            #break
        num_scheduled += 1
    if ( debug_mem and (num_scheduled%10 == 0) ): #guppy memory debug
        print " * debug_mem, num jobs scheduled:",num_scheduled,", hp.heap():\n"
        debug_mem(hp_start)


#%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
def debug_mem(hp_start=None):
    hp = hpy()
    hp_after = hp.heap()
    hp_leftover =  hp_after - hp_start
    print "   * hp_start:\n", hp_start
    print "   * hp_leftover after x jobs:\n", hp_leftover
    hrefcount = hp_leftover.byrcs
    print "   * hp_leftover byrcs:\n", hrefcount
    raise Exception ("scheduler.py::debug_mem() stopped for debugging")

def job_pretty(job, short=True):
    """function to return a summary about a job for prints"""
    ret = ""
    for field in job:
        if short and (type(field) is list):  #discarding hostmap
            continue
        ret += " "+str(field)
    return ret

# shortest job first 
sjf_sort_key = (
    lambda job :  job.predicted_run_time
)

# shortest job first
end_time_sort_key = (
    lambda job :  job.end_time
)

def list_copy(my_list):
        result = []
        for i in my_list:
            result.append(i)
        return result

def list_print(my_list):
        for i in my_list:
            print i
        print

class Slice(object):
    def __init__(self, job_no, start, end, size):
        self.job_no, self.size = job_no, size
        self.start, self.end   = start, end
