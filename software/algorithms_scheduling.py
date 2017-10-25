#
# algorithms_scheduling.py :- all the queue servicing 
#

import sys

class SchedulingAlgorithm (object):
    algorithm = None
    def __init__(self, sched_dict, resource_manager):
        self.algorithm = sched_dict['scheduling']
        self.resource_manger = resource_manager
        self.sched_dict = sched_dict

    def enumerate_free_testshot(self, num_nodes):
         hostmap = [i for i in xrange(num_nodes)]

    #def find() -- derived class must overload function

    
            
            
class FCFS (SchedulingAlgorithm):
    """Random Mapping Class"""

    def __init__(self, sched_dict, res_man):
        SchedulingAlgorithm.__init__(self, sched_dict, res_man)

        str_me = "    * SchedulingAlgorithm FCFS::__init__() --"
        print (str_me+" initialized")

    def find_next (self, jobs):
        """oldest job"""

        str_me = "    * SchedulingAlgorithm FCFS::find_next() --"
        print (str_me+" initialized")

        #get job with smallest submittime
        candidate = jobs[0]
        min_submittime = get_attr(candidate, "submittime")

        for job in jobs:
            if get_attr(job, "submittime") <  min_submittime:
                candidate = job
                min_submittime = get_attr(candidate, "submittime")

        if candidate == None:
            raise Exception (str_me+"No matching candidate FCFS job ")
        return candidate

 
class LJF (SchedulingAlgorithm):
    """Longest Job First -LJF discipline , based on node-hours """
    """Random Mapping Class"""

    def __init__(self, sched_dict, res_man):
        SchedulingAlgorithm.__init__(self, sched_dict, res_man)

        str_me = "    * SchedulingAlgorithm LJF::__init__() --"
        print (str_me+" initialized")

    def find_next (self, jobs):
        """oldest job"""

        str_me = "    * SchedulingAlgorithm LJF::find_next() --"
        print (str_me+" finding candidate")

        
        max_node_hours = -1
        candidate = None
        c = self.sched_dict['cores_per_node']

        for job in jobs:
            node_hours = (job[1]/c) * (get_attr(job, "walltimef")/3600.0)
            if node_hours > max_node_hours:
                max_node_hours = node_hours
                candidate = job

        if candidate == None:
            raise Exception (str_me+"No matching candidate LJF-LargestJobFirst ")

        return candidate


class SJF (SchedulingAlgorithm):
    """Shortest Job First (SJF) scheduling , based on node-hours """

    def __init__(self, sched_dict, res_man):
        SchedulingAlgorithm.__init__(self, sched_dict, res_man)

        str_me = "    * SchedulingAlgorithm SJF::__init__() --"
        print (str_me+"initialized")

    def find_next (self, jobs):
        str_me = "    * SchedulingAlgorithm SJF::find_next() --"

        # find node hours for all jobs
        min_node_hours = sys.maxint
        candidate = None
        c = self.sched_dict['cores_per_node']

        for job in jobs:
            node_hours = (job[1]/c) * (get_attr(job, "walltimef")/3600.0)
            if node_hours < min_node_hours:
                min_node_hours = node_hours
                candidate = job

        if candidate == None:
            raise Exception (str_me+"No matching candidate SJF-LargestJobFirst ")

        return candidate


class  PriorityQueue (SchedulingAlgorithm):
    """Policy based priority queues, 0 gets highest and 9 gets lowest priority"""

    #dict_queue = {"prod-capability": {'priority': 1, 'size':4096, 'runtime':86400}, 
    #              "prod-long"      : {'priority': 2, 'size':512, 'runtime':6*3600}, 
    #              "prod-short"     : {'priority': 3, 'size':0, 'runtime':0}, 
    #              "backfill"       : {'priority': 9, 'size':0, 'runtime':0}, 
    #             }

    def __init__(self, sched_dict, res_man):
        SchedulingAlgorithm.__init__(self, sched_dict, res_man)

        str_me = "    * SchedulingAlgorithm PriorityQueue::__init__() --"
        print (str_me+"initialized")


    def find_next (self, jobs):
        str_me = "    * SchedulingAlgorithm PriorityQueue::__init__() --"

        #get lowest queue # of all jobs
        min_queue = min( [get_attr(job, "queue") for job in jobs] )

        #candidates on min_queue
        candidates = [job for job in jobs if get_attr(job, "queue") ==  min_queue ]

        if len(candidates) <1:
            raise Exception (str_me+" no candidates to find priority job")

        #FIFO in the priority queue
        candidate = candidates[0]
        min_submittime = get_attr(candidate, "submittime")

        for job in jobs:
            if get_attr(job, "submittime") <  min_submittime:
                candidate = job
                min_submittime = get_attr(candidate, "submittime")

        return candidate


class  ScoreBased (SchedulingAlgorithm):
    """Shortest Job First (SJF) Scheduling"""

    def __init__(self, sched_dict, res_man):
        SchedulingAlgorithm.__init__(self, sched_dict, res_man)

        str_me = "    * SchedulingAlgorithm ScoreBased::__init__() --"
        print (str_me+"initialized")

    def find_next (self, jobs):
        str_me = "    * SchedulingAlgorithm ScoreBased::find_next() --"
        print (str_me+" NOT DEFINED")


class EasyPlusPlus(SchedulingAlgorithm):
    """ This algorithm implements the algorithm in the paper
        of Tsafrir, Etzion, Feitelson, june 2007?
    """
    def __init__(self, sched_dict, res_man,):
        SchedulingAlgorithm.__init__(self, sched_dict, res_man,jobq)
        str_me = "    * SchedulingAlgorithm FCFS::__init__() --"

        #def __init__(self, num_processors):
        #num_processors = res_man.num_free_cores()
        #super(EasyPlusPlusScheduler, self).__init__(num_processors)
        #self.cpu_snapshot = CpuSnapshot(num_processors)
        #self.unscheduled_jobs = []
        self.jobq = jobq

        print (str_me+" initialized")


    def find_next (self, current_time):
        "Schedules jobs that can run right now, and returns them"

        result = []

        for job in jobs:
            user_estimated_run_time = job[4]
            job_user_id = job[8]

            if self.jobq.user_run_time_prev[job_user_id] != None:
                average = int((self.user_run_time_last[job_user_id] 
                             + self.user_run_time_prev[job_user_id]) / 2)
                job[16] = min(user_estimated_run_time, average)  #predicted_run_time

        #def _backfill_jobs(self, current_time):
        first_job = self.jobq.waiting[0]
        tail = list_copy(self.jobq.waiting[1:])
        tail_of_jobs_by_sjf_order = sorted(tail, key=sjf_sort_key)


        self.createTentativeSlice(first_job,current_time)
        #self.cpu_snapshot.assignJobEarliest(first_job, current_time)

        for job in tail_of_jobs_by_sjf_order:
            if self.cpu_snapshot.canJobStartNow(job, current_time):
                self.unscheduled_jobs.remove(job)
                self.cpu_snapshot.assignJob(job, current_time)
                result.append(job)

        self.cpu_snapshot.delJobFromCpuSlices(first_job)

        return result


    def get_backfill_size():
        num_free_cores = self.resource_manager.num_free_cores()
        

    #def new_events_on_job_under_prediction(self, job, current_time):
    #    assert job.predicted_run_time <= job.user_estimated_run_time
    #    self.cpu_snapshot.assignTailofJobToTheCpuSlices(job)
    #    job.predicted_run_time = job.user_estimated_run_time
    #    return []


# shortest job first 
sjf_sort_key = (
    lambda job :  job.predicted_run_time
)

