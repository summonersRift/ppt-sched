#
# job_queue.py :- manages queues of jobs
#

import sys
from operator import itemgetter   #sorting list of lists by a key

class JobQueue(object):
    """An event queue to manage and handle all the events."""

    def __init__(self):
        str_me = "   * JobQueue::__init__() -- "
        print (str_me+"-- creating queues")
        self.running = []                 # queue of running jobs
        self.unscheduled_jobs = []                 # queue of waiting jobs
        self.finished = []                # queue of finished jobs
        self.reservation = []            # queue of reservation jobs/events

        #supported job-queues allowed to submit jobs in, size and runtime estimates queue
        self.queues = ['prod-capability', 'prod-long',  'prod-short','backfill']

        #Causes to terminate a job
        self.termination_causes = ['normal', 'error', 'timeout',
                                   'preemption','reservation']

        # counter to assign unique job_no
        self.previous_job_no = 10000

        #for EasyPlusPlusSheduler
        self.user_run_time_prev = {}
        self.user_run_time_last = {}

 
    def submit(self, job):   ## append a job to `unscheduled` queue
        str_me = "       * JobQueue::submit() --"
        self.previous_job_no += 1
        self.unscheduled_jobs.append(job)

        #For EasyPlusPlusScheduling
        if not self.user_run_time_last.has_key(job.user_id):
            self.user_run_time_prev[job.user_id] = None
            self.user_run_time_last[job.user_id] = None


    def schedule (self, candidate):   ## move `unscheduled` job to `running`
        str_me = "    * JobQueue::schedule() :-"
        #for job in self.unscheduled_jobs:
        #    if job == candidate:
        #        self.running.append(candidate)
        #        self.unscheduled_jobs.remove(job)
        #        print ("%s scheduled job no:%d" % (str_me, candidate.id))
        #        return True
        #raise Exception (str_me + " failed to schedule job id:"+str(candidate.id) )

        ##ASSUMING job already POPPED by scheduler
        self.running.append(candidate)


    def terminate (self, terminated_at, job_no, cause="normal"):
        """App termination either by runtime estimate, by error, or by preemption"""
          
        str_me = "    * JobQueue::terminate() --"
        if cause not in self.termination_causes:
            raise Exception (str_me+" ERROR: terminating without a valid cause")
        print ("%s terminating job_no: %d, cause: %s" % (str_me, job_no, cause) )

        ## POP from running and apend to finished queue
        for job in self.running:

            if  job.id == job_no:      # find matching job with job_no
                job.status = "finished-"+cause
                self.finished.append(job)
                self.running.remove(job)          # pop
                print ("%s terminated job_no:%d cause:%s" % (str_me, job_no, cause))

                # validate starttime < termination time
                #if float(app[10]) < float(terminated_at):
                #    app[11]  = terminated_at - app[10]
                #else:
                #    print ("       * job_no:%d, starttime:%f terminated_at:%f "
                #         % (app[0], get_attr(app,'starttime'), terminated_at ))
                #    print ("       * job_no:%d, start:%d walltimef:%d end:%d,\n job:%s"
                #         % (app[0], app[10], app[5], terminated_at, app ))
                #raise Exception (str_me+ "start time cant be greater than terminate time")

                #For EasyPlusPlus
                assert self.user_run_time_last.has_key(job.user_id) == True
                assert self.user_run_time_prev.has_key(job.user_id) == True
                self.user_run_time_prev[job.user_id] = self.user_run_time_last[job.user_id]
                self.user_run_time_last[job.user_id] = job.actual_run_time #walltimef
                return

        raise Exception (str_me + " failed, no matching job in RUNNING queue. \
                job_no:"+str(job_no)+", running queue:"+ str(self.get_jobs("present")) )

    def get_job_no_by_root_node(self, root_node_id):
        str_me = "   * JobQueue::get_job_no_by_root_node() :-"
        print (str_me + " finding job with root node:" + str(root_node_id))
        for job in self.running:
            if root_node_id in job.hostmap:
                return job.id
        return -1

    def get_job_by_root_node(self, root_node_id):  ## fidn job with supplied node in hostmap
        str_me = "   * JobQueue::get_job_by_root_node() --"
        print (str_me + " finding job with root node:" + str(root_node_id))
        for job in self.running:
            if root_node_id in job.hostmap:
                return job
        return -1

    def get_running_job_by_job_no(self, job_no):
        str_me = "   * JobQueue::get_running_job_by_job_no() :-"
        print (str_me + " finding running job with jon id:" + str(job_no))
        for job in self.running:
            if job.id == job_no:
                return job
        return -1


    def shutdown(self, cause="error"):
        """if there is a memory overflow, we move all waiting jobs to finish with error"""

        str_me = "      * JobQueue::shutdown() --"
        print ( "%s shutting down all unscheduled jobs" % (str_me)  )
        while self.unscheduled_jobs:
            app = self.unscheduled_jobs.pop()
            if app.status == "waiting":           # state update
                app.status = "finished-"+cause
            else:
                raise Exception (str_me+" job must be WAITING to terminate:"+str(app[12]) )
            self.finished.append(app)
        return True


    def get_jobs (self, state="all", timestamp=0):
        if state == "waiting": return self.unscheduled_jobs
        elif state == "running": return self.running
        elif state == "finished": return self.finished
        elif state == "all": return self.unscheduled_jobs + self.running + self.finished
        elif state == "oldest": return self.unscheduled_jobs[0]   #since jobs are sorted by submittime
        else: raise Exception ("   * JobQueue::get_jobs() :- ERROR: invalid state="+state)


    def job_numbers (self, q = "waiting"): ## list of "job_nos" from a queue
        str_me = "   * JobQueue::job_numbers() -- "
        if q == "waiting": return  [job.id for job in self.unscheduled_jobs]
        elif q == "running": return  [job.id for job in self.running]
        elif q == "finished": return  [job.id for job in self.finished]
        else: raise Exception (str_me+"ERROR: invalid state="+q)


    def get_status (self, job_no):  ##returns status of a job_no
        for job in self.running:
            if job.id == job_no:
                return "running" 
        for job in self.unscheduled_jobs:
            if job.id == job_no:
                return "waiting" 
        ## we might need to check if finished or not, for complexity not doing it now
        return "finished"


    @property
    def num_jobs_running (self):
        return len(self.running)

    @property
    def num_jobs_waiting (self):
        return len(self.unscheduled_jobs)

    @property
    def are_jobs_waiting (self):
        if len(self.unscheduled_jobs) > 0: return True
        return False

    @property
    def are_jobs_running (self):
        if len(self.running) > 0: return True
        return False


    def status_all (self, short=True):
        """Returns a summary string with job_no of all jobs and their status"""
        if short == True:
            ret = "\twaiting: " + str(len(self.unscheduled_jobs))
            ret += "\n\tRunning: " + str(len(self.running))
            ret += "\n\tFinished: " + str(len(self.finished))
        else:
            ret = "\twaiting:"    + str([job.id for job in self.unscheduled_jobs]) +\
                  "\n\trunning:"  + str([job.id for job in  self.running]) +\
                  "\n\tfinished:" + str([job.id for job in self.finished])
        return ret


    def write_all_to_file(self, fileroot="_stats_jobq.out", what_else="#"):
        """writes all the queues of jobs to the file"""

        str_me = "       * JobQueue::write_all_to_file() --"
        out = open(fileroot+"_jobq.out", 'w')
        if out is None:
            print ("%s couldnt open file to write. fileroot:%s" %(str_me, fileroot) )
 
        out.write("#-------------------------------------\n")
        out.write("#JobQueue::write_all_to_file() summary\n")
        out.write("#-------------------------------------\n")
        out.write("#unscheduled:" + str([job.id for job in self.unscheduled_jobs])+"\n" )
        out.write("#running:" + str([job.id for job in self.running])+"\n" )
        out.write("#finished:" + str([job.id for job in self.finished])+"\n" )
        out.write("#-------------------------------------\n")
        out.write(what_else)
        out.write("#-------------------------------------\n")

        all_jobs = self.finished+self.unscheduled_jobs + self.running

        out.write("id \tsubmit_time \tstart_time \tend_time\n")
        
        for job in all_jobs:
            #replace executable function with just the name
            #job_short = job  #short format so that trace is readable
            #job_short[6] = job[6].__name__
            out.write("%s \t%s \t%s \t%s\n" 
                       % (job.id, job.submit_time, job.start_time, job.end_time))
        out.write("#EOF")
        out.close()
        print ("%s wrote all queues in: %s" %(str_me, fileroot) )
        return True
