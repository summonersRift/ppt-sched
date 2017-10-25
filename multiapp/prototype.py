#! /usr/bin/env python

import sys
class Job(object):
    def __init__(self, id, user_estimated_run_time, actual_run_time, num_required_processors, \
            submit_time=0, admin_QoS=0, user_QoS=0, user_id=0): # TODO: are these defaults used?

        assert num_required_processors > 0, "job_id=%s"%id
        assert actual_run_time > 0, "job_id=%s"%id
        assert user_estimated_run_time > 0, "job_id=%s"%id

        self.id = id
        self.user_estimated_run_time = user_estimated_run_time
        self.predicted_run_time = user_estimated_run_time  
        self.actual_run_time = actual_run_time
        self.num_required_processors = num_required_processors
	self.user_id = user_id
        
        # not used by base
        self.submit_time = submit_time # Assumption: submission time is greater than zero
        self.start_to_run_at_time = -1 # TODO: convert to None

        # the next are essentially for the MauiScheduler
        self.admin_QoS  = admin_QoS # the priority given by the system administration
        self.user_QoS   = user_QoS # the priority given by the user
        self.maui_bypass_counter = 0
        self.maui_counter = 0

        # the next is for the look ahead scheduler
        self.backfill_flag = 0

        # the next is for the probabilistic easy scheduler
        self.expected_predicted_run_time = user_estimated_run_time 

        
    @property
    def finish_time(self):
        assert self.start_to_run_at_time != -1
        return self.start_to_run_at_time + self.actual_run_time
    
    @property
    def predicted_finish_time(self):
        assert self.start_to_run_at_time != -1
        return self.start_to_run_at_time + self.predicted_run_time

    def __repr__(self):
        return type(self).__name__ + "<id=%(id)s, user_estimated_run_time=%(user_estimated_run_time)s, actual_run_time=%(actual_run_time)s, required_processors=%(num_required_processors)s, start_to_run_at_time=%(start_to_run_at_time)s, submit_time=%(submit_time)s>" % vars(self)



def parse_job_lines_quick_and_dirty(lines):
    """
    parses lines in Standard Workload Format, yielding pairs of (submit_time, <Job instance>)

    This should have been:

      for job_input in workload_parser.parse_lines(lines):
        yield job_input.submit_time, _job_input_to_job(job_input)

    But instead everything is hard-coded (also hard to read and modify) for
    performance reasons.

    Pay special attention to the indices and see that you're using what you
    expect, check out the workload_parser.JobInput properties and
    _job_input_to_job comments to see extra logic that isn't represented here.
    """
    for line in lines:
        x = line.split()
        yield int(x[1]), Job(
            id = int(x[0]),
            user_estimated_run_time = int(x[8]),
            actual_run_time = int(x[3]),
            num_required_processors = max(int(x[7]), int(x[4])), # max(num_requested,max_allocated)
        )

def _job_input_to_job(job_input, total_num_processors):
    # if job input seems to be problematic  
    if job_input.run_time <= 0 or job_input.num_allocated_processors <= 0 or job_input.submit_time < 0 :
        return Job(
            id = job_input.number,
            user_estimated_run_time = 1,
            actual_run_time = 1, 
            num_required_processors = max(1, job_input.num_allocated_processors),  
            submit_time = max(job_input.submit_time, 1), 
            user_id = job_input.user_id, 
        )

    return Job(
        id = job_input.number,
        user_estimated_run_time = int(max(job_input.requested_time, job_input.run_time, 1)),
        actual_run_time = int(max(min(job_input.requested_time, job_input.run_time), 1)), 
        num_required_processors = max(min(job_input.num_allocated_processors, total_num_processors), 1), 
        submit_time = job_input.submit_time, 
        user_id = job_input.user_id, 
    )

def _job_inputs_to_jobs(job_inputs, total_num_processors):
    #print "_job_inputs_to_jobs :", job_inputs
    for job_input in job_inputs:
        yield _job_input_to_job(job_input, total_num_processors)


def simple_job_generator(num_jobs):
    import random
    start_time = 0
    for id in xrange(num_jobs):
        start_time += random.randrange(0, 15)
        yield start_time, Job(
            id=id,
            user_estimated_run_time=random.randrange(400, 2000),
            actual_run_time=random.randrange(30, 1000),
            num_required_processors=random.randrange(2,100),
        )
