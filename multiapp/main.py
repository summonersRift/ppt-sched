#
# main.py :- multi app modeling on scheduler using simian-ppt
#

import gc, sys, time, random

sys.path.append('../')
sys.path.append('../apps/util')
sys.path.append('../apps/library')
from ppt import *
# sys.path.append("pyss/")

# parallel workload parser

# importing hpc applications implemented
from app_stencil import stencil
from app_snapsim import snap_process
from app_communication import app_empty, simple_communications, app_one_way
from app_background import background, background_empty, foreground_empty
# pyss
from prototype import _job_inputs_to_jobs
from workload_parser import parse_lines

# util
from size_to_topo import balanced_cube

if __name__ == "__main__":  # main()
    """main function of scheduling and mapping"""

    ##DEFAULT configuration
    scheduling      = "fcfs"
    mapping         = "round-robin-node"
    cartesian       = "1,1,1"
    partitions      = 0
    cores_per_node  = 1
    #input_file = "data/ANL-Intrepid-2009-1200"
    #input_file = "data/LLNL-Atlas-2006-2-April12007-April152007"
    input_file      = "data/sdsc_sp2_last_10k"
    pattern         = "empty"
    
    #foreground application configs
    foreground = "none/0"
    # foreground_duration = 1
    total = 20  # num of forgrounds to inject
    runtime = 3600

    ##ARGS from user
    if len(sys.argv[1:]) == 0:
        print " * main.py:- no paramenters supplied."
    elif len(sys.argv[1:]) > 5:
        # if len(sys.argv)<6: raise Exception "Not enough argv:", sys.argv
        scheduling  = sys.argv[1]
        mapping     = sys.argv[2]
        input_file  = sys.argv[3]
        pattern     = sys.argv[4]
        cartesian   = sys.argv[5]
        partitions  = sys.argv[6]
        if (pattern) == "dense-comm":
            params['execs'] = sys.argv[7]
        elif "foreground" in sys.argv or "background" in sys.argv:
            foreground = sys.argv[7]
            params['timing'] = sys.argv[8]
    else:
        raise Exception("main.py -- invalid list of arguments:" + str(sys.argv))
    print (" * main.py:- \n\t*scheduling:%s, mapping:%s, "
           "\n\t* cartesian:%s, partitions:%s, cores_per_node:%s"
           "\n\t* input_file:%s, pattern:%s" %
           (scheduling, mapping, cartesian, cores_per_node, partitions, input_file, pattern))
    ##CHECK
    valid_patterns = ["empty", "first-last", "ring", "bipartite", "allreduce",
                      "broadcast", "one-way", "dense-comm", "allreduce-sync",
                      "stencil", "snapsim", "phase-based",
                      "background-foreground", "foreground-only", "background-timer"]
    if pattern not in valid_patterns:
        raise Exception(" * main.py::main() unsupported application:", pattern)

    ##CONFIGURE
    fileroot = scheduling + "_" + mapping + "_" + pattern + "_" + input_file.split("/")[-1] + "_" + cartesian + "_"
    fileroot += str(partitions) + "_" + foreground.split("/")[0] + "-" + foreground.split("/")[1]
    cartesian = [int(d) for d in cartesian.split(",")]

    model_dict = {
        # simian parameters (these are required unless explicitly
        # specified when instantiating Cluster)
        "model_name": "multiapp_scheduling",  # name of the model
        "sim_time": 1e9,  # simulation time
        "use_mpi" : False, # whether using mpi for parallel
        #"use_mpi": True,  # whether using mpi for parallel
        "mpi_path": "/usr/lib/x86_64-linux-gnu/libmpich.so",  # ubuntu-14@vega

        "intercon_type": "Gemini",  # IMPORTANT: type is case sensitive
        # standardized interconnect models
        # "torus" : configs.hopper_intercon,
        # "torus" : configs.cielo_intercon,
        # "torus" : configs.sdsc_sp2_intercon,
        # "torus" : configs.titan_intercon,
        "torus": configs.cielo_intercon_deterministic,

        # compute node parameters; IMPORTANT: type is case sensitive
        "host_type": "CieloNode",  # cielo with or without mpi installed
        # "host_type" : "TTNNode",  # cielo with or without mpi installed
        # "host_type" : "Host",       # generic compute node with or without mpi installed

        # each host type can have a distinct configuration like
        # interconnect (not yet implemented)

        # optional libraries/modules to be loaded onto compute nodes
        # "load_libraries": set([]),
        "load_libraries": set(["mpi"]),  # IMPORANT: lib names are case sensitie
        # "debug_options": set(["hpcsim", "intercon", "torus", "mpi-recv", "host"]),
        # "debug_options": set(["intercon", "mapping"]),  #obaida expt
        # "debug_options": set(["hpcsim", "intercon", "torus", "host"]),
        # "debug_options": set(["hpcsim", "intercon", "torus"]),
        "debug_options": set(["torus"]),
        # obaida - what stat to print? and write?

        # mpi configurations (necessary if mpi is loaded)
        # "mpiopt" : configs.gemini_mpiopt,  # standard mpi config for Gemini
        # "mpiopt" : {
        #    "min_pktsz" : 2048,    # max packet size
        #    "max_pktsz" : 10000,   # min packet size
        #    "resend_intv" : 0.01,  # time between resends
        #    "resend_trials" : 10,  # num of trials until giving up
        #    "call_time" : 1e-7,    # async calls may still cost time
        #    "max_injection" : 1e9  # max mpi injection rate into network
        # },
        # gemini_mpiopt = {
        #    "min_pktsz" : 0,
        #    "max_pktsz" : 64,
        #    "put_data_overhead" : 32,
        #    "put_ack_overhead" : 9,
        #    "get_data_overhead" : 17,
        #    "get_ack_overhead" : 24,
        #    "max_injection" : 6e9,
        #    "resend_intv" : 0.1,
        # }
        "mpiopt": {
            "min_pktsz": 0,
            "max_pktsz": 2048,
            "put_data_overhead": 32,
            "put_ack_overhead": 9,
            "get_data_overhead": 17,
            "get_ack_overhead": 24,
            "max_injection": 6e9,
            "resend_intv": 0.1,
        }
    }

    ## initialize cluster and run simulation
    cluster = Cluster(model_dict)
    num_hosts = cluster.num_hosts()
    num_cores = num_hosts * cores_per_node
    num_processors = num_cores
    print (" * main.py::main() total hosts:%d,  total cores:%d"
           % (num_hosts, num_cores))

    # parse input_file file to list of apps
    input_file = open(input_file)
    # jobs = _job_inputs_to_jobs(parse_lines(input_file), num_processors)
    ##GENERATORS can be used only once, so we need to copy it into a list
    # https://stackoverflow.com/questions/231767/what-does-the-yield-keyword-do-in-python
    jobs = list(_job_inputs_to_jobs(parse_lines(input_file), num_processors))

    # expected app fields
    #  0: job_no       4: runtime      8. user_id      12. state       
    #  1: size         5. walltimef    9. project_id   13. hostmap
    #  2: queue        6. executable  10. starttime    14. partitions 
    #  3: submittime   7. args        11. walltime     15.


    # load executables of requested pattern(application)
    if pattern in ["first-last", "ring", "bipartite", "allreduce", "broadcast"]:
        for job in jobs:
            executable = {'app': simple_communications.__name__,
                          'module': sys.modules[simple_communications.__module__].__name__}
            job[6] = executable
            job[7] = pattern
            job[14] = partitions

    elif pattern == "one-way":
        for job in jobs:
            executable = {'app': app_one_way.__name__,
                          'module': sys.modules[app_one_way.__module__].__name__}
            job[6] = executable
            job[7] = [partitions, 1024]
            job[14] = partitions

    elif pattern == "dense-comm":
        execs = params["execs"].split("-")
        exec_dense = {'app': app_dense_comm.__name__,
                      'module': sys.modules[app_dense_comm.__module__].__name__}

        exec_empty = {'app': app_empty.__name__,
                      'module': sys.modules[app_empty.__module__].__name__}
        index = 0
        for job in jobs:
            if execs[index] == "1":
                job[6] = exec_dense
            elif execs[index] == "0":
                job[6] = exec_empty
            else:
                raise Exception("* main.py: undefined executable for:" + pattern)

            index += 1
            # job_id, data_size, partitions, virtual_partitions
            job[7] = [job[0], 1024000, partitions, partitions]
            job[14] = partitions

    elif pattern == "allreduce-sync":
        exec_allreduce_sync = {'app': app_allreduce_sync.__name__,
                               'module': sys.modules[app_allreduce_sync.__module__].__name__}
        for job in jobs:
            job[6] = exec_allreduce_sync
            # job_id, data_size, partitions, virtual_partitions
            job[7] = [job[0], 10240, partitions, partitions]
            job[14] = partitions


    elif pattern == "stencil":
        for job in jobs:
            job[6] = stencil
            size = job[1]

            dim = balanced_cube(size)  # approximate [x*y*z] for the size
            if len(dim) == 0:   dim = [size]  # no balanced cube found
            if len(dim) == 1:   dim.append(1)  # make app 2d
            if len(dim) == 2:   dim.append(1)  # make app 3d

            pattern = pattern.split("-")[-1]
            print " * main.py::main()  pattern:", pattern, "dim:", dim
            job[7] = [pattern, dim]  # args -- app[7] = ["3d", dim]
            job[14] = partitions  # num of partitions u want

    elif pattern == "snapsim":
        for job in jobs:
            job[6] = snap_process
            job[14] = partitions


    elif pattern == "background-foreground":
        if "timing" in params:
            with open(params["timing"], "r") as fp:
                for i, line in enumerate(fp):
                    foreground_times = [float(d) for d in line.split(",")]
                    line2 = next(fp)
                    foreground_starts = [float(d) for d in line.split(",")]
                    break
        backgrounds = ["allreduce"]  # , "bipartite"] #"stencil/3d"]   #patterns: ring, broadcast etc
        n = len(backgrounds)

        for job in jobs:
            rand = random.randint(0, n - 1)
            # print "   * background app rand is", rand
            job[6] = background
            job[7] = [backgrounds[rand], foreground_starts]
            job[14] = partitions

        foreground, size = foreground.split("/")
        size = int(size)

        if "stencil" in foreground:
            nn = foreground.split("-")[-1]
            dim = balanced_cube(size)
            if len(dim) == 0:   dim = [size]
            if len(dim) == 1:   dim.append(1)
            if len(dim) == 2:   dim.append(1)
            exe = stencil
            args = [nn, dim]
        elif "allreduce" in foreground:
            exe = simple_communications
            args = "allreduce"
        else:
            raise Exception(" * main.py: invalid foregroun app requested")

        # create and inject foreground to apps
        print " * before foregrounds: len(apps):", len(jobs)
        for i in xrange(len(foreground_times)):
            job = [0, size, 0, foreground_times[i], runtime, runtime, exe, args,
                   0, 0, 0, 0, None, None, partitions]
            jobs.append(job)
            print " * foreground app:", i, "=", job
        print " * after foregrounds: len(apps):", len(jobs)


    elif pattern == "foreground-only":
        if "timing" in params:
            with open(params["timing"], "r") as fp:
                for i, line in enumerate(fp):
                    print "* foreground-only ine:", line
                    foreground_times = [float(d) for d in line.strip().split(",")]
                    line2 = next(fp)
                    foreground_starts = [float(d) for d in line.split(",")]
                    break
        else:
            raise Exception(" * main.py: no timing file supplied")

        if len(foreground_times) != len(foreground_starts):
            raise Exception(" * main.py: length mismatch, foreground times and starts")

        print " * foreground-only:", foreground

        # configure empty backgrounds
        for job in jobs:
            job[6] = background_empty
            job[7] = "background"
            job[14] = partitions

        foreground, size = foreground.split("/")
        size = int(size)

        if "stencil" in foreground:
            nn = foreground.split("-")[-1]
            dim = balanced_cube(size)
            if len(dim) == 0:   dim = [size]
            if len(dim) == 1:   dim.append(1)
            if len(dim) == 2:   dim.append(1)
            exe = stencil
            args = [nn, dim]
        elif "allreduce" in foreground:
            exe = simple_communications
            args = "allreduce"
        else:
            raise Exception(" * main.py: invalid foregroun app requested")

        # create and inject foreground to apps
        print " * before foregrounds: len(apps):", len(jobs)
        for i in xrange(len(foreground_times)):
            job = [0, size, 0, foreground_times[i], runtime, runtime, exe, args,
                   0, 0, 0, 0, None, None, partitions]
            jobs.append(job)
            print " * foreground app:", i, "=", job
        print " * after foregrounds: len(apps):", len(jobs)


    elif pattern == "background-timer":
        first_submit = min([job[3] for job in jobs])
        last_submit = max([job[3] for job in jobs])
        foreground_times = sorted([random.randint(first_submit, last_submit) for i in xrange(total)])

        if first_submit == last_submit:
            foreground_times = [first_submit + 0.000000005]
            total = 1

        for job in jobs:
            job[6] = background_empty
            job[7] = "background"
            job[14] = partitions

        foreground, size = foreground.split("/")
        size = int(size)
        foregrounds = []

        print " * before foregrounds: len(apps):", len(jobs)

        for i in xrange(total):
            job = [0, size, 0, foreground_times[i], runtime, runtime, foreground_empty, "foreground",
                   0, 0, 0, 0, None, None, partitions]
            foregrounds.append(job)
            jobs.append(job)
        print "  * foreground times (save it):", foreground_times


    else:  # empty
        print " * main() using empty application"
        executable = {'app': app_empty.__name__,
                      'module': sys.modules[app_empty.__module__].__name__
                      }
        print " * app executable:", executable
        num_jobs = 0
        for job in jobs:
            job.app = executable  # no communication application
            job.args = None
            job.partitions = partitions
            num_jobs += 1

    print " * main.py -- total applications :", num_jobs, "\n"

    ##dictionaries
    sched_dict = {  # job scheduler configuration
        'scheduling': scheduling,  # more: sjf, score, queue-priority
        'mapping': mapping,
        'fileroot': fileroot,
        'cores_per_node': cores_per_node,
        # 'mapping'     : "random-node/router", "round-robin-group/router/node"
        #                   "double-ended-linear/xyz/zxy" "machine-learning:clustering:SVG",
        # 'divide_at'     : 512,        # default is 512
        # 'cartesian'     : [4,2,3],
        'cartesian': cartesian,
    }
    print " * main.py- sched_dict:", sched_dict

    """
    ##DISCARD jobs that doesnt match cores_per_node
    ##oversized jobs are resized automatically in pyss parsing
    
    #discard apps with size larger than cluster and asks integer num nodes
    #discarded = [job for job in jobs if job[1] > num_cores or job[1] % cores_per_node != 0]
    #jobs = [job for job in jobs if job[1] <= num_cores and job[1] % cores_per_node == 0]
    #print " * main.py::main() discarded:", len(discarded)
    if False:
          fp_discarded = open (fileroot+"_discarded.out", "w")
          fp_discarded.write("#fileroot:"+str(fileroot)+"\n")
          fp_discarded.write("#modeleing parameters:"+str(params)+"\n")
          fp_discarded.write("#system num_cores:"+str(num_cores)+"\n")
          for job in discarded:
              line = " \t".join([str(d) for d in job])
              fp_discarded.write(line+"\n")
          fp_discarded.close()
    print " * main.py::main() after discarding total apps:", len(jobs)
    del discarded
    """

    # submit all valid apps to scheduler
    cluster.attach_scheduler(sched_dict=sched_dict, jobs=jobs)

    # to guarantee deallocation of apps
    del jobs

    timestamp = int(time.time())
    cluster.run()

    finishtime = int(time.time())
    time_elapsed = finishtime - timestamp
    print ("  * main.py::main() time elapsed: %f (minutes)" % (time_elapsed / 60.0))
