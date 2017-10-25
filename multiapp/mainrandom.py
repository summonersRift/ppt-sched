#
# main.py :- multi app modeling on scheduler using simian-ppt
#


import gc, sys, time, random
sys.path.append('../')
sys.path.append('../apps/util')
sys.path.append('../apps/library')
from ppt import *

#parallel workload parser
from workload import workload_to_lists

#importing hpc applications implemented
from app_stencil       import stencil
from app_snapsim       import snap_process
from app_communication import app_empty, simple_communications, app_one_way, app_sync_start
from app_background    import background, background_empty, foreground_empty

#util
from size_to_topo import balanced_cube


def main(params):
  #DEFAULT configuration
  discipline     = "sjf"
  mapping        = "random-node"
  cartesian      = "1,1,1"
  partitions     = 0
  cores_per_node = 1

  #trace = "data/ANL-Intrepid-2009-1200"
  trace = "data/LLNL-Atlas-2006-2-April12007-May12007"
  #trace = "data/sdsc_sp2_last_10k"

  pattern    = "empty"
  foreground = "none/0"
  #foreground_duration = 1

  # If configuration supplied
  if "discipline" in params:     discipline = params['discipline']
  if "mapping" in params:        mapping = params['mapping']
  if "cartesian" in params:      cartesian = params['cartesian']
  if "partition" in params:      partitions = int(params['partition'])
  if "cores_per_node" in params: cores_per_node = params['cores_per_node']
  if "trace" in params:          trace = params['trace']
  if "pattern" in params:        pattern = params['pattern']
  if "foreground" in params:     foreground = params["foreground"]
  if "apps" in params:           apps = params['apps']
  #del params

  total   = 10     #num of forgrounds to inject
  runtime = 3600   #foreground walltimef

  fileroot  = discipline+"_"+mapping+"_"+pattern+"_"+trace.split("/")[-1]+"_"+cartesian+"_"
  fileroot += str(partitions)+"_"+foreground.split("/")[0]+"-"+foreground.split("/")[1]
  #'fileroot'       : discipline+"_"+mapping+"_"+pattern+"_"+trace.split("/")[-1],

  
  cartesian = [int(d) for d in cartesian.split(",")]

  sched_dict ={          # job scheduler configuration
      'discipline'     : discipline,  #more: sjf, score, queue-priority
      'algorithm'      : mapping,
      'fileroot'       : fileroot,
      'cores_per_node' : cores_per_node,
      #'algorithm'      : "random-node/router", "round-robin-group/router/node"
      #                   "double-ended-linear/xyz/zxy" "machine-learning:clustering:SVG",
      #'divide_at'      : 512,        # default is 512
      #'cartesian'      : [4,2,3],
      'cartesian'      : cartesian,
  }
  print "sched_dict", sched_dict
 
  #requested appplication valid?
  valid_patterns= ["empty", "first-last", "ring", "bipartite", "allreduce", "broadcast", 
                   "one-way", "sync-allreduce", "sync-dense-comm", "sync-even-odd",
                   "stencil", "snapsim", "phase-based", 
                   "background-foreground", "foreground-only", "background-timer"]
  if pattern not in valid_patterns:
      raise Exception (" * main.py::main() unsupported application:", pattern)
 
  #note timestamp
  timestamp = int(time.time())
  
  #parse trace file to list of apps
  apps = workload_to_lists (trace)
  
  #expected app fields
  #  0: job_no       4: runtime      8. user_id      12. state       
  #  1: size         5. walltimef    9. project_id   13. hostmap
  #  2: queue        6. executable  10. starttime    14. partitions 
  #  3: submittime   7. args        11. walltime     15.

  #load executables of requested pattern(application)
  if pattern in ["first-last", "ring", "bipartite", "allreduce", "broadcast"]:
     for app in apps:
        executable = {'app'    : simple_communications.__name__,
                      'module' : sys.modules[simple_communications.__module__].__name__ }
        app[6]  = executable
        app[7]  = pattern
        app[14] = partitions

  elif pattern in ["sync-allreduce", "sync-dense-comm", "sync-even-odd"]:
    exec_sync  = {'app'    : app_sync_start.__name__,
                  'module' : sys.modules[app_sync_start.__module__].__name__ }
    exec_empty = {'app'    : app_empty.__name__,
                  'module' : sys.modules[app_empty.__module__].__name__ }
    for app in apps:
        app[6]  = exec_sync
        #job_id, pattern, data_size, partitions, virtual_partitions
        app[7]  = [app[0], pattern, 1024000, 60.0, partitions,partitions]
        app[14] = partitions
    if params['execs'] is not None:
        execs = params["execs"].split("-")
        index = 0
        for app in apps:
            if execs[index] == "0": 
                app[6] = exec_empty
                app[7] = None
            elif execs[index] != "1": 
                raise Exception ("* main.py: undefined exe for:"+pattern)
            index += 1

  elif pattern == "background-foreground":
        if "timing" in  params:
            with open(params["timing"], "r") as fp:
                for i, line in enumerate(fp):
                    foreground_times = [float(d) for d in line.split(",")]
                    line2 = next(fp)
                    foreground_starts  = [float(d) for d in line.split(",")]
                    break
        ## randomized backgrouna application pool
        #backgrounds = ["allreduce"]#, "bipartite"] #"stencil/3d"]   #patterns: ring, broadcast etc
        #n = len(backgrounds)
        #rand = random.randint(0, n-1)

        exe_background = {'app'    : background.__name__,
                          'module' : sys.modules[background.__module__].__name__ }
        max_job_no = 0
        for app in apps:
            app[6]  = exe_background
            if app[1] >= 2048:
                data_size = 1024000
            elif app[1] >= 1024:
                data_size = 2048000
            elif app[1] >= 512:
                data_size = 4096000
            elif app[1] < 512:
                data_size = 8096000
            else:
                raise Exception ("something went wrong with data_size")
            app[7] = [app[0], "even-odd", data_size, foreground_starts]
            app[14] = partitions
            if app[0] >max_job_no: max_job_no = app[0]

        foreground, size = foreground.split("/")
        size = int(size)

        if "even-odd" in foreground:
            exe = {'app'    : app_sync_start.__name__,
                   'module' : sys.modules[app_sync_start.__module__].__name__ }
        elif "stencil" in foreground:
            nn = foreground.split("-")[-1]
            dim = balanced_cube( size )
            if len(dim) == 0:   dim = [size]  
            if len(dim) == 1:   dim.append(1)
            if len(dim) == 2:   dim.append(1)
            exe = {'app'    : stencil.__name__,
                   'module' : sys.modules[stencil.__module__].__name__ }
            args  = [nn, dim]
        elif "allreduce" in foreground:
            exe = {'app'    : simple_communications.__name__,
                   'module' : sys.modules[simple_communications.__module__].__name__ }
            args  = "allreduce"
        else:
            raise Exception (" * main.py: invalid foregroun app requested")
        
        #create and inject foreground to apps
        print " * before foregrounds: len(apps):", len(apps)
        for i in xrange(len(foreground_times)):
            max_job_no += 1
            if "even-odd" in foreground:
                args = [max_job_no, foreground, 0.0, 1024000, partitions, partitions]
            app = [max_job_no, size, 0, foreground_times[i], runtime, runtime, exe, args, 
                    0, 0, 0, 0, None, None, partitions]
            apps.append(app)
            print " * foreground app:",i,"=", app
        print " * after foregrounds: len(apps):", len(apps)


  elif pattern == "foreground-only":
        if "timing" in  params:
            with open(params["timing"], "r") as fp:
                for i, line in enumerate(fp):
                    print "* foreground-only line:", line
                    foreground_times = [float(d) for d in line.strip().split(",")]
                    line2 = next(fp)
                    foreground_starts  = [float(d) for d in line.split(",")]
                    break
        else:
            raise Exception (" * main.py: no timing file supplied")
        if len(foreground_times) != len(foreground_starts):
            raise Exception (" * main.py: length mismatch, foreground times and starts")
        print " * foreground-only:", foreground

        exe_empty = {'app'    : app_empty.__name__,
                     'module' : sys.modules[app_empty.__module__].__name__ }
        max_job_no = 0
        for app in apps:
            app[6]  = exe_empty
            app[7]  = "background"
            app[14] = partitions
            if app[0] >max_job_no: max_job_no = app[0]

        foreground, size = foreground.split("/")
        size = int(size)

        if "even-odd" in foreground:
            exe = {'app'    : app_sync_start.__name__,
                   'module' : sys.modules[app_sync_start.__module__].__name__ }
        elif "stencil" in foreground:
            nn = foreground.split("-")[-1]
            dim = balanced_cube( size )
            if len(dim) == 0:   dim = [size]  
            if len(dim) == 1:   dim.append(1)
            if len(dim) == 2:   dim.append(1)
            exe = {'app'    : stencil.__name__,
                   'module' : sys.modules[stencil.__module__].__name__ }
            args  = [nn, dim]
        elif "allreduce" in foreground:
            exe = {'app'    : simple_communications.__name__,
                   'module' : sys.modules[simple_communications.__module__].__name__ }
            args  = "allreduce"
        else:
            raise Exception (" * main.py: invalid foreground:"+foreground)
        
        #create and inject foreground to apps
        print " * before foregrounds: len(apps):", len(apps)
        for i in xrange(len(foreground_times)):
            max_job_no += 1
            if "even-odd" in foreground:
                args = [max_job_no, foreground, 0.0, 1024000, partitions, partitions]
            app = [max_job_no, size, 0, foreground_times[i], runtime, runtime, exe, args, 
                    0, 0, 0, 0, None, None, partitions]
            apps.append(app)
            print " * foreground app:",i,"=", app
        print " * after foregrounds: len(apps):", len(apps)

  elif pattern == "background-timer":
        first_submit = min([app[3] for app in apps])
        last_submit = max([app[3] for app in apps])
        foreground_times = sorted([random.randint(first_submit, last_submit) for i in xrange(total) ])

        exe_empty = {'app'    : app_empty.__name__,
                     'module' : sys.modules[app_empty.__module__].__name__ }

        exe_forg_empty = {'app'    : foreground_empty.__name__,
                          'module' : sys.modules[foreground_empty.__module__].__name__ }
        if first_submit == last_submit:
            foreground_times = [first_submit+0.000000005]
            total = 1

        max_job_no = 0
        for app in apps:
            app[6]  = exe_empty
            app[7]  = "background"
            app[14] = partitions
            if app[0] >max_job_no: max_job_no = app[0]
        foreground, size = foreground.split("/")
        if size < 1:
            raise Exception (" * main.py:"+pattern+" size<0:"+str(size))
        size = int(size)
        foregrounds = []
        print " * fground application size:", size
        print " * before fgrounds: len(apps):", len(apps)
        for i in xrange(total):
            max_job_no  += 1
            app = [max_job_no, size, 0, foreground_times[i], runtime, runtime, exe_forg_empty, "foreground", 
                    0, 0, 0, 0, None, None, partitions]
            foregrounds.append(app)
            apps.append(app)
        print "  * foreground times (save it):", foreground_times

  elif pattern == "one-way":
     for app in apps:
        executable = {'app'    : app_one_way.__name__,
                      'module' : sys.modules[app_one_way.__module__].__name__ }
        app[6]  = executable
        app[7]  = [partitions, 1024] 
        app[14] = partitions

  elif pattern == "stencil":
    executable = {'app'    : stencil.__name__,
                  'module' : sys.modules[stencil.__module__].__name__ }
    for app in apps:
        app[6] = stencil
        size   = app[1]
 
        dim = balanced_cube(size)             # approximate [x*y*z] for the size
        if len(dim) == 0:   dim = [size]      # no balanced cube found
        if len(dim) == 1:   dim.append(1)     # make app 2d
        if len(dim) == 2:   dim.append(1)     # make app 3d
 
        pattern = pattern.split("-")[-1]
        print " * main.py::main()  pattern:", pattern, "dim:",dim
        app[7]  = [pattern, dim]              # args -- app[7] = ["3d", dim]
        app[14] = partitions             #num of partitions u want

  elif pattern == "snapsim":
    for app in apps:
        app[6]  = snap_process
        app[14] = partitions


  else:                                         #empty 
    print " * main() using empty application"
    executable = {'app'    : app_empty.__name__,
                  'module' : sys.modules[app_empty.__module__].__name__
                 }
    print " * app executable:", executable
    for app in apps:
        app[6]   = executable                   #no communication application
        #app[14] = partitions
  
  print " * main.py -- total applications :", len(apps), "\n"
  

  model_dict = {
      # simian parameters (these are required unless explicitly
      # specified when instantiating Cluster)
      "model_name" : "multiapp_scheduling_random", # name of the model
      "sim_time" : 1e9, # simulation time
      #"use_mpi" : False, # whether using mpi for parallel
      "use_mpi" : True, # whether using mpi for parallel
      "mpi_path" : "/usr/lib/x86_64-linux-gnu/libmpich.so",  #ubuntu-14@vega
      
      "intercon_type" : "Gemini",         # IMPORTANT: type is case sensitive
      #standardized interconnect models
      #"torus" : configs.hopper_intercon,
      #"torus" : configs.cielo_intercon,
      #"torus" : configs.sdsc_sp2_intercon,
      #"torus" : configs.titan_intercon,
      "torus" : configs.cielo_intercon_deterministic,
      
      # compute node parameters; IMPORTANT: type is case sensitive
      "host_type" : "CieloNode",  # cielo with or without mpi installed
      #"host_type" : "TTNNode",  # cielo with or without mpi installed
      #"host_type" : "Host",       # generic compute node with or without mpi installed
      
      # each host type can have a distinct configuration like
      # interconnect (not yet implemented)
      
      # optional libraries/modules to be loaded onto compute nodes
      #"load_libraries": set([]),
      "load_libraries": set(["mpi"]),     # IMPORANT: lib names are case sensitie
      #"debug_options": set(["hpcsim", "intercon", "torus", "mpi-recv", "host"]), 
      #"debug_options": set(["intercon", "mapping"]),  #obaida expt
      #"debug_options": set(["hpcsim", "intercon", "torus", "host"]), 
      #"debug_options": set(["hpcsim", "intercon", "torus"]), 
      "debug_options": set(["torus"]), 
      # obaida - what stat to print? and write?
      
      # mpi configurations (necessary if mpi is loaded)
      #"mpiopt" : configs.gemini_mpiopt,  # standard mpi config for Gemini
      #"mpiopt" : {
      #    "min_pktsz" : 2048,    # max packet size
      #    "max_pktsz" : 10000,   # min packet size
      #    "resend_intv" : 0.01,  # time between resends
      #    "resend_trials" : 10,  # num of trials until giving up
      #    "call_time" : 1e-7,    # async calls may still cost time
      #    "max_injection" : 1e9  # max mpi injection rate into network
      #},
      #gemini_mpiopt = { 
      #    "min_pktsz" : 0,
      #    "max_pktsz" : 64,
      #    "put_data_overhead" : 32,
      #    "put_ack_overhead" : 9,
      #    "get_data_overhead" : 17,
      #    "get_ack_overhead" : 24,
      #    "max_injection" : 6e9,
      #    "resend_intv" : 0.1,
      #}
      "mpiopt" : {
          "min_pktsz" : 0,
          "max_pktsz" : 2048,
          "put_data_overhead" : 32,
          "put_ack_overhead" : 9,
          "get_data_overhead" : 17,
          "get_ack_overhead" : 24,
          "max_injection" : 6e9,
          "resend_intv" : 0.1,
      }

  }
  
 
  # initialize cluster and run simulation
  cluster = Cluster(model_dict)
  num_hosts = cluster.num_hosts()
  num_cores = num_hosts*cores_per_node
  print (" * main.py::main() total hosts:%d,  total cores:%d" 
                                    %(num_hosts, num_cores)  )
  #discard apps with size larger than cluster and asks integer num nodes
  discarded = [ app for app in apps if app[1] > num_cores or app[1]%cores_per_node!=0 ]
  apps = [ app for app in apps if app[1] <= num_cores and app[1]%cores_per_node==0 ]

  print " * main.py::main() discarded:", len(discarded)
  if False:
        fp_discarded = open (fileroot+"_discarded.out", "w")
        fp_discarded.write("#fileroot:"+str(fileroot)+"\n")
        fp_discarded.write("#modeleing parameters:"+str(params)+"\n")
        fp_discarded.write("#system num_cores:"+str(num_cores)+"\n")
        for app in discarded:
            line = " \t".join([str(d) for d in app])
            fp_discarded.write(line+"\n")
        fp_discarded.close()
  print " * main.py::main() after discarding total apps:", len(apps)
 
  #submit all valid apps to scheduler
  cluster.attach_scheduler(sched_dict, apps)
  
  # to guarantee deallocation of apps
  del discarded
  del apps 
  
  cluster.run()
  
  finishtime = int(time.time())

  time_elapsed = finishtime - timestamp
  print ("  * main.py::main() time elapsed: %f (minutes)" % (time_elapsed/60.0) )
  return time_elapsed
  
 
if __name__ == "__main__":

    params = {}
    
    if len(sys.argv[1:]) ==0:
        print " * main.py:: no paramenters supplied."
    elif len(sys.argv[1:]) > 5:
        #if len(sys.argv)<6: raise Exception "Not enough argv:", sys.argv
        params['discipline']      = sys.argv[1]
        params['mapping']         = sys.argv[2]
        params['trace']           = sys.argv[3]
        params['pattern']         = sys.argv[4]
        params['cartesian']       = sys.argv[5]
        params['partition']       = sys.argv[6]
        params['cores_per_node']  = 1

        if (params['pattern']) in ["sync-dense-comm", "sync-even-odd"] :
            try:
                params['execs']  = sys.argv[7]
            except IndexError:
                params['execs']  = None
        if params['pattern'] in ["background-timer", "foreground-only", "background-foreground"]:
            params['foreground']  = sys.argv[7]
            try:
                params['timing']  = sys.argv[8]
            except IndexError:
                params['timing']  = None
    else:
        raise Exception ("main.py -- invalid list of arguments:"+str(sys.argv) )

    #run the program
    print " * main.py -- parameters:", params
    time_elapsed = main(params)
#
