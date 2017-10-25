#
# cluster.py :- an HPC cluster consists of a number of hosts (compute
#               nodes) connected by an interconnection network
#

from simian import Simian
from interconnect import *
from nodes import *

#obaida imports
import sys



class Cluster(object):
    """Represents an entire HPC cluster."""

    # local variables:
    #   hpcsim_dict: model parameters as a dictionary
    #   simian: the simian simulation engine (also hpcsim_dict['simian'])
    #   intercon: the interconnection network (also hpcsim_dict['intercon'])

    def __init__(self, hpcsim_dict=None, **kargs):
        """Instantiates the entire HPC model.

        With parameters passed altogether in a dictionary
        (hpcsim_dict), and as explicit keyword arguments (which would
        be merged into the dictionary but would take higher precedence
        if both exist).
        """

        # the reason we don't pass in reference to simian engine as
        # agument (rather we init simian inside this function) is that
        # simian requires min_delay, which can only be obtained when
        # the cluster is instantiated:
        # <old interface->
        # def __init__(self, simian, hpcsim_dict=None, **kargs)

        # 0. consolidate keyword arguments into hpcsim_dict
        if hpcsim_dict is None:
            hpcsim_dict = kargs;
        else:
            try:
                # this may overwrite what was in hpcsim_dict
                hpcsim_dict.update(kargs)
            except AttributeError:
                raise Exception("keyword argument 'hpcsim_dict' must be a dictionary")
        self.hpcsim_dict = hpcsim_dict

        # 0.5 debug options is used for selectively printing useful
        # debug information in various modules we have; now if the
        # user didn't set it, we set it here wishfully. You can change
        # it according to your debugging needs.
        if "debug_options" not in hpcsim_dict:
            hpcsim_dict["debug_options"] = set(["hpcsim", "torus", "dragonfly", "fattree"])

        # 1. settle system-wide default values (we set them all here
        #    if they are not already set by user); system default
        #    values are the ones which would be used where the model
        #    parameters are not explicitly specified by user.
        if "default_configs" not in hpcsim_dict:
            hpcsim_dict["default_configs"] = dict()

        # default network bandwidth is 1G bits-per-second
        if "intercon_bandwidth" not in hpcsim_dict["default_configs"]:
            hpcsim_dict["default_configs"]["intercon_bandwidth"] = 1e9

        # default network interface buffer size is 100M bytes
        if "intercon_bufsz" not in hpcsim_dict["default_configs"]:
            hpcsim_dict["default_configs"]["intercon_bufsz"] = 1e8

        # default network link delay is 1e-6 seconds, i.e., 1 microsecond
        if "intercon_link_delay" not in hpcsim_dict["default_configs"]:
            hpcsim_dict["default_configs"]["intercon_link_delay"] = 1e-6

        # default nodal processing delay is 0 seconds
        if "intercon_proc_delay" not in hpcsim_dict["default_configs"]:
            hpcsim_dict["default_configs"]["intercon_proc_delay"] = 0

        # default memory bandwidth for message passing via memory is 500 Gbps
        if "mem_bandwidth" not in hpcsim_dict["default_configs"]:
            hpcsim_dict["default_configs"]["mem_bandwidth"] = 5e11

        # default buffer size for message passing via memory is 16 GB
        if "mem_bufsz" not in hpcsim_dict["default_configs"]:
            hpcsim_dict["default_configs"]["mem_bufsz"] = 1.6e10

        # default latency for message passing via memory is 100 ns
        if "mem_delay" not in hpcsim_dict["default_configs"]:
            hpcsim_dict["default_configs"]["mem_delay"] = 1e-7

        # default routing for torus is "adaptive dimension order" routing
        if "torus_route_method" not in hpcsim_dict["default_configs"]:
            hpcsim_dict["default_configs"]["torus_route_method"] = "adaptive_dimension_order"
            
        # default routing for dragonfly is "minimal"
        if "dragonfly_route_method" not in hpcsim_dict["default_configs"]:
            hpcsim_dict["default_configs"]["dragonfly_route_method"] = "minimal"
        
        # default routing for fat-tree is "multiple LID nearest common ancestor"
        if "fattree_route_method" not in hpcsim_dict["default_configs"]:
            hpcsim_dict["default_configs"]["fattree_route_method"] = "multiple_lid_nca"
        
        # default mpi resend interval is 1e-3 seconds, i.e., 1 millisecond
        if "mpi_resend_intv" not in hpcsim_dict["default_configs"]:
            hpcsim_dict["default_configs"]["mpi_resend_intv"] = 1e-3

        # default max number of mpi resend is 10
        if "mpi_resend_trials" not in hpcsim_dict["default_configs"]:
            hpcsim_dict["default_configs"]["mpi_resend_trials"] = 10
            
        # default minimum mpi message size is 0 bytes (no padding)
        if "mpi_min_pktsz" not in hpcsim_dict["default_configs"]:
            hpcsim_dict["default_configs"]["mpi_min_pktsz"] = 0
            
        # default maximum mpi message size is 4K bytes
        if "mpi_max_pktsz" not in hpcsim_dict["default_configs"]:
            hpcsim_dict["default_configs"]["mpi_max_pktsz"] = 4096
            
        # default time for mpi calls (for asynchronous methods) is 0 seconds
        if "mpi_call_time" not in hpcsim_dict["default_configs"]:
            hpcsim_dict["default_configs"]["mpi_call_time"] = 0
            
        # default header overhead for mpi data (put or get) is 0 bytes
        if "mpi_data_overhead" not in hpcsim_dict["default_configs"]:
            hpcsim_dict["default_configs"]["mpi_data_overhead"] = 0
            
        # default header overhead for mpi ack message (put or get) is 0 bytes
        if "mpi_ack_overhead" not in hpcsim_dict["default_configs"]:
            hpcsim_dict["default_configs"]["mpi_ack_overhead"] = 0

        # default threshold for PUT/GET (i.e., upper size limit for PUT) is 4K bytes 
        if "mpi_putget_thresh" not in hpcsim_dict["default_configs"]:
            hpcsim_dict["default_configs"]["mpi_putget_thresh"] = 4096
       
        # default maximum injection rate is 1G bytes-per-second
        if "mpi_max_injection" not in hpcsim_dict["default_configs"]:
            hpcsim_dict["default_configs"]["mpi_max_injection"] = 1e9

        # 1.5 print out all model parameters if requested so
        if "hpcsim" in hpcsim_dict["debug_options"]:
            for k, v in sorted(hpcsim_dict.iteritems()):
                if type(v) is dict:
                    print("hpcsim_dict['%s']:"%k)
                    for kk,vv in sorted(v.iteritems()):
                        if k=='torus' and kk=='hostmap':
                            # exception: we don't want to print
                            # torus.hostmap (it's too long):
                            print("  hostmap: (provided)")
                        else:
                            print("  %s = %r" % (kk, vv))                        
                else:
                    # for all else, simply print them
                    print("hpcsim_dict['%s'] = %r" % (k, v))

        # 2. instantiate simian engine (with proper parameters)

        # check necessary simian parameters are in place
        if "model_name" not in hpcsim_dict:
            raise Exception("model_name must be specified")
        if "sim_time" not in hpcsim_dict:
            raise Exception("sim_time must be specified.")
        if "use_mpi" not in hpcsim_dict:
            raise Exception("use_mpi must be specified.")
        #
        # ADD THIS IF USING SimianPie:
        #
        elif hpcsim_dict["use_mpi"] and "mpi_path" not in hpcsim_dict:
            raise Exception("mpi_path must be specified if use_mpi is true.")
            
        # calculate min_delay from interconnect model (before it has
        # been instantiated)
        intercontype = self.get_intercon_typename(hpcsim_dict)
        hpcsim_dict["min_delay"] = intercontype.calc_min_delay(hpcsim_dict)

        if "hpcsim" in hpcsim_dict["debug_options"]:
            print("hpcsim_dict['min_delay'] = %.9f (calculated)" % hpcsim_dict["min_delay"]) 

        # instantiate (start) the simian engine
        #
        # USE THIS IF USING SimianPie.MPI4Py
        #
        #self.simian = Simian(hpcsim_dict["model_name"], 0, hpcsim_dict["sim_time"], 
        #                     hpcsim_dict["min_delay"], hpcsim_dict["use_mpi"])
        #
        # USE THIS INSTEAD IF USING SimianPie
        #
        if hpcsim_dict["use_mpi"]:
            self.simian = Simian(hpcsim_dict["model_name"], 0, hpcsim_dict["sim_time"], 
                                 hpcsim_dict["min_delay"], hpcsim_dict["use_mpi"],
                                 hpcsim_dict["mpi_path"])
        else: 
            self.simian = Simian(hpcsim_dict["model_name"], 0, hpcsim_dict["sim_time"], 
                                 hpcsim_dict["min_delay"])
        
        hpcsim_dict["simian"] = self.simian

        # 3. instantiate the interconnect according to model
        # configuration (important: this in turn will instantiate all
        # the hosts, a.k.a. the compute nodes)
        #intercontype = self.get_intercon_typename(hpcsim_dict)
        #print "obaida: intercontype", intercontype
        self.intercon = intercontype(self, hpcsim_dict)
        hpcsim_dict["intercon"] = self.intercon


    def num_hosts(self):
        """Returns the total number of hosts (compute nodes)."""
        return self.intercon.num_hosts()

    def sched_raw_xfer(self, t, src, dst, sz, blaze_trail=False):
        """Schedules data transfer at given time and of given size.

        The data transfer will start at time t from host src to host dst of
        size sz; if blaze_trail is true, the packet will carry the list
        of all hops it traverses (including src and dst hosts); this
        function is mostly for testing purposes without engaging with
        the mpi calls.
        """
         
        # we assume that the hosts are derived from the Host class,
        # which has test_raw_xfer already defined
        nodetype = self.get_host_typename(self.hpcsim_dict)
        if not getattr(nodetype, "test_raw_xfer", None):
            raise Exception("host %s does not support sched_raw_xfer" % hpcsim_dict["host_type"])
        if 0 <= src < self.intercon.num_hosts() and \
           0 <= dst < self.intercon.num_hosts():
            dict = { 'dest':dst, 'sz':sz, 'blaze':blaze_trail }
            self.simian.schedService(t, "test_raw_xfer", dict, "Host", src)
        else:
            raise Exception("host src=%d or dst=%d out of range: total #hosts=%d)" % 
                            (src, dst, self.intercon.num_hosts()))

    def start_mpi(self, hostmap, main_process, *args):
        """Starts mpi from a given routine with arguments."""

        # this function assumes that the hosts are mpi enabled,
        # meaning that host_type must be mpihost or any derived class
        # of mpihost, where create_mpi_proc has to be defined
        nodetype = self.get_host_typename(self.hpcsim_dict)
        if not getattr(nodetype, "create_mpi_proc", None):
            raise Exception("host %s does not support mpi" % self.hpcsim_dict["host_type"])

        # configure mpi with options
        mpiopt = self.hpcsim_dict.get("mpiopt", {})
        if "resend_intv" not in mpiopt:
            mpiopt["resend_intv"] = self.hpcsim_dict["default_configs"]["mpi_resend_intv"]
        if "resend_trials" not in mpiopt:
            mpiopt["resend_trials"] = self.hpcsim_dict["default_configs"]["mpi_resend_trials"]
        if "min_pktsz" not in mpiopt:
            mpiopt["min_pktsz"] = self.hpcsim_dict["default_configs"]["mpi_min_pktsz"]
        if "max_pktsz" not in mpiopt:
            mpiopt["max_pktsz"] = self.hpcsim_dict["default_configs"]["mpi_max_pktsz"]
        if "call_time" not in mpiopt:
            mpiopt["call_time"] = self.hpcsim_dict["default_configs"]["mpi_call_time"]
        if "putget_thresh" not in mpiopt:
            mpiopt["putget_thresh"] = self.hpcsim_dict["default_configs"]["mpi_putget_thresh"]
        if "max_injection" not in mpiopt:
            mpiopt["max_injection"] = self.hpcsim_dict["default_configs"]["mpi_max_injection"]

        # for backward compatibility, if data/ack_overhead is defined
        # we treated them for both PUT and GET, as long as specific
        # PUT/GET overhead is not defined
        if "put_data_overhead" not in mpiopt:
            if "data_overhead" in mpiopt:
                mpiopt["put_data_overhead"] = mpiopt["data_overhead"]
            else:
                mpiopt["put_data_overhead"] = self.hpcsim_dict["default_configs"]["mpi_data_overhead"]
        if "get_data_overhead" not in mpiopt:
            if "data_overhead" in mpiopt:
                mpiopt["get_data_overhead"] = mpiopt["data_overhead"]
            else:
                mpiopt["get_data_overhead"] = self.hpcsim_dict["default_configs"]["mpi_data_overhead"]
        if "put_ack_overhead" not in mpiopt:
            if "ack_overhead" in mpiopt:
                mpiopt["put_ack_overhead"] = mpiopt["ack_overhead"]
            else:
                mpiopt["put_ack_overhead"] = self.hpcsim_dict["default_configs"]["mpi_ack_overhead"]
        if "get_ack_overhead" not in mpiopt:
            if "ack_overhead" in mpiopt:
                mpiopt["get_ack_overhead"] = mpiopt["ack_overhead"]
            else:
                mpiopt["get_ack_overhead"] = self.hpcsim_dict["default_configs"]["mpi_ack_overhead"]

        # output mpiopt if hpcsim/mpi debug flags are set
        if "hpcsim" in self.hpcsim_dict["debug_options"] or \
           "mpi" in self.hpcsim_dict["debug_options"]:
            print("mpiopt: min_pktsz=%d" % mpiopt["min_pktsz"])
            print("mpiopt: max_pktsz=%d" % mpiopt["max_pktsz"])
            print("mpiopt: put_data_overhead=%d" % mpiopt["put_data_overhead"])
            print("mpiopt: put_ack_overhead=%d" % mpiopt["put_ack_overhead"])
            print("mpiopt: get_data_overhead=%d" % mpiopt["get_data_overhead"])
            print("mpiopt: get_ack_overhead=%d" % mpiopt["get_ack_overhead"])
            print("mpiopt: putget_thresh=%d" % mpiopt["putget_thresh"])
            print("mpiopt: resend_intv=%f" % mpiopt["resend_intv"])
            print("mpiopt: resend_trials=%d" % mpiopt["resend_trials"])
            print("mpiopt: call_time=%f" % mpiopt["call_time"])
            print("mpiopt: max_injection=%f" % mpiopt["max_injection"])

        # schedule service on the list of hosts
        for idx in range(len(hostmap)):
            if 0 <= hostmap[idx] < self.intercon.num_hosts():
                data = { 
                    "main_proc" : main_process,
                    "rank" : idx,
                    "hostmap" : hostmap,
                    "args" : args,
                    "mpiopt" : mpiopt
                }
                # simian ensures that the create_mpi_proc service is
                # scheduled only on local entities
                self.simian.schedService(self.simian.now, "create_mpi_proc", data, 
                                         "Host", hostmap[idx])
            else:
                raise Exception("mpi rank %d mapped to host %d out of range: total #hosts=%d)" % 
                                (idx, hostmap[idx], self.intercon.num_hosts()))


    def attach_scheduler(self, sched_dict, jobs, *args):                   #obaida
        """Initiates a node to handle application submissions and scheduling."""

        str_me = "* Cluster::attach_scheduler() :-"
        # add a scheduler node, create entity -- from nodes.JobSchedulerNode
        uid = 0
        self.simian.addEntity("JobScheduler", JobSchedulerNode, uid, 
                self.hpcsim_dict, sched_dict)
        self.start_mpi([], None)  #hostmap, 

        last_submit_time = 0
        num_jobs = 0
        for job in (jobs):
            submit_time = job.submit_time
            num_jobs += 1
            self.simian.schedService(submit_time, "job_submission_evt", job, 
                    "JobScheduler", uid)
            if submit_time > last_submit_time:
                last_submit_time = submit_time
            
        #schedule evt to collect statistics, an hour after last job arrives
        self.simian.schedService(last_submit_time+3600.0, "statistics_output_evt", 
                num_jobs, "JobScheduler", uid)
        print ("\n%s created arrival events for %d jobs" % (str_me, num_jobs) )
        
    def run(self):
        """Runs simulation to completion."""

        self.simian.run()
        self.simian.exit()

    @staticmethod
    def get_intercon_typename(hpcsim_dict):
        """Returns the type name of the interconnect class string."""
        
        if "intercon_type" not in hpcsim_dict:
            raise Exception("intercon_type must be specified")
        try:
            return eval(hpcsim_dict["intercon_type"])
        except SyntaxError:
            raise Exception("interconnect type %s not implemented" %
                            hpcsim_dict["intercon_type"])

    @staticmethod
    def get_host_typename(hpcsim_dict):
        """Returns the type name of the host class string."""
        
        if "host_type" not in hpcsim_dict:
            raise Exception("host_type must be specified")

        # check if mpi needs to be loaded and if so fix the class
        # hierarchy so that the host (compute node) is derived from
        # the MPIHost class, rather than Host
        if "load_libraries" not in hpcsim_dict:
            hpcsim_dict["load_libraries"] = dict()
        try:
            if "mpi" not in hpcsim_dict["load_libraries"]:
                if "MPIHost" == hpcsim_dict["host_type"]:
                    hpcsim_dict["host_type"] = "Host"
                return eval(hpcsim_dict["host_type"])
            else:
                if "Host" == hpcsim_dict["host_type"]:
                    hpcsim_dict["host_type"] = "MPIHost"
                if "MPIHost" == hpcsim_dict["host_type"]:
                    return eval(hpcsim_dict["host_type"])
                else:
                    name = "%s%s"%("MPI",hpcsim_dict["host_type"])
                    return type(name, (eval(hpcsim_dict["host_type"]),MPIHost), dict())
        except SyntaxError:
            raise Exception("host type %s not implemented" %
                            hpcsim_dict["intercon_type"])
