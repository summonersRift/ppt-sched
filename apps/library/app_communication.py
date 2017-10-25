#
# app_communication.py :- simple mpi application with communication patterns
#
# IMPORTANT: To make it to work, PATHONAPTH must be set to include
# PPT's 'code' directory, where the 'ppt.py' file is located

from sys import path
path.append('../..')
from ppt import *

#%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
def app_empty(mpi_comm_world, walltimef, pattern = None):
    """ mpi application with no communciation """

    n = mpi_comm_size(mpi_comm_world)                   # total # ranks
    p = global_core_id = mpi_comm_rank(mpi_comm_world)  # rank of this process
    #if p == 0:
    #    print ("     * [p:%d/%d] time:%f"% ( p, n, mpi_wtime(mpi_comm_world))  )

    #if (p==0):
    if walltimef <= 0:
            raise Exception (str_me+" ERROR! computetime:"+str(computetime)  )
    #sleep actual runtime of the application
    #tnow = mpi_wtime(mpi_comm_world)
    mpi_ext_sleep(walltimef, mpi_comm_world)
    #mpi_terminate(mpi_comm_world)

    mpi_finalize(mpi_comm_world)

#%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
def app_sync_start (mpi_comm_world, walltimef, *args):
    """send/receive among even/odd partitions"""

    args = args[0]   # arguments is received as a tuple (['3d', [0, 128, 128]],
    tStart = mpi_wtime(mpi_comm_world)
    try:
        job_no, pattern, data_size, delay, partitions, virtual_partitions = args
    except IndexError:
        job_no, pattern, data_size, delay, partitions, virtual_partitions = 0, "even-odd", 0.0, 1024000, 2, 2
 
    str_me = " * app_sync_start/"+pattern+"-"   
    n = mpi_comm_size(mpi_comm_world)
    p = mpi_comm_rank(mpi_comm_world)
    if p == 0:
        print " * in app_sync_start() job_no:%d, patern:%s, data_size::%f delay:%f"  %(job_no, pattern, data_size, delay)
        #raise Exception(" * apps/library/app_communications.py: checking app launch")


    if delay >0.0:
        step = 60                          #for congestion jobs starts at 1 minute
        computetime = step - tStart%60     #assuming,  tStart < step
        mpi_ext_sleep(computetime, mpi_comm_world)
        tBefore = mpi_wtime(mpi_comm_world)
        if (tBefore%step != 0.0): raise Exception (str_me+": time sync skewed.")
    else:
        tBefore = mpi_wtime(mpi_comm_world)

    if pattern == "sync-dense-comm":
        partitions = abs(partitions)
        ranks_per_part = int(n/partitions)
        my_part_id = int(p/ranks_per_part)
        if my_part_id%1 == 0:            #node lies in even partition id
            dst = (p+ranks_per_part)%n    #dst is node in next partition
            succ = mpi_send(dst, tBefore, data_size, mpi_comm_world)
            if not succ: raise Exception(str_me+" send failed at p"+str(p) );
        else:        #odd partition
            succ = mpi_recv(mpi_comm_world)
            if not succ: raise Exception(str_me+" recv failed at p"+str(p) );

    elif pattern == "sync-allreduce":
        r = mpi_allreduce(p, mpi_comm_world, data_size)
        if r is None:
            raise Exception("error occurred in running allreduce")

    elif pattern in ["even-odd"]:
        if n%2 != 0:
            raise Exception (str_me+" job size must be even. Received:"+str(n))
        if p%2 == 0:
            dst = (p+1)%n
            succ = mpi_send(dst, tBefore, data_size, mpi_comm_world)
            if not succ: raise Exception(str_me+" send failed at p"+str(p) );
        else:
            succ = mpi_recv(mpi_comm_world)
            if not succ: raise Exception(str_me+" recv failed at p:"+str(p) );
        mpi_barrier(mpi_comm_world)

    elif pattern == "sync-even-odd":
        if n%2 != 0:
            raise Exception (str_me+" job size must be even. Received:"+str(n))
        run = 1
        while (tBefore-tStart+delay) < walltimef:
            if p%2 == 0:
                dst = (p+1)%n
                succ = mpi_send(dst, tBefore, data_size, mpi_comm_world)
                if not succ: raise Exception(str_me+" send failed at p"+str(p) );
            else:
                succ = mpi_recv(mpi_comm_world)
                if not succ: raise Exception(str_me+" recv failed at p:"+str(p) );
            mpi_barrier(mpi_comm_world)

            tnow = mpi_wtime(mpi_comm_world)
            duration = tnow - tBefore
            if duration > 0.0:
                print("[finished run:%d, of:%d, time:%f, %s-%d]runtime:%f, p:=%d, data_size=%d. " 
                       % (run, job_no,tnow,  pattern, n, duration, p, data_size))
            else: raise Exception (str_me+" communication finished in no time")

            computetime = delay - tnow%60     #assuming,  tStart < step
            mpi_ext_sleep(computetime, mpi_comm_world)
            tBefore = mpi_wtime(mpi_comm_world)
            run += 1
        
   
    else:
        raise Exception (str_me+" undefined pattern.")

    #print runtime and sleep for remaining time
    duration = mpi_wtime(mpi_comm_world) - tBefore
    if duration > 0.0:
        print("[finished:%d %s-%d]runtime:%f, p:=%d, data_size=%d." 
               % (job_no, pattern, n, duration, p, data_size))
    else: raise Exception (str_me+" communication finished in no time")

    time_elapsed = mpi_wtime(mpi_comm_world) -tStart
    computetime = walltimef - time_elapsed
    if computetime > 0.0:
        mpi_ext_sleep(computetime, mpi_comm_world)
    mpi_finalize(mpi_comm_world)


#%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
def simple_communications(mpi_comm_world, walltimef, pattern = None):
    """ mpi application with simple communciation patterns """
  
    n = mpi_comm_size(mpi_comm_world)                   # total # ranks
    p = global_core_id = mpi_comm_rank(mpi_comm_world)  # rank of this process

    t0 = mpi_wtime(mpi_comm_world)
    data_size = 1024

    host = mpi_ext_host(mpi_comm_world)
    #core_id = p % cores_per_node
    #core = host.cores[core_id]

    str_me = "app_communciation.py::simple_communications() --"
    if p == 0:
        print ("     * app pattern:%s [p:%d/%d] time:%f"
                % (pattern, global_core_id, n, mpi_wtime(mpi_comm_world))  )
    elif p == n-1 :
        print ("     * app pattern:%s [p:%d/%d] time:%f"
                % (pattern, global_core_id, n, mpi_wtime(mpi_comm_world))  )

    if pattern is None:            #application with NO communciation
        computetime = walltimef   # its actual runtime of the application
        if computetime <= 0:
            raise Exception (str_me+" ERROR! computetime:"+str(computetime)  )

    elif pattern == "allreduce":
        r = mpi_allreduce(p, mpi_comm_world, data_size)
        if r is None:
            raise Exception("error occurred in running allreduce")
        tnow = mpi_wtime(mpi_comm_world)
        print("[finished]%s-%d p:=%d data_size=%d duration=%f" 
                % (pattern, n, p, data_size, tnow-t0 ))

    elif pattern =="first-last":  #first process sends to last and vice versa
        if (p==0):                            #p0 sends to last process
            succ = mpi_send(n-1, t0, 128, mpi_comm_world)
            if not succ: raise Exception(pattern+" send failed at p"+str(p) );
            succ = mpi_recv(mpi_comm_world)
            if not succ: raise Exception(pattern+" recv failed at p"+str(p) );
        if (p == n-1):
            succ = mpi_recv(mpi_comm_world)   #p(n-1) receives
            if not succ: raise Exception(pattern+" recv failed at p"+str(p) );
            t1 = mpi_wtime(mpi_comm_world)
            succ = mpi_send(0, t1, 128, mpi_comm_world)
            if not succ: raise Exception(pattern+" send failed at p"+str(p) );

    elif pattern  == "ring":           #ping pong in a ring
        origin = 0
        if p == origin:
            succ = mpi_send( (p+1)%n, t0, data_size, mpi_comm_world )
            if not succ: raise Exception(pattern+" send failed at p"+str(p) );
            succ = mpi_recv(mpi_comm_world)
            if not succ: raise Exception(pattern+" recv failed at p"+str(p) );
        else:
            succ = mpi_recv(mpi_comm_world)
            if not succ: raise Exception(pattern+" recv failed at p"+str(p) );
            t1 = mpi_wtime(mpi_comm_world)
            succ = mpi_send( (p+1)%n , t1, data_size, mpi_comm_world)
            if not succ: raise Exception(pattern+" send failed at p"+str(p) );

    elif pattern == "bipartite": #bipartitte/ even-odd
        if p%2 == 0:             # the even ranks are senders
            t1 = mpi_wtime(mpi_comm_world)
            succ = mpi_send( (p+1)%n, t1, data_size, mpi_comm_world)
            if not succ: raise Exception(pattern+" send failed at p"+str(p) );
        else:
              succ = mpi_recv(mpi_comm_world)
        #mpi_barrier()

    elif pattern == "broadcast":
        if p == 0: mpi_ext_sleep(100, mpi_comm_world)
        #mpi_bcast(root, data, mpi_comm, data_size=4)
        r = mpi_bcast(0, 100+p, mpi_comm_world)
        print("%f: myapp: rank %d bcast(root=0, data=%d) %s" %
              (mpi_wtime(mpi_comm_world), p, p+100, "=> %d"%r if r is not None else ": failed"))

    else:
        raise Exception ("unknows pattern requested:"+str(pattern))

    #sleep for remaining time
    tnow = mpi_wtime(mpi_comm_world)
    computetime = walltimef - (tnow-t0)
    if computetime > 0.0:
        mpi_ext_sleep(computetime, mpi_comm_world)

    # Finalize mpi and the simulation
    mpi_finalize(mpi_comm_world)


#%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
def app_one_way(mpi_comm_world, walltimef, *args):
    """send/receive among first and last partition"""

    #read arguments
    args = args[0]   # arguments is received as a tuple (['3d', [0, 128, 128]],
    partitions = args[0]
    tStart = mpi_wtime(mpi_comm_world)
    #get data size and other parameters
    try:
        size = args[1]
    except IndexError:
        size = 1024
    n = mpi_comm_size(mpi_comm_world)
    p = mpi_comm_rank(mpi_comm_world)

    #EXCEPTION when partitions == 1, because mpi_send is blocking
    if partitions == 1:
        partitions = 2   #sends from first half to last half
    partitions = abs(partitions)
   
    #send
    if p < (n/partitions):
        dst = (n - n/partitions) + p
        succ = mpi_send(dst, tStart, 128, mpi_comm_world)
        if not succ: raise Exception("app_one_way send failed at p"+str(p) );
    #receive
    if p >= (n - n/partitions):
	src = (n - n/partitions) + p
        succ = mpi_recv(mpi_comm_world)   #p(n-1) receives
        if not succ: raise Exception("app_one_way recv failed at p"+str(p) );
    #print runtime and sleep for remaining time
    duration = tStart - mpi_wtime(mpi_comm_world)
    if duration > 0.0:
        print("[finished]app_one_way-%d/%d p:=%d data_size=%d duration=%f" 
               % (n, partitions, p, size, duration))
    computetime = walltimef - duration
    if computetime > 0.0:
        mpi_ext_sleep(computetime, mpi_comm_world)
    mpi_finalize(mpi_comm_world)
