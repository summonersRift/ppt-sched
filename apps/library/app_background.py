#
# app_background.py :- application for background foreground testing
#
from sys import path
path.append('../..')
from ppt import *



def background(mpi_comm_world, walltimef, *args):
    """ mpi application with simple communciation patterns """

    str_me = " * app_background.py::background() --"
    args = args[0]   # arguments is received as a tuple (['3d', [0, 128, 128]],
    tStart = mpi_wtime(mpi_comm_world)
    try:
        #app[7] = [app[0], "even-odd", data_size, foreground_starts]
        job_no, pattern, data_size, foreground_times = args
    except IndexError:
        raise Exception (str_me+" missing arguments.")


    tStart = mpi_wtime(mpi_comm_world)
    #discard unnecessay foreground times
    foreground_times = [d for d in foreground_times if d >= tStart and d < tStart+walltimef]


    n = mpi_comm_size(mpi_comm_world)  #total ranks
    p = mpi_comm_rank(mpi_comm_world)  # rank of this process

    if p == 0:
        print (" * background app pattern:%s [p:%d/%d] time:%f, foreground_times:%s"
                % (pattern, p, n, mpi_wtime(mpi_comm_world) , foreground_times) )

    for ti in foreground_times:
        tnow = mpi_wtime(mpi_comm_world)
	if (ti - tnow > 0):
	        mpi_ext_sleep(ti - tnow, mpi_comm_world)
	if p ==0:  print " * background communication starts for a foreground app at", ti

        tBefore = mpi_wtime(mpi_comm_world)
        if pattern == "even-odd":
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
    
        elif pattern == "allreduce":
            r = mpi_allreduce(p, mpi_comm_world, data_size)
            if r is None:
                raise Exception("error occurred in running allreduce")
            mpi_barrier(mpi_comm_world)
        elif pattern == "bipartite":
            raise Exception ("unknows pattern requested:"+str(pattern))
        elif pattern == "broadcast":
            raise Exception ("unknows pattern requested:"+str(pattern))
        elif pattern =="first-last":
            raise Exception ("unknows pattern requested:"+str(pattern))
        else:
            raise Exception ("unknows pattern requested:"+str(pattern))

        #print runtime and sleep for remaining time
        duration = mpi_wtime(mpi_comm_world) - tBefore
        if duration > 0.0:
            print("[finished:backkground-%d %s-%d]runtime:%f, p:=%d, data_size=%d." 
                   % (job_no, pattern, n, duration, p, data_size))
        else: raise Exception (str_me+" communication finished in no time")
    
    

        tnow = mpi_wtime(mpi_comm_world)
	if p == 0: print " * background communication ended for a foreground app at",tnow


    tnow = mpi_wtime(mpi_comm_world)
    computetime = walltimef - (tnow-tStart)
    if computetime < 0:
        raise Exception(" * background start:"+str(tStart)+", walltimef:"+str(walltimef)
                        + ", endtime:"+ str(tStart+walltimef)+ ", tnow:"+str(tnow))
    elif computetime >0:
         mpi_ext_sleep(computetime, mpi_comm_world)
    mpi_finalize(mpi_comm_world)



#%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
#%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
def background_old(mpi_comm_world, walltimef, *args): #pattern = None, observer_times):
    """ mpi application with simple communciation patterns """

    args = args[0]
    pattern = args[0]
    foreground_times = args[1]

    now = mpi_wtime(mpi_comm_world)
    foreground_times = [d for d in foreground_times if d >= now and d < now+walltimef]
    #discard unnecessay foreground times


    n = mpi_comm_size(mpi_comm_world)                   # total # ranks
    p = global_core_id = mpi_comm_rank(mpi_comm_world)  # rank of this process

    t0 = mpi_wtime(mpi_comm_world)
    sz = 102400
   


    host = mpi_ext_host(mpi_comm_world)
    #core_id = p % cores_per_node
    #core = host.cores[core_id]

    str_me = "app_background.py::background() --"

    if p == 0:
        print (" * background app pattern:%s [p:%d/%d] time:%f, foreground_times:%s"
                % (pattern, p, n, mpi_wtime(mpi_comm_world) , foreground_times) )

    for ti in foreground_times:
        tnow = mpi_wtime(mpi_comm_world)
	if (ti - tnow > 0):
	        mpi_ext_sleep(ti - tnow, mpi_comm_world)
	if p ==0:  print " * background communication starts for a foreground app at", ti

        if pattern == "bipartite":
            sz *= 2
            if p%2 == 0:         #even ranks sends first
                succ = mpi_send( (p+1)%n, ti, sz, mpi_comm_world)
                if not succ: raise Exception(pattern+" send failed at p"+str(p) );
            else:
                succ = mpi_recv(mpi_comm_world)
                if not succ: raise Exception(pattern+" mpi_recv failed at p"+str(p) );
            mpi_barrier(mpi_comm_world)        #mpi_reduce(0, 0, mpi_comm_world)
            if p%2 == 1:         # the even ranks are senders
                succ = mpi_send( (p-1)%n, ti, sz, mpi_comm_world)
                if not succ: raise Exception(pattern+" send failed at p"+str(p) );
            else:
                succ = mpi_recv(mpi_comm_world)
                if not succ: raise Exception(pattern+" mpi_recv failed at p"+str(p) );

        elif pattern == "allreduce":
            tnow = mpi_wtime(mpi_comm_world)
            r = mpi_allreduce(p, mpi_comm_world, sz)
            if r is None:  raise Exception("error occurred in running allreduce")
            t = mpi_wtime(mpi_comm_world)
            if p == n-1 or p == 0 :
                print("[finished]%s-%d p:=%d data_size=%d duration=%f background" 
                     % (pattern, n, p, sz, (t-tnow)))


        elif pattern == "broadcast":
            r = mpi_broadcast(p, mpi_comm_world, sz)
            raise Exception ("No definitions just yet."+str(pattern))

        elif pattern =="first-last":  #First process sends to last and vice versa

            if (p==0):                            #p0 sends to last process
                succ = mpi_send(n-1, ti, 128, mpi_comm_world)
                if not succ: raise Exception(pattern+" send failed at p"+str(p) );
                succ = mpi_recv(mpi_comm_world)
                if not succ: raise Exception(pattern+" recv failed at p"+str(p) );

            if (p == n-1):
                succ = mpi_recv(mpi_comm_world)   #p(n-1) receives
                if not succ: raise Exception(pattern+" recv failed at p"+str(p) );
                t1 = mpi_wtime(mpi_comm_world)
                succ = mpi_send(0, ti, 128, mpi_comm_world)
                if not succ: raise Exception(pattern+" send failed at p"+str(p) );

        else:
            raise Exception ("unknows pattern requested:"+str(pattern))
        tnow = mpi_wtime(mpi_comm_world)
	if p == 0: print " * background communication ended for a foreground app at",tnow

    if (p==0):
        #my_host = mpi_ext_host(mpi_comm_world)
        tnow = mpi_wtime(mpi_comm_world)
        computetime = walltimef - (tnow-t0)
	if computetime < 0:
            raise Exception(" * background start:"+str(t0)+", walltimef:"+str(walltimef)
                            + ", endtime:"+ str(t0+walltimef)+ ", tnow:"+str(tnow))
        elif computetime >0:
            mpi_ext_sleep(computetime, mpi_comm_world)
        #mpi_terminate(mpi_comm_world)

    mpi_finalize(mpi_comm_world)
       


######################################### empty applications for timing

def background_empty(mpi_comm_world, walltimef, *args):
    """ mpi application with no communciation """

    p = global_core_id = mpi_comm_rank(mpi_comm_world)  # rank of this process
    if (p==0):
        #pattern = args[0]
        if walltimef <= 0: raise Exception ("background_empty() invalid walltimef" )
    mpi_ext_sleep(walltimef, mpi_comm_world)
    mpi_finalize(mpi_comm_world)


def foreground_empty(mpi_comm_world, walltimef, *args):
    """ mpi application with no communciation """

    p = global_core_id = mpi_comm_rank(mpi_comm_world)  # rank of this process
    if (p==0):
        pattern = args[0]
        now = mpi_wtime(mpi_comm_world)
        print " * foreground p0 running, now:", now, ", walltimef:", walltimef, ", pattern:",pattern
        if walltimef <= 0: raise Exception ("foreground_empty() invalid walltimef" )
    mpi_ext_sleep(walltimef, mpi_comm_world)
    mpi_finalize(mpi_comm_world)
