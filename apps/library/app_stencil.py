#
# app_stencil.py :- an mpi application for use by multiapp scheduler
#

from sys import path
path.append('../..')
from ppt import *

#import math
#import os
#import random

debug_me =  False


def stencil(mpi_comm_world, walltimef, *args):
    """MPI 2d/3d stencil communciation with netighbors"""

    # i used args to maintain same format as other mpi applications

    #print " * stencil:args", args
    args = args[0]   # arguments is received as a tuple (['3d', [0, 128, 128]],)
    pattern = args[0]
    dim_app = args[1]

    debug = False
    tStart = mpi_wtime(mpi_comm_world)                      #tStart
    n = mpi_comm_size(mpi_comm_world)                   # total # ranks
    p = global_core_id = mpi_comm_rank(mpi_comm_world)  # rank of this process
    #app_phases = ['cpu*5', 'comm', 'cpu*10',"comm","cpu*5", "comm","cpu*10","comm"]
    sz = 512
    str_me = " * app_stencil.py::stencil() --"

    #debug msgs and validation in rank0
    if p == 0:
        print (" * START stencil-%s [p:%d/%d] time:%f dim_app:%s"
                % (pattern, global_core_id, n, mpi_wtime(mpi_comm_world), dim_app)  )
        #checking and validating input
        if len(dim_app)<1: raise Exception (str_me+" invalid dim_app:"+str(dim_app) )
        if walltimef <= 0: raise Exception (str_me+" ERROR! walltimef<0::"+str(walltimef) )

    #elif p == n :
    #    print ("     * stencil-%s [p:%d/%d] time:%f"
    #            % (pattern, global_core_id, n, mpi_wtime(mpi_comm_world))  )
 

    # for faster running, we do this here
    if dim_app == 1:  dim_app.append(1)
    if dim_app == 2:  dim_app.append(1) 
    if len(dim_app) == 3:
        xmax, ymax, zmax = map(int, dim_app)

    # find neighbors
    neighbors = neighboring_ranks(p, pattern, xmax, ymax, zmax)


    #calculate something from a 7 point stencil -- we use isend/irecv atm
    for dest in neighbors:
        succ = mpi_isend(dest, tStart, sz, mpi_comm_world )
        if not succ: raise Exception(pattern+" mpi_isend failed at p"+str(p) );

    msgs_required = len(neighbors)
    while  msgs_required !=0 :
        succ = mpi_recv(mpi_comm_world)     # Wait for a receive
        if not succ: raise Exception(pattern+" recvsend failedat p"+str(p) );
        msgs_required -= 1

    # to synchronize all the ranks 
    mpi_barrier(mpi_comm_world)

    # finalize and termiante
    if (p == 0):
        tNow = mpi_wtime(mpi_comm_world)
        print (" * FINISH stencil-%s [p:%d/%d] time:%f dim_app:%s, RUNTIME:%f"
               % (pattern, p, n, mpi_wtime(mpi_comm_world), dim_app, tNow-tStart)  )
        #my_host = mpi_ext_host(mpi_comm_world)
        computetime = walltimef - (tNow-tStart)
        mpi_ext_sleep(computetime, mpi_comm_world)
        mpi_terminate(mpi_comm_world)    #obaida, terminate

    mpi_finalize(mpi_comm_world)



def neighboring_ranks (pid, stencil="2d", xmax=1, ymax=1, zmax=1):
    """ Return a list of neighbors of a supplied node id or pid of an application """

    neighbors = set()
    #neighbors = []
    if stencil == "2d":
        x, y, z = get_xyz_from_process_id(pid, xmax,ymax, zmax)
        #neighbors.append( z*xmax*ymax + y*xmax + ((x+1)%xmax) )  # x+
        #neighbors.append( z*xmax*ymax + y*xmax + ((x-1)%xmax) )  # x-
        #neighbors.append( z*xmax*ymax + ((y+1)%ymax)*xmax + x )  # y+
        #neighbors.append( z*xmax*ymax + ((y-1)%ymax)*xmax + x )  # y-

        neighbors.add( z*xmax*ymax + y*xmax + ((x+1)%xmax) )  # x+
        neighbors.add( z*xmax*ymax + y*xmax + ((x-1)%xmax) )  # x-
        neighbors.add( z*xmax*ymax + ((y+1)%ymax)*xmax + x )  # y+
        neighbors.add( z*xmax*ymax + ((y-1)%ymax)*xmax + x )  # y-

        if debug_me:
            print (" stencil 2d neighbors of %d:%s" % (pid, neighbors))

    elif stencil == "3d":
        x, y, z = get_xyz_from_process_id(pid, xmax, ymax, zmax)
        #neighbors.append( z*xmax*ymax + y*xmax + ((x+1)%xmax) )  # x+
        #neighbors.append( z*xmax*ymax + y*xmax + ((x-1)%xmax) )  # x-
        #neighbors.append( z*xmax*ymax + ((y+1)%ymax)*xmax + x )  # y+
        #neighbors.append( z*xmax*ymax + ((y-1)%ymax)*xmax + x )  # y-
        #neighbors.append( ((z+1)%zmax)*xmax*ymax + y*xmax + x )  # x+
        #neighbors.append( ((z-1)%zmax)*xmax*ymax + y*xmax + x )  # x+

        neighbors.add( z*xmax*ymax + y*xmax + ((x+1)%xmax) )  # x+
        neighbors.add( z*xmax*ymax + y*xmax + ((x-1)%xmax) )  # x-
        neighbors.add( z*xmax*ymax + ((y+1)%ymax)*xmax + x )  # y+
        neighbors.add( z*xmax*ymax + ((y-1)%ymax)*xmax + x )  # y-
        neighbors.add( ((z+1)%zmax)*xmax*ymax + y*xmax + x )  # x+
        neighbors.add( ((z-1)%zmax)*xmax*ymax + y*xmax + x )  # x+

        if debug_me:
            print ("      * stencil 3d neighbors(no duplicate) of %d:%s" % (pid, neighbors))
    else:
        raise Exception (" neighboring_ranks() invalid stencil: "+str(stencil) )

    if len(neighbors) > 0:
       return neighbors
    else:
       raise Exception ("neighboring_ranks() found no neighbor.")

def get_xyz_from_process_id (pid, xmax, ymax, zmax):
    #helper for neighboring ranks, less params for fastest
    z = int (  pid / (xmax * ymax)         )
    y = int ( (pid % (xmax * ymax)) / xmax )
    x = int ( (pid % (xmax * ymax)) % xmax )
    return x, y, z

