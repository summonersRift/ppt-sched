#
# resource_manager.py :- manages hpc resources
#

import math
import random

class ResourceManager(object):
    
    def __init__(self, num_hosts, cores_per_node):
        str_me = "   * ResourceManager::__init__() --"
        print (str_me +"initializing ")
        self.allocate = []
        self.cores_per_node = 0
        self.sem_resource_manager_busy = False
        self.debug_me = False #debug me?

        #self.hpcsim_dict = hpcsim_dict
        #self.total_hosts = hpcsim_dict['intercon'].num_hosts()
        #self.cores_per_node = hpcsim_dict['sched_dict']['cores_per_node']

        self.total_hosts = num_hosts
        self.cores_per_node = cores_per_node

        self.available = [i for i in xrange(self.total_hosts)]
        self.snapshot  = [i for i in xrange(self.total_hosts)]
        self.available_cores = [int(math.floor(float(i)/cores_per_node))  \
                                for i in xrange(num_hosts*cores_per_node)]
        #print ("\n%s available: %s \n\t  ......\n\t  ......\n\t %s"
        #           % (hostmap[:17],  hostmap[-(cores_per_host):] ) )


        

    def is_available (self, num_nodes):
        """verify if a number of nodes are free"""

        if num_nodes <= len(self.available):
            return True 
        else:
            return False


    def is_free(self, nodes):
        """ check whether a set of 'nodes' are available"""

        str_me = "   *ResourceManager::is_free()  -- "
        if set(nodes) < set(self.available):         #nodes is subset of available
            return True
        else:
            return False

    def num_free_hosts(self):
        """returns num of hosts free"""

        return len(self.available)

    def num_free_cores(self):
        return len(self.available)*self.cores_per_node


    def alloc(self, nodes):   #app_dict
        """allocates a set of nodes for the requested application"""

        str_me = "   * ResourceManager::allocate() --"
        if self.debug_me:
            print ("%s alloc nodes %s " %(str_me, nodes) )
        else:
            print ("%s alloc len(nodes): %d " %(str_me, len(nodes)) )

        if (len(nodes)<1):
            raise Exception (str_me+" ERROR! empty nodes list:"+str(nodes) )
            
        # delete 'nodes' from available and add to self.allocate and allocation_map
        if set(nodes) <= set(self.available):
            self.available = [item for item in self.available if item not in nodes]
            self.allocate = self.allocate + nodes
            #app_dict['hostmap'] = nodes
            return True
        else:
            raise Exception ("%s nodes not free:%s, available:%s" 
                                 % (str_me, nodes, self.available ))
            

    
    def free (self, nodes):
        """ we claim the nodes from the application"""

        str_me = "   * ResourceManager::free()  --"
        if self.debug_me:
            print ("%s freeing nodes:%s" %(str_me, nodes))
        else:
            print ("%s freeing len(nodes): %d " %(str_me, len(nodes)) )


        # delete 'nodes' from self.allocate and add to available
        if set(nodes) <= set(self.allocate):
            self.allocate = [item for item in self.allocate if item not in nodes]
            self.available = self.available + nodes
            return True

        else:
            raise Exception (str_me+" nodes werent allocated. u wanted to free:"+ 
                          str(nodes) +" allocate:"+str(self.allocate) )
