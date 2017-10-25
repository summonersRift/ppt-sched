#
# algorithms_mapping.py :- all the mapping algorithms
#

import math
import random

class Mapping(object):
    mapping = None
    def __init__(self, sched_dict, resource_manager):
        self.mapping = sched_dict['mapping']
        self.resource_manager = resource_manager
        self.sched_dict = sched_dict

    #def find() -- derived class must overload function
            
class RandomMapping(Mapping):
    """Random Mapping Class"""

    def __init__(self, sched_dict, res_man):
        Mapping.__init__(self, sched_dict, res_man)

        str_me = "    * RandomMapping::__init__() -- "
        self.valid_levels = ['node', 'router', 'group']
        # requested level of randomness
        self.level = sched_dict['mapping'].split("random-")[-1]
        #check level and print debug msg
        if self.level not in self.valid_levels:
            raise Exception (str_me+" invalid randomness level:"+str(self.level))
        print ("%s initialized with level=%s" % ( str_me, str(self.level) )  )

    def find(self, total_ranks, partitions=0):
        """reuturns a list of random nodes from available in resource manager"""
        cores_per_node = self.sched_dict['cores_per_node']
        num_nodes = total_ranks / cores_per_node
        str_me = "  * RandomMapping::find() --"
        print ("%s find %d nodes in random" % (str_me, num_nodes))
        
        if not ( self.resource_manager.is_available(num_nodes) ):
            raise Exception (str_me+" ERROR: not enough nodes for the job. "+str(num_nodes) )
        else:             #enough nodes available
            found = None
            available = self.resource_manager.available
            
            # different levels of randomness 
            if self.level =="node":             # simple: shuffle and get from beginning
                random.shuffle(available)
                found = available[0:num_nodes]
            else:
                raise Exception (str_me+" unsupported randomness level:"+str(self.level))
 
        found_router = sorted(set([d/2 for d in found]))
        print ("%s requested: %d ranks(%d nodes), found: %d nodes: found not-printing, found_router=%s"
            %(str_me, total_ranks, num_nodes, len(found), str(pretty_hostmap(found_router))  ))

        #create hostmap
        if (self.resource_manager.alloc(found)  == True) :
            hostmap = []
            for node in found:
                hostmap +=  [int(math.floor(float(i)/cores_per_node)) 
                                for i in xrange( node*cores_per_node, (node+1)*cores_per_node )]
            return hostmap


class DoubleEndedMapping(Mapping):
    """Double ended scheduling [SC-2016] : to improve BW or delay for application"""

    def __init__(self, sched_dict, hpcsim_dict, res_man):
        """Initializing double-ended mapping, proposed in SC'2016"""

        Mapping.__init__(self, sched_dict, res_man)
        str_me = "    * DoubleEndedMapping::__init__() --"
    
        # implemented axis orders
        allowed_orders = ["xyz", "zxy", "xzy", "linear"] #, "linear", "linearzxy", "linearxyz"

        self.axis_order = sched_dict['mapping'].split("double-ended-")[1]
        if self.axis_order not in allowed_orders:
            raise Exception (str_me+" invalid axis order requested:"+str(self.axis_order))
        
        # a size to decide which side to scedule from, e.g. we can do 10% of whole topology
        if "divide_at" not in sched_dict:
            self.divide_at = 512                        # # of ranks here
        else:
            self.divide_at = sched_dict['divide_at']

        if "cartesian" not in sched_dict:  self.cartesian = [1,1,1]
        else:
            self.cartesian = sched_dict['cartesian']
            if len(self.cartesian)<1:
                raise Exception (str_me+" invalid cartesian dimensions:" + str(self.cartesian) )
            if len(self.cartesian) == 1 : self.cartesian.append(1)
            if len(self.cartesian) == 2 : self.cartesian.append(1)

        self.boxes, self.hosts_organized = get_cartesian_boxes(sched_dict, hpcsim_dict)

        print ("%s initialized [dir/div]: %s/%d, cartesian:%s" 
                 % (str_me, self.axis_order, self.divide_at, str(self.cartesian)))


    def find(self, total_ranks, partitions=0):
        """find num_nodes using double-ended mapping algorithm """

        cores_per_node = self.sched_dict['cores_per_node']
        num_nodes = total_ranks / cores_per_node
        str_me = "  * DoubleEndedMapping::find() --"
        #print ("%s finding %d ranks, or, %d nodes)" %(str_me, total_ranks, num_nodes) )

        if not ( self.resource_manager.is_available(num_nodes) ): #not enough nodes available
            print ("%s ERROR: not enough nodes for the job. requested=%d" % (str_me, num_nodes) )
            return []

        found = []
        available = sorted(self.resource_manager.available)


        #double-ended simple mapping
        if self.axis_order == "linear":
            if self.divide_at <= num_nodes:          #large job goes at the end
                found = available[-num_nodes:]
            else:                                    #small jobs starts at the beginning
                found = available[:num_nodes]
        else:
            available_cartesian = [d for d in self.hosts_organized if d in available]

            if self.divide_at <= num_nodes:          #large job goes at the end
                found = available_cartesian[-num_nodes:]
            else:                                    #small jobs starts at the beginning
                found = available_cartesian[:num_nodes]
 
        if (len(found) != num_nodes):
            raise Exception (str_me+" ERROR! found and requested mismatch. Requested:"
                                   +str(num_nodes)+", found:"+str(found) )

        #print ("%s found nodes:%s" % (str_me, str(found))  )
        print ("%s requested: %d ranks(%d nodes), found: %d nodes:%s"
                 %(str_me, total_ranks, num_nodes, len(found),str(pretty_hostmap(found)) )  )

        #pretty print


        #create hostmap for num_cores
        if self.resource_manager.alloc(found)  == True :
            hostmap = []
            for node in found:
                hostmap +=  [int(math.floor(float(i)/cores_per_node)) 
                                  for i in xrange( node*cores_per_node, (node+1)*cores_per_node )]
            return hostmap


class RoundRobinMapping(Mapping):
    """Round-Robin mapping"""

    def __init__(self, sched_dict, hpcsim_dict, res_man):
        """Initializing round-robin"""

        Mapping.__init__(self, sched_dict, res_man)
        str_me = "    * RoundRobinMapping::__init__() -- "
    
        self.valid_levels = ['node', 'router']
        # axis ordering supported
        allowed_orders = ["xyz", "zyx", "xzy", "zxy", "yxz" ] 
        self.level = sched_dict['mapping'].replace("round-robin-","")

        if any(d in self.level for d in self.valid_levels):
            print (str_me+"initialized with level="+str(self.level) )
        else:
            raise Exception (str_me+"invalid round-robin level:"+str(self.level))

        
        self.axis_order = "xyz"
        #fixed for TORUS
        if "-" in self.level: self.level, self.axis_order = self.level.split("-")
        else: sched_dict['mapping'] = sched_dict['mapping']+"-xyz"

        if self.axis_order not in allowed_orders:
            raise Exception (str_me+" invalid axis order:"+str(self.axis_order))
        #for dragonfly and fat tree linear axis order
        
        if "cartesian" not in sched_dict:
            self.cartesian = [1,1,1]
        else:
            self.cartesian = sched_dict['cartesian']
            if len(self.cartesian)<1:
                raise Exception (str_me+" invalid cartesian dimensions:" + str(self.cartesian) )
            if len(self.cartesian) == 1 :
                self.cartesian.append(1)
            if len(self.cartesian) == 2 :
                self.cartesian.append(1)
        # the cartesian node ordering trick
        self.boxes, self.hosts_organized = get_cartesian_boxes(sched_dict, hpcsim_dict)
        print ("%s initialized [dir:%s, Cartesian:%s" 
                 % (str_me, self.axis_order, str(self.cartesian)))
        #available_cartesian = [d for d in self.hosts_organized if d in available]
        self.resource_manager.available =  [d for d in self.hosts_organized]

    def find(self, total_ranks, partitions=0):
        """find num_nodes using round-robin mapping algorithm """

        cores_per_node = self.sched_dict['cores_per_node']
        num_nodes = total_ranks / cores_per_node
        str_me = "  * RoundRobinMapping::find() --"
        #print ("%s finding %d ranks, or, %d nodes)" %(str_me, total_ranks, num_nodes) )

        if not ( self.resource_manager.is_available(num_nodes) ): #not enough nodes available
            print ("%s ERROR: not enough nodes for the job. requested=%d" % (str_me, num_nodes) )
            return []

        found = []
        available = self.resource_manager.available
        if self.level == "node":
            found = partitioning(available, num_nodes, partitions)
        else:
            raise Exception (str_me+" ERROR! level undefined:"+self.level)
 
        if (len(found) != num_nodes):
            raise Exception (str_me+" ERROR! found and requested mismatch. Requested:"
                                   +str(num_nodes)+", found:"+str(found) )

        #print ("%s found nodes:%s" % (str_me, str(found))  )
        print ("%s requested: %d ranks(%d nodes), found: %d nodes:%s"
                 %(str_me, total_ranks, num_nodes, len(found),str(pretty_hostmap(found)) )  )

        #create hostmap for num_cores
        if (self.resource_manager.alloc(found)  == True) :
            hostmap = []
            for node in found:
                hostmap +=  [int(math.floor(float(i)/cores_per_node)) 
                                  for i in xrange( node*cores_per_node, (node+1)*cores_per_node )]
            return hostmap


def get_cartesian_boxes(sched_dict, hpcsim_dict):
        """creating node order for cartesian"""

        str_me = " * algorithms_mapping.py::get_cartesian_boxes() --"
        if "torus" not in hpcsim_dict: 
            raise Exception (str_me+" no 'torus' found.")

        if 'cartesian' not in sched_dict:
            raise Exception (str_me+" no 'cartesian' in sched_dict but boxing requested.")
        cartesian = sched_dict['cartesian']

        axis_order = (sched_dict["mapping"]).split("-")[-1]
        dims = hpcsim_dict["torus"]["dims"]

        #build a list of axis order vs dimension provided in 'torus'
        virtual_topology  = []
        for axis in list(axis_order):
            if axis =="x":
                dim =  hpcsim_dict['torus']['dimx']
            elif axis =="y":   
                dim =  hpcsim_dict['torus']['dimy']
            elif axis =="z":   
                dim =  hpcsim_dict['torus']['dimz']
            else:
                raise Exception (str_me+" axis option is not yet supported. axis:", axis)
            virtual_topology.append( [axis, dim] )

        print ("%s virtual topolgy:%s" % (str_me, virtual_topology) )


        # find the low and high point for each of the cartesian boxes, IBM Blue Gene/Q has 6 axis
        boxes = []
        i = j = k = l = m = n = 0

        for k in xrange (0, virtual_topology[2][1], cartesian[2]):    #third axis, z in xyz
          for j in xrange(0, virtual_topology[1][1], cartesian[1]):   #second axis, y in xyz
            for i in xrange(0,virtual_topology[0][1], cartesian[0]):  #first axis, x in xyz
                item = {        #assume physical ordeting is xyz
                                virtual_topology[0][0]+'min' : i,
                                virtual_topology[1][0]+'min' : j,
                                virtual_topology[2][0]+'min' : k,
                                virtual_topology[0][0]+'max' :  i + cartesian[0]-1, #cart[0] is first --z
                                virtual_topology[1][0]+'max' :  j + cartesian[1]-1, #cart[1] is first--x
                                virtual_topology[2][0]+'max' :  k + cartesian[2]-1, #cart[0] is first --y
                                'sw' : [],
                                'hosts' : [],
                       }
                boxes.append(item)
                if "mapping" in hpcsim_dict["debug_options"]:
                    print ("    [x%d,y%d,z%d] --- [x%d,y%d,z%d]" 
                             % ( item['xmin'], item['ymin'], item['zmin'], 
                                 item['xmax'], item['ymax'], item['zmax'] ) )
 
        # validate we have same number of boxes as expected nswitches/cart_volume
        nswitches = hpcsim_dict['intercon'].nswitches
        nhosts = hpcsim_dict['intercon'].nhosts
        cart_volume = 1
        for cart in cartesian:
            cart_volume *= cart
        print ("\n%s total boxes:%d, nswitches/cart_volume:%d" % (str_me, len(boxes), nswitches/cart_volume) )

        #find the box for each of the hosts
        intercon  = hpcsim_dict['intercon']
        for hid in xrange(nhosts):
          c, p = intercon.hid_to_coords(hid)
          x, y, z = c                       # x,y,z,d,e,t for bluegeneq
          # WARNING-- check if swid_to_coords retrns reverse list

          for box in boxes:
            if ((x>= box['xmin']) and (x <= box['xmax']) and
                (y>= box['ymin']) and (y <= box['ymax']) and
                (z>= box['zmin']) and (z <= box['zmax']) ):

                swid = intercon.coords_to_swid(c)
                if swid not in box['sw']:
                    box['sw'].append(swid)
                box['hosts'].append(hid)

                #if "mapping" in hpcsim_dict["debug_options"]:
                #    print ("  hid:%d swid:%d, min<=sw<=max:  %d,%d,%d<=%d,%d,%d<=%d,%d,%d" 
                #            % ( hid, swid, box['xmin'], box['ymin'], box['zmin'], x, y, z,
                #                box['xmax'], box['ymax'], box['zmax'] ) )
                break 


        #check if all the boxes are full  
        hosts = []
        for box in boxes:
            if "mapping" in hpcsim_dict["debug_options"]:
                print ("    [x%d,y%d,z%d]-[x%d,y%d,z%d] \tSwitches:%d, Hosts:%d"
                         % ( box['xmin'], box['ymin'], box['zmin'],
                             box['xmax'], box['ymax'], box['zmax'],
                             len(box['sw']), len(box['hosts'])  ))

            # box total switch validation
            if not (len(box['sw']) == cart_volume):
                raise Exception (str_me + " box doesnt have enough nodes. box:" + str(box) )

            hosts += box['hosts']
        if "mapping" in hpcsim_dict["debug_options"]:
            print ("   Final cartesian hosts list: %s" % str(hosts) )
        return boxes, hosts
        #------------------------------------------ 

def partitioning(nodes, n, partitions):
    """get number of nodes a partition"""

    str_me = "algorithms_mapping.py::partitioning()"
    if partitions == -1:
        return nodes[-n:]

    elif partitions==1 or partitions==0:
        return nodes[:n]

    if partitions%2 !=0:   #this is supported ight now
        raise Exception (" %d not divisible by 2. "%(partitions))
 
    if partitions == -2:
       ret =  nodes[0:n/2] + nodes[-n/2:]
       return ret
    elif partitions == 2:
       ret =  nodes[0:n/2] + nodes[len(nodes)/2: len(nodes)/2+n/2 ]
       return ret
    else:
        ret = partitioning(nodes[0:len(nodes)/2],  n/2, partitions/2)
        ret += partitioning(nodes[-len(nodes)/2:], n/2, partitions/2)
        return ret

def pretty_hostmap(nodes):
    if len(nodes) <5:
        return nodes
    #print "pretty_hostmap input:", nodes
    nodes = sorted(nodes)
    start = nodes[0]
    end = nodes[0]
    prev = end
    ret = "["+str(start)
    ttl = 0
    for i in xrange(1,len(nodes)):
        if nodes[i] == (end+1):
            end = nodes[i]
        else:
            start = nodes[i]
            if prev == end:
                ret += ", "+str(start)
            else:
                ret += "-"+str(end)+", "+str(start)
            prev = start
            ttl += 1
            end   = nodes[i]

    #print "pretty_hostmap output:", ret
    return ret+"-"+str(end)+"]="+str(ttl)

