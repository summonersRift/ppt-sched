import math



def balanced_cube(size):
    """make balanced cube from integer """
    debug_me = False
   
    y = z = 0
    power = math.log(size,2)
    #print "size:%d, power:%f" % (size, power)

    if power < 1:
        return []
        #raise Exception(" invalid conversion of size to cube:"+str(size))

    x = math.ceil(power/3.0)
    power -= x

    if power  !=0:
      y = math.ceil(power/2.0)
      z = power - y
    #print "(X x Yx Z) = (%d,%d,%d)" % (x, y, z)
    x = int(pow(2,x))
    y = int(pow(2,y))
    z = int(pow(2,z))
    if debug_me:
        print "size:%d, log(size,2):%f, x,y,z " % (size, power,x,y,z)
    if x*y*z != size:
        return []
    return [x,y,z]


if __name__=='__main__':
    sizes = [2, 4, 8, 16, 32, 48, 50,  64, 123]
    for size in sizes:
        dim = balanced_cube(size)
        print " size-%d, %s " % (size, dim)
