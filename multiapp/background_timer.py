import sys

def background_timer(shell_out):
    """retrieve foreground times and scheduled times for background-timer pattern"""
    # because of the memory issue it is done this way
    timer_out = shell_out.replace("sh.out", "timer.out")
    fp = open(timer_out, "w")
    with open (shell_out) as FileObj:
        #i = 0, you can use break if too long timer file
        for line in FileObj:
            #  * foreground times (save it): [506519, 509600, 510154, 513774, 518684, 
            #                                 527756, 530705, 549552, 555531, 558729]
            if 'foreground times' in line:      #line of interest
                print " * line is:", line
                times = line.split(": [")[-1].split("]")[0]
                times = times.replace(" ", "")
                print " * foreground_times:", times
                fp.write(times)
                break

    jobq = shell_out.replace("sh.out", "jobq.out")
    with open (jobq) as FileObj:
        #i = 0, you can use break if too long timer file
        starttimes = []
        for line in FileObj:
            if 'foreground_empty' in line:      #line of interest
                fields = line.split()
                starttimes.append(fields[10])
        starttimes = ','.join(str(i) for i in starttimes)
        print " * background_timer() foreground_starttimes:", starttimes
        fp.write("\n"+starttimes)
    fp.close()



if __name__=='__main__':
 
    if len(sys.argv[1:]) ==  0:
        raise Exception (" * background_timer.py: need a background shell out file")
    else:
        shell_out = sys.argv[1]
        background_timer(shell_out)
