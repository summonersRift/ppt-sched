#
# trace_filter.py :- filters the trace 
#

out   = open("LLNL-Atlas-2006-2-April12007-May12007", "w")

cluster_size    = 4096
min_submittitme = 1175410801 #
max_submittitme = 1178002801  #16mil
unix_start = 1163199901

num_jobs  = 0
selected  = 0
discarded = 0

max_size_selected = 500000 
size_selected = 0
max_size = 0

#read file reverse order since I want latest jobs
for job in open("LLNL-Atlas-2006-2").readlines():
    #discard comments and empty
    if job.strip()[0] == ";" or len(job.strip()) == 0:
        out.write(job)
        continue
    num_jobs += 1


    fields = job.strip().split()

    submittime = int(fields[1])
    size_req = int(fields[7])
    time_req = int(fields[8])
    walltimef = float(fields[3]) 

    if (size_req > cluster_size or size_req <2 
        or time_req<1 or walltimef <1):
        discarded += 1
        continue
    if size_req %2 != 0:
        discarded += 1
        print " * job requested odd number of ranks:", num_req_processors
        continue
    unix_submittime = submittime + unix_start
    if unix_submittime > min_submittitme and unix_submittime < max_submittitme:
        selected += 1
        size_selected += size_req
        if size_req >max_size:
             max_size = size_req
        out.write(job)
out.close()

print ("# * cluster_size: %d" % cluster_size)
print ("# * JOBS total : %d, discarded: %d, selected:%d"  % (num_jobs, discarded, selected)  )
print ("# * total # of ranks in all selected jobs:%d, size of largest job:%d"  
             % (size_selected, max_size)  )
 
#job_id = int(fields[0])
#submit_time = int(fields[1])
#wait_time = int(fields[2]) 
#run_time = float(fields[3]) 
#num_allocated_processors = int(fields[4])
#avg_cpu_time_used = float(fields[5])
#used_memory = int(fields[6])
#req_time = int(fields[8])
#req_mem = int(fields[9])
#status = int(fields[10])
#user_id = int(fields[11])
#group_id = int(fields[12])
#executable_num = int(fields[13])
#queue_num = int(fields[14])
#partition_num = int(fields[15])
#preceding_job_num = int(fields[16])
#think_time_from_preceding_job = int(fields[17])
