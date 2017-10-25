#
# trace_filter.py :- filters the trace 
#

out   = open("ANL-Intrepid-2009-filtered", "w")

cluster_size    = 9216*16 #147456
min_submittitme = 16e6  #16mil

num_jobs      = 0
num_oversized = 0
num_selected  = 0
size_selected = 0
selected = []

max_size_selected = 500000 


#read file reverse order since I want latest jobs
for job in reversed(open("ANL-Intrepid-2009-1").readlines()):
    #discard comments and empty
    if job.strip()[0] == ";" or len(job.strip()) == 0:
        #out.write(job)
        continue
    num_jobs += 1

    fields = job.strip().split()

    submittime = int(fields[1])
    num_req_processors = int(fields[7])

    if num_req_processors > cluster_size:
            num_oversized += 1
            continue
    #ok sized job
    #if submittime > min_submittitme:

    size_selected += num_req_processors
    selected.insert(0, job)

    if size_selected >  max_size_selected:
        break

#write to out
for job in selected:
    out.write(job)
out.close()

first_submittime = int(selected[0].strip().split()[1])
last_submittime  = int(selected[len(selected)-1].strip().split()[1])

diff_submittime = last_submittime-first_submittime


prev = 0
for t in xrange (first_submittime, last_submittime):
    num_present = 0   #count
    vol_present = 0
    for d in selected:
        if  int(d.split()[1]) >= t and t<=int(d.split()[1])+int(d.split()[8] ):
            num_present += 1
            vol_present += int(d.split()[7])
    if num_present != prev:
        print t," ", num_present, " ", vol_present
        prev = num_present




print ("# * cluster_size: %d" % cluster_size)
print ("# * JOBS total : %d, oversized: %d"  % (num_jobs , num_oversized)  )
print ("#   * selected :%d,  volume: %s" % (len(selected), size_selected) )
print ("#   * diff_submittime: %d " % (diff_submittime) )
 



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
