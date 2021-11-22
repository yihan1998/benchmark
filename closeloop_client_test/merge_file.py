import glob, os, sys, csv

read_files = glob.glob(r'rtt_core_*.txt')

rtt = []

name = "rtt.txt"

for read_file in read_files:
    with open(read_file) as f:
        file = csv.reader(f)
        for line in file:
            if not len(line):
                continue
            rtt.append(int(line[0]))
    
for read_file in read_files:
    os.remove(read_file)

rtt.sort()

tail_latency = rtt[int(len(rtt) * 0.9)]
median_latency = rtt[int(len(rtt) * 0.5)]
average_latency = round(sum(rtt)/len(rtt),2)

output = "[" + str(sys.argv[1]) + "] average rtt: " + str(average_latency) + ", 99 percent rtt: " + str(tail_latency) + ", median rtt: " + str(median_latency) + "\n"

print(output)

with open(name, "a") as f:
    f.writelines(output)