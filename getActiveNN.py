#! /usr/bin/python
import subprocess as SP
import sys,getopt

# Parse arguments
argv=sys.argv[1:]
if len(argv) == 0:
 print 'getActiveNN.py -c <hdfs-site.xml location>'
 sys.exit(2)

try:
 opts, args = getopt.getopt(argv,"hc:",["config_loc="])
except getopt.GetoptError:
 print 'getActiveNN.py -c <hdfs-site.xml location>'
 sys.exit(2)

for opt, arg in opts:
 if opt == '-h':
  print 'getActiveNN.py -c <hdfs-site.xml location>'
  sys.exit(2)
 elif opt in ("-c", "--config_loc"):
  ConfLoc = arg

# Get nameservice name
args = ["hdfs --config "+ConfLoc+" getconf -confKey dfs.nameservices"]
p = SP.Popen(args, shell=True, stdout=SP.PIPE)
for line in p.stdout.readlines():
 if line.startswith("Error:"):
  sys.exit("ERROR: Nameservice not found")
 else:
  NameService=line.strip()

# Get namenodes
args = ["hdfs --config "+ConfLoc+" getconf -confKey dfs.ha.namenodes."+NameService]
p = SP.Popen(args, shell=True, stdout=SP.PIPE)
for line in p.stdout.readlines():
 if line.startswith("Error:"):
  sys.exit("ERROR: Namenodes not found")
 else:
  NameNodes=line.strip().split(",")

# Get Active namenode
for nn in NameNodes:
 args = ["hdfs  --config "+ConfLoc+" haadmin -getServiceState "+nn]
 p = SP.Popen(args, shell=True, stdout=SP.PIPE)
 for line in p.stdout.readlines():
  if line.startswith("Error:"):
   sys.exit("ERROR: Active namenode not found")
  else:
   if line.strip()=="active":
    activeNN=nn

# Get Active namenode host
args = ["hdfs --config "+ConfLoc+" getconf -confKey dfs.namenode.rpc-address."+NameService+"."+activeNN]
p = SP.Popen(args, shell=True, stdout=SP.PIPE)
for line in p.stdout.readlines():
 if line.startswith("Error:"):
  sys.exit("ERROR: Active namenode host not found")
 else:
  activeNNDetails=line.strip().split(":")
  activeNNHost=activeNNDetails[0]

print(activeNNHost)
