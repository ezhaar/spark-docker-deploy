This is my quick automation script for launching a spark cluster using
hadoop-2.4.0, scala-2.10.4

**TESTED ON** Arch Linux, docker -v1.4.1

**Note:** This is a work in progress and some functionality shown in the help menu
may not be available yet.

```bash

# Get Help
spark-deploy -h

# Create a 2 slaves spark cluster
sudo ./spark-deploy launch --slaves 2 --cluster_name c1

```

**ToDo**:
- Exception handling
- open ports for web interfaces
- check if a keyhost container and dns-server are already running
- ability to add more nodes
- ability to delete a cluster
- replace python subprocess calls with docker-api calls
- ability to mount a volume on master
- verbosity
- dry run
