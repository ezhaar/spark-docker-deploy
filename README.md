This is my quick automation script for launching a spark cluster using
hadoop-2.4.0, scala-2.10.4

**Note:** This is a work in progress and some functionality shown in the help menu
may not be available yet.

```bash

# Get Help
spark-deploy -h

# Create a 2 slaves spark cluster
spark-deploy --slaves 2 --cluster_name c1

```

**ToDo**:
- check if a keyhost container and dns-server are already running
- ability to add more nodes
- ability to delete a cluster
- verbosity
- dry run
