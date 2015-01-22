#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


from __future__ import with_statement
import sys 
import argparse
from subprocess import Popen, PIPE
from shutil import copy2


def parse_arguments():

    parser = argparse.ArgumentParser(description="Create a Spark Cluster on "
                                     "Docker Host.", epilog="Example Usage: "
                                     "./spark-deploy launch " \
                                     "--slaves 2 --cluster_name "\
                                     "clusterName")
    parser.add_argument('action', help = "launch|destroy|add-nodes")
    parser.add_argument("-c", "--cluster_name", metavar="",
                        dest="cluster_name",
                        action="store",
                        help="Name for the cluster.")
    parser.add_argument("-s", "--slaves", metavar="", dest="num_slaves",
                        type=int, action="store",
                        help="Number of slave nodes to spawn.")
    parser.add_argument("-v", "--verbose", dest="verbose",
                        action="store_true", help="verbose output")
    parser.add_argument("-D", "--dryrun", dest="dryrun",
                        action="store_true", help="Dry run")

    return parser.parse_args()


def shell_exec(args):
    return Popen(args, stdout=PIPE).communicate()[0].decode("utf-8")


def restart_dnsmasq():
    args = ["docker", "exec", "-d", "dns_server", "/root/dnsmasq_sighup.sh"]
    return shell_exec(args)


def run_container(container_name, image_name):
    args = ["docker", "run", "-d", "--name", container_name, image_name]
    return shell_exec(args)
    

def run_container_master(container_name, dns_info, image_name):
    
    args = ["docker", "run", "-d", "--volumes-from", "keyhost", "--name",
    container_name, "--dns-search=localdomain", "-h",
    container_name+'.localdomain', dns_info, "--dns=8.8.8.8", image_name]
    return shell_exec(args)

def run_container_slave(container_name, master_name, dns_info, image_name):
    
    args = ["docker", "run", "-d", "--volumes-from", "keyhost",
    "--volumes-from", master_name, 
    "--name", container_name, "--dns-search=localdomain", "-h",
    container_name+'.localdomain', dns_info, "--dns=8.8.8.8", image_name]
    return shell_exec(args)


def get_container_ip(container_name):
    args = ["docker", "inspect", "-f", "'{{ .NetworkSettings.IPAddress }}'",
            container_name] 
    cont_ip = shell_exec(args)
    return cont_ip.replace("'", "")


def create_files(num_slaves, master_name, master_ip, slaves_dict):
    hosts_file = open("/tmp/hosts.localdomain", "w")
    slaves_file = open("/tmp/slaves", "w")
    hosts_file.write(master_ip + "\t" + master_name + ".localdomain\t" +
            master_name + "\n")
    for key, val in slaves_dict.items():
        hosts_file.write(val + "\t" + key + ".localdomain\t" + key + "\n")
        slaves_file.write(key + "\n")
    

def main():
   
    args = parse_arguments()
    cluster_name = args.cluster_name
    num_slaves = args.num_slaves
    dns_img = "ezhaar/docker-dnsmasq"
    keyhost_img = "ezhaar/docker-ssh-keys"
    spark_img = "ezhaar/docker-spark"
    master_name = cluster_name + "-master"
    
    # boot dns-server
    dns_server_id = run_container("dns_server", dns_img).rstrip()
    dns_server_ip = get_container_ip("dns_server")
    dns_info = "--dns="+str(dns_server_ip)
    dns_rootfs = "/var/lib/docker/devicemapper/mnt/" + dns_server_id
    
    # boot keyhost
    keyhost_id = run_container("keyhost", keyhost_img)
    
    #boot master
    master_id = run_container_master(master_name, dns_info,
    spark_img).rstrip()
    master_ip = get_container_ip(master_name).rstrip()
    master_rootfs = "/var/lib/docker/devicemapper/mnt/" + master_id
    
    #boot slaves
    slaves_dict = {}
    for i in range(1, num_slaves + 1):
        sl_name = cluster_name + "-slave" + str(i)
        sl_id = run_container_slave(sl_name, master_name, dns_info, spark_img)
        slaves_dict[sl_name] = get_container_ip(sl_name).rstrip()

    # create hosts and slaves files
    create_files(num_slaves, master_name, master_ip, slaves_dict)
    
    hosts_file_path = dns_rootfs + "/rootfs/etc/"
    root_file_path = master_rootfs + "/rootfs/root/"
    #hadoop_file_path = master_rootfs + "/rootfs/usr/local/hadoop-2.4.0/etc/hadoop/"
    
    copy_args = ["mv", "/tmp/hosts.localdomain", hosts_file_path]
    status = shell_exec(copy_args)
    
    #copy_args = ["cp", "/tmp/slaves", hadoop_file_path]
    copy_args = ["mv", "start-bdas.sh", root_file_path]
    status = shell_exec(copy_args)
    
    copy_args = ["mv", "/tmp/slaves", root_file_path]
    status = shell_exec(copy_args)
    
    restart_dnsmasq()


if __name__ == "__main__":
    main()
