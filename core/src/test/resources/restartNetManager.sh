#!/usr/bin/env bash

# Host for ssh connection to node with primary
host_replication_cluster=$1

# Root password for ssh connection to primary cluster
root_password_on_remote_node=$2

sshpass -p ${root_password_on_remote_node} ssh -o StrictHostKeyChecking=no root@${host_replication_cluster} -T << EOSSH systemctl restart network
EOSSH

