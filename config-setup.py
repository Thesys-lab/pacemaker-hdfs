"""
Run this script to generate the configuration files for total workers and each
of the disk groups.
"""
import argparse
import json
import os
import subprocess

BASE_DIR = "/tmp/hadoop-3.2.0"

# Read in the no.of nodes in the cluster
NUM_NODES = 0
EXP_NAME = ""

# Assumption: Master node is always node0


def get_hostname_mapping():
    """
    Gets the mapping from type h-type to rr-type
    :return: Dictionary with key as node0, node1 etc. and value as rr-type DNS
    """
    hosts = "node[1-%s].%s.apt.emulab.net" % (NUM_NODES-1, EXP_NAME)
    result = subprocess.run(["pdsh", "-w", hosts, "hostname -f"],
                            stdout=subprocess.PIPE, check=True)
    lines = result.stdout.decode("utf-8").strip().split("\n")
    lines = list(map(lambda line: (line.split()[0][:-1],
                                   line.split()[1].strip()), lines))

    return dict(lines)


def get_worker_nodes(hostname_mapping):
    return [value for key, value in hostname_mapping.items() if key != "h0"]


def validate_config(disk_group_config):
    print("Validating JSON config file")
    total_nodes = disk_group_config["totalNumNodes"]
    disk_groups = disk_group_config["diskGroups"]
    n = 0
    for group in disk_groups:
        ec_policy = group["ecPolicy"]
        num_nodes = group["numNodes"]
        width, parity = [int(x) for x in ec_policy.split("-") if x.isdigit()]
        # Assert that the EC policy has at-least the required no.of nodes
        assert num_nodes >= (width + parity), \
            "Not enough nodes allocated for %s policy" % ec_policy
        n += num_nodes

    assert n <= total_nodes, "Allocating more than totalNumNodes"

    # No.of workers are total_nodes-1 or
    # total_nodes (when a datanode is running on master)
    assert (n == total_nodes) or (n == total_nodes-1), \
        "Every node (other than namenode) MUST be a part of a disk group"


def create_workers_file(hostname_mapping):
    """
    Write the list of workers into etc/hadoop/workers file
    :return: None
    """
    print("Creating etc/hadoop/workers file")
    workers_file_path = os.path.join(BASE_DIR, "etc/hadoop/workers")
    if not os.path.exists(workers_file_path):
        os.mknod(workers_file_path)

    with open(workers_file_path, "w") as fout:
        for host in get_worker_nodes(hostname_mapping):
            fout.write(host + "\n")


def create_workers_for_disk_groups(disk_group_config, hostname_mapping):
    print("Creating config files for disk groups")
    n = 0
    workers = get_worker_nodes(hostname_mapping)

    for group in disk_group_config["diskGroups"]:
        policy = group["ecPolicy"]
        num_nodes = group["numNodes"]

        # Create an empty excludes file for the policy
        excludes_file = os.path.join(BASE_DIR, "etc/hadoop", policy + "-excludes.txt")
        if not os.path.exists(excludes_file):
            os.mknod(excludes_file)

        # Create the workers list for the policy
        f = os.path.join(BASE_DIR, "etc/hadoop", policy + ".txt")
        with open(f, "w") as fout:
            for node in workers[n:n+num_nodes]:
                fout.write(node + "\n")
        n += num_nodes


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("config_file",
                        help="Configuration for disk groups in JSON format")
    parser.add_argument("exp_name", help="Name of the experiment")
    args = parser.parse_args()

    # Load the disk group configuration
    disk_group_config = json.load(open(args.config_file, "r"))
    NUM_NODES = disk_group_config["totalNumNodes"]
    EXP_NAME = args.exp_name

    # Validate the configuration file provided
    validate_config(disk_group_config)

    # TODO: Do we really need a mapping? Just a list of hostnames in rr-format
    # should be enough?
    # Get the hostname mapping
    hostname_mapping = get_hostname_mapping()
    print(hostname_mapping)

    # Create the workers file
    create_workers_file(hostname_mapping)

    # Create workers list for disk groups
    create_workers_for_disk_groups(disk_group_config, hostname_mapping)
