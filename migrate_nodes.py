"""
Input: A file with a list of (node, old_disk_group, new_disk_group), one per line.
Process:
    - Decommissions each node from old_disk_group.
    - Restarts Datanode daemon
    - Erases the data directory of the datanode
    - Add the node to the new_disk_group
Output: True if migration successful for all the nodes. False in case of failures
"""
import os
import time
import subprocess
import getpass
from monitor import monitor_decommissioned


BASE_DIR = "/tmp/hadoop-3.2.0"
HDFS_PATH = os.path.join(BASE_DIR, "bin/hdfs")


def get_disk_group_file(disk_group):
    return os.path.join(BASE_DIR, "etc/hadoop", disk_group + ".txt")


def get_disk_group_excludes_file(disk_group):
    return os.path.join(BASE_DIR, "etc/hadoop", disk_group + "-excludes.txt")


# Returns True if the node is in the disk_group
def is_node_in_disk_group(node, disk_group):
    r = subprocess.run(["grep", "-Fxq", node, get_disk_group_file(disk_group)],
                       stdout=subprocess.PIPE)
    return r.returncode == 0


def remove_line(line, filepath):
    # TODO: Not so efficient for large no.of nodes, but it's okay for now
    with open(filepath, "r") as fin:
        lines = fin.readlines()
    with open(filepath, "w") as fout:
        for l in lines:
            if l.strip("\n") != line:
                fout.write(l)


def add_line(line, filepath):
    with open(filepath, "a") as fout:
        fout.write(line + "\n")
    # r = subprocess.run(["echo", line, ">>", filepath], stdout=subprocess.PIPE)
    # return r.returncode


def refresh_nodes():
    r = subprocess.run([HDFS_PATH, "dfsadmin", "-refreshNodes"])
    return r.returncode


def decommission_nodes(decommission_list):
    """
    Add the hostname to excludes list of the disk_group
    Call dfsadmin refreshNodes and then the monitoring function.
    :param decommission_list A list of tuples (hostname, old_disk_group)
    :return True if successfully decommissioned, else False
    """
    print("Adding to exclude lists")
    for hostname, disk_group in decommission_list:
        # Validate whether the hostname is present in this disk_group
        assert(is_node_in_disk_group(hostname, disk_group))

        # Add the hostnames to excludes list
        excludes_file = get_disk_group_excludes_file(disk_group)
        add_line(hostname, excludes_file)

    print("refresh nodes")
    refresh_nodes()

    # Wait until all the nodes are decommissioned
    result = monitor_decommissioned([host for host, dg in decommission_list])

    if not result:
        print("Error occurred in decommission! Please check logs")

    # If successfully decommissioned, remove them from the disk_group_file and
    # excludes_file
    print("Removing nodes from the disk group and exclude nodes")
    for hostname, disk_group in decommission_list:
        disk_group_file = get_disk_group_file(disk_group)
        excludes_file = get_disk_group_excludes_file(disk_group)
        remove_line(hostname, excludes_file)
        remove_line(hostname, disk_group_file)

    return True


def add_nodes(addition_list):
    """
    Adds the nodes to the given disk groups
    :param addition_list: A list of tuples (hostname, new_disk_group)
    :return:
    """
    print("Adding the nodes to new disk-groups")
    for hostname, disk_group in addition_list:
        disk_group_file = get_disk_group_file(disk_group)
        add_line(hostname, disk_group_file)

    refresh_nodes()

    # Start the datanode daemons on all the hosts
    print("Starting datanode daemons")
    for hostname, _ in addition_list:
        r = subprocess.run(["pdsh", "-w", hostname, HDFS_PATH, "--daemon",
                            "start", "datanode"], stdout=subprocess.PIPE)
        print(r.stdout.decode("utf-8"))

    return True


def clean_restart_datanode(hostname):
    """
    Restarts the datanode daemon on the hostname and also cleans up the
    data directory
    :param hostname: Hostname of the ndoe
    :return: None
    """
    print("%s: Stopping datanode daemon" % hostname)
    # Stop and start the datanode daemon on the datanode
    r = subprocess.run(["pdsh", "-w", hostname, HDFS_PATH, "--daemon",
                        "stop", "datanode"], stdout=subprocess.PIPE)
    print(r.stdout.decode("utf-8"))

    # Delete the data directory
    print("%s: Deleting the data directory" % hostname)
    username = getpass.getuser()
    data_dir = os.path.join("/tmp", "hadoop-%s" % username)
    subprocess.run(["pdsh", "-w", hostname, "rm", "-rf", data_dir])


def migrate_node(hostname, old_disk_group, new_disk_group):
    """
    Migrates the node from old_disk_group to new_disk_group. Use this API for
    migrating a single node
    """
    print("Migrating %s from %s to %s" % (hostname, old_disk_group,
                                          new_disk_group))
    decommission_nodes([(hostname, old_disk_group)])
    clean_restart_datanode(hostname)
    add_nodes([(hostname, new_disk_group)])
    print("Migrating DONE %s from %s to %s" % (hostname, old_disk_group,
                                               new_disk_group))


if __name__ == "__main__":
    migration_list = [("apt002.apt.emulab.net", "RS-LEGACY-6-3-1024k",
                       "RS-LEGACY-7-3-1024k")]

    decommission_list = [(hostname, old_dg) for hostname, old_dg, new_dg
                         in migration_list]
    addition_list = [(hostname, new_dg) for hostname, old_dg, new_dg
                     in migration_list]

    decommission_nodes(decommission_list)
    for hostname in [a for a, _ in decommission_list]:
        clean_restart_datanode(hostname)

    add_nodes(addition_list)
