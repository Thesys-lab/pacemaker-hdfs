import time
import subprocess
import pprint


def split_report(report):
    lines = [line.strip() for line in report.split('\n')]
    blocks = []
    i = 0
    while i < len(lines):
        if (len(lines[i]) == 0 and i + 20 < len(lines)
                and len(lines[i+19]) == 0 and len(lines[i+20]) == 0):
            block = lines[i+1:i+19]
            blocks.append(block)
            i += 19
        i += 1
    return blocks


def parse_block(block):
    info = {}
    for line in block:
        key, value = line.split(':', 1)
        key = key.strip()
        value = value.strip()
        info[key] = value
    return info


def parse_report(report):
    blocks = split_report(report)
    infos = [parse_block(b) for b in blocks]
    info_by_hostname = {}
    for info in infos:
        info_by_hostname[info['Hostname']] = info
    return info_by_hostname


def get_host_status(infos):
    status = {}
    for hostname, info in infos.items():
        status[hostname] = info['Decommission Status']
    return status


def dfsadmin_report():
    result = subprocess.run(
            ['/tmp/hadoop-3.2.0/bin/hdfs', 'dfsadmin', '-report'],
            stdout=subprocess.PIPE)
    report = result.stdout.decode('utf-8')
    return report


def monitor_decommissioned(targets, interval=30, max_checks=20):
    pp = pprint.PrettyPrinter()
    for i in range(max_checks):
        time.sleep(interval)

        report = dfsadmin_report()
        infos = parse_report(report)
        status = get_host_status(infos)
        print('Current DataNode status:')
        pp.pprint(status)

        exist = all([hostname in status for hostname in targets])
        if not exist:
            print('Error: not all targets are live')
            return False

        decom = all([status[h] == 'Decommissioned' for h in targets])
        if decom:
            return True

    print('Error: reach the max number of checks')
    return False


if __name__ == '__main__':
    pp = pprint.PrettyPrinter()
    report = dfsadmin_report()
    infos = parse_report(report)
    status = get_host_status(infos)
    print('Current DataNode status:')
    pp.pprint(status)

