#!/usr/bin/env python2

import pprint
import re
import subprocess
import time


_connections = []


def _build_regex(attrs):
    regex = ""

    for attr in attrs:
        if not regex:
            regex += r"^"
        else:
            regex += r"\t"
        regex += r"(?P<%s>.+?)" % attr

    regex += r"$"

    return regex


def exec_command(cmd):
    p = subprocess.Popen(cmd.split(), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out, err = p.communicate()

    if err:
        print "Got stderr: %s" % err

    if out:
        out_lines = out.split('\n')
        return out_lines


def get_channels():
    cmd = "sudo rabbitmqctl list_channels pid connection consumer_count messages_unacknowledged"

    p = subprocess.Popen(cmd.split(), stdout=subprocess.PIPE)
    out, err = p.communicate()

    if err:
        print "Got stderr: %s" % err

    out_lines = out.split('\n')

    channels = []

    def parse_item(line):
        regex = _build_regex(['pid', 'connection', 'consumer_count', 'messages_unacknowledged'])
        m = re.search(regex, line)
        if m:
            item = m.groupdict()
            return item

    for line in out_lines:
        if line.startswith('<'):
            chan = parse_item(line)
            channels.append(chan)

    return channels


def get_connections():
    cmd = "sudo rabbitmqctl list_connections pid name host port peer_host peer_port state timeout"

    def parse_conn(line):
        regex = r"^(?P<pid>.+?)\t(?P<name>.+?)\t(?P<host>.+?)\t(?P<port>.+?)\t(?P<peer_host>.+?)\t(?P<peer_port>.+?)\t(?P<state>.+?)\t(?P<timeout>.+?)$"
        m = re.search(regex, line)
        if m:
            conn = m.groupdict()
            return conn


    p = subprocess.Popen(cmd.split(), stdout=subprocess.PIPE)
    out, err = p.communicate()

    if err:
        print "Got stderr: %s" % err

    out_lines = out.split('\n')

    conns = []

    for line in out_lines:
        if line.startswith('<'):
            conn = parse_conn(line)
            conns.append(conn)

    return conns


def get_connection_for_pid(pid):
    global _connections
    if not _connections:
        _connections = get_connections()

    return filter(lambda x: x['pid'] == pid, _connections)[0]


def print_clogged():
    chans = get_channels()

    unacked = filter(lambda x: int(x['messages_unacknowledged']) > 0, chans)
    interesting = []

    for chan in unacked:
        conn = get_connection_for_pid(chan['connection'])
        if conn:
            interesting.append((conn['pid'], conn['peer_host'], conn['peer_port'], chan['messages_unacknowledged']))

    for item in sorted(interesting, key=lambda x: x[0]):
        print "{0:>50s} to {1}:{2} has {3} unacked mesg.s".format(*item)


def shotgun():
    # poll rabbit queues and shoot connections that have over 200 waiting unacked
    # these tend to be the troublemakers in the mix

    chans = get_channels()

    bad = filter(lambda x: int(x['messages_unacknowledged']) > 200, chans)

    if bad:
        print "Found [%s] connections with over 200 unacked. Closing..." % len(bad)
        print "%s" % bad

        for bad_item in bad:
            exec_command('sudo rabbitmqctl close_connection %s BadAck' % bad_item['connection'])


if __name__ == '__main__':
    print_clogged()
    shotgun()
