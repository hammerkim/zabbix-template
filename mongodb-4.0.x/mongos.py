#!/usr/bin/python
#
from __future__ import print_function
import sys, getopt
from pyzabbix import ZabbixMetric, ZabbixSender
from pymongo import MongoClient
import time
import subprocess
import re
import urllib
import platform
import os

ZBSERVER = '127.0.0.1'
ZBPORT = 10051

os_ = platform.system()

#for item in os.environ:
#    print("%s = %s" % (item, os.environ[item]))

#print(" TEMP = " + os.environ["TEMP"])



#sys.stdout = io.TextIOWrapper(sys.stdout.detach(), encoding = 'utf-8')
#sys.stderr = io.TextIOWrapper(sys.stderr.detach(), encoding = 'utf-8')

try:
    opts, args = getopt.getopt(sys.argv[1:],"h:n:p:u:s:")
except getopt.GetoptError:
    print ("Usage:\nmongos.py -h <hostname or ip mongod is listening to> -n <hostname in zabbix> -u <user> -s <secret pass> -p <mongod port>")
    sys.exit(2)

for opt, arg in opts:
    if opt == '-h':
        mongohost = arg
    elif opt == '-n':
        zbhost = arg
    elif opt == '-p':
        mongoport = arg
    elif opt == '-u':
        muser = arg
    elif opt == '-s':
        mpass = arg

# faced a strange problem - zabbix server add a space character to parameter passed to script
# and spend a hour to catch the bug :(
mongohost = mongohost.rstrip()
zbhost = zbhost.rstrip()


#print("DEBUG: STR: " + arg)
#print("DEBUG: RES: " + res)
#print("DEBUG: ERR: " + err + str(len(err)))

state = 0

# Read saved opcounters from previous check
try:
    temp_dir = os.environ["TEMP"]
    if os_ == "Windows":
        #f = open("C:\\Users\\hammer\\" + mongohost + "-mongos-opcounters")
        f = open(temp_dir + "\\" + mongohost + "-mongos-opcounters")
    elif os_ == "Linux":
        f = open("/tmp/" + mongohost + "-mongos-opcounters")

    s = f.read()
    f.close()
    ts, _insert, _query, _update, _delete, _getmore, _command, _commandRepl, _flushes, _vsize, _rsize, _q_readers, _q_writers, _a_readers, _a_writers, \
    _net_in, _net_out, _conn = s.split(" ")
except Exception as e:
    ts, _insert, _query, _update, _delete, _getmore, _command, _commandRepl, _flushes, _vsize, _rsize, _q_readers, _q_writers, _a_readers, _a_writers,  _net_in, _net_out, _conn = [1,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]
    print(e)

print("History opcounters")
print(ts, _insert, _query, _update, _delete, _getmore, _command, _commandRepl, _flushes, _vsize, _rsize, _q_readers, _q_writers, _a_readers, _a_writers, _net_in, _net_out, _conn)

# Get serverStatus stats
try:
    mo = MongoClient('mongodb://' + muser + ':' + urllib.quote(mpass) + '@' + mongohost + ':' + mongoport + '/admin', connectTimeoutMS=5000)
except Exception as e:
    print ('Can\'t connect to '+mongohost)
    print ("ERROR:", e)
    sys.exit(1)

res = mo.admin.command('serverStatus')
now = time.time()

insert  = int((float(res['opcounters']['insert'])))
query   = int((float(res['opcounters']['query'])))
update  = int((float(res['opcounters']['update'])))
delete  = int((float(res['opcounters']['delete'])))
getmore = int((float(res['opcounters']['getmore'])))
command = int((float(res['opcounters']['command'])))

# not in monogos
commandRepl = 0
# not in monogos
flushes = 0

vsize= int((float(res['mem']['virtual'])))
rsize= int((float(res['mem']['resident'])))

# not in monogos
q_readers = 0
q_writers = 0
a_readers = 0
a_writers = 0

#_q_readers= int((float(res['globalLock']['currentQueue']['readers'])))
#_q_writers= int((float(res['globalLock']['currentQueue']['Zwriters'])))
#_a_readers= int((float(res['globalLock']['activeClients']['readers'])))
#_a_writers= int((float(res['globalLock']['activeClients']['writers'])))

net_in= int((float(res['network']['bytesIn'])))
net_out= int((float(res['network']['bytesOut'])))

conn=int((float(res['connections']['current'])))

insert_  = int((float(insert) - float(_insert))/((now - float(ts))))
query_   = int((float(query) - float(_query))/((now - float(ts))))
update_  = int((float(update) - float(_update))/((now - float(ts))))
delete_  = int((float(delete) - float(_delete))/((now - float(ts))))
getmore_ = int((float(getmore) - float(_getmore))/((now - float(ts))))
command_ = int((float(command) - float(_command))/((now - float(ts))))
commandRepl_ =  int((float(commandRepl) - float(_commandRepl))/((now - float(ts))))

flushes_ =  int(flushes)

# unit is Megabytes, so convert to bytes
vsize_   =  int(vsize) * 1024 * 1024
rsize_   =  int(rsize) * 1024 * 1024

q_readers_ = int(q_readers)
q_writers_ = int(q_writers)
a_readers_ = int(a_readers)
a_writers_ = int(a_writers)
net_in_    = int((float(net_in) - float(_net_in))/((now - float(ts))))
net_out_   = int((float(net_out) - float(_net_out))/((now - float(ts))))
conn_      = int(conn)

# Save opcounters
try:
    if os_ == "Windows":
        temp_dir = os.environ["TEMP"]
        #f = open("C:\\Users\\hammer\\" + mongohost + "-mongos-opcounters", 'w')
        f = open(temp_dir + "\\" + mongohost + "-mongos-opcounters", 'w')
    elif os_ == "Linux":
        f = open("/tmp/" + mongohost + "-mongos-opcounters", 'w')

    f.write(str(int(now)) + ' ' + str(insert) + ' ' + str(query) + ' ' + str(update) + ' ' + \
            str(delete) + ' ' + str(getmore) + ' ' + str(command) + ' ' + str(commandRepl) + ' ' +\
            str(flushes) + ' ' + str(vsize) + ' ' + str(rsize) + ' ' +\
            str(q_readers) + ' ' + str(q_writers) + ' ' + str(a_readers) + ' ' + str(a_writers)  + ' ' +\
            str(net_in) + ' ' + str(net_out) + ' ' + str(conn)
            )
    f.close()
except Exception as e:
    print("Can't update stats!")
    print(e)
    sys.exit(1)

mongos_total_ops = int(insert_) + int(query_) + int(update_) + int(delete_) + int(getmore_) + int(command_)
print( insert_, query_, update_, delete_, getmore_, command_, commandRepl_, flushes_, vsize_, rsize_, q_readers_, q_writers_, a_readers_, a_writers_, net_in_, net_out_, conn_)

#mongos_total_ops=0
err = 'OK'
state = 1

packet = [ ZabbixMetric(zbhost, 'mongos_state', state),
           ZabbixMetric(zbhost, 'mongos_errstr', err) ]

packet.append(ZabbixMetric(zbhost, "mongos_insert", int(insert_)))
packet.append(ZabbixMetric(zbhost, "mongos_query", int(query_)))
packet.append(ZabbixMetric(zbhost, "mongos_update", int(update_)))
packet.append(ZabbixMetric(zbhost, "mongos_delete", int(delete_)))
packet.append(ZabbixMetric(zbhost, "mongos_getmore", int(getmore_)))
packet.append(ZabbixMetric(zbhost, "mongos_command", int(command_)))
packet.append(ZabbixMetric(zbhost, "mongos_commandRepl", int(commandRepl_)))
packet.append(ZabbixMetric(zbhost, "mongos_flushes", int(flushes_)))
packet.append(ZabbixMetric(zbhost, "mongos_vsize", int(vsize_)))
packet.append(ZabbixMetric(zbhost, "mongos_rsize", int(rsize_)))
packet.append(ZabbixMetric(zbhost, "mongos_qReaders", int(q_readers_)))
packet.append(ZabbixMetric(zbhost, "mongos_qWriters", int(q_writers_)))
packet.append(ZabbixMetric(zbhost, "mongos_aReaders", int(a_readers_)))
packet.append(ZabbixMetric(zbhost, "mongos_aWriters", int(a_writers_)))
packet.append(ZabbixMetric(zbhost, "mongos_netin", int(net_in_)))
packet.append(ZabbixMetric(zbhost, "mongos_netout", int(net_out_)))
packet.append(ZabbixMetric(zbhost, "mongos_conn", int(conn_)))
packet.append(ZabbixMetric(zbhost, "mongos_total_ops", int(mongos_total_ops)))

try:
    t = ZabbixSender(zabbix_port = ZBPORT, zabbix_server = ZBSERVER).send(packet)
except Exception as e:
    print("Can't send daat to zabbix server")
    print(e)
    sys.exit(1)

print(t)
