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
import io

ZBSERVER = 'nngp005'
ZBPORT = 10051

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
    #f = open("/tmp/" + mongohost + "-mongos-opcounters")
    f = open("C:\\Users\\hammer\\" + mongohost + "-mongos-opcounters")
    s = f.read()
    f.close()
    ts, insert, query, update, delete, getmore, command, commandRepl, flushes, vsize, res, q_readers, q_writers, a_readers, a_writers, \
    net_in, net_out, conn = s.split(" ")
except Exception as e:
    ts, insert, query, update, delete, getmore, command, commandRepl, flushes, vsize, res, q_readers, q_writers, a_readers, a_writers,  net_in, net_out, conn = [1,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]
    print(e)

print("History opcounters")
print(ts, insert, query, update, delete, getmore, command, commandRepl, flushes, vsize, res, q_readers, q_writers, a_readers, a_writers, net_in, net_out, conn)

# Get serverStatus stats
try:
    mo = MongoClient('mongodb://' + muser + ':' + urllib.quote(mpass) + '@' + mongohost + ':' + mongoport + '/admin', connectTimeoutMS=5000)
except Exception as e:
    print ('Can\'t connect to '+mongohost)
    print ("ERROR:", e)
    sys.exit(1)

res = mo.admin.command('serverStatus')
now = time.time()

_insert  = int((float(res['opcounters']['insert'])))
_query   = int((float(res['opcounters']['query'])))
_update  = int((float(res['opcounters']['update'])))
_delete  = int((float(res['opcounters']['delete'])))
_getmore = int((float(res['opcounters']['getmore'])))
_command = int((float(res['opcounters']['command'])))

# not in monogos
_commandRepl = 0
# not in monogos
_flushes = 0

_vsize= int((float(res['mem']['virtual'])))
_res= int((float(res['mem']['resident'])))

# not in monogos
_q_readers = 0
_q_writers = 0
_a_readers = 0
_a_writers = 0

#_q_readers= int((float(res['globalLock']['currentQueue']['readers'])))
#_q_writers= int((float(res['globalLock']['currentQueue']['Xwriters'])))
#_a_readers= int((float(res['globalLock']['activeClients']['readers'])))
#_a_writers= int((float(res['globalLock']['activeClients']['writers'])))

_net_in= int((float(res['network']['bytesIn'])))
_net_out= int((float(res['network']['bytesOut'])))

_conn=int((float(res['connections']['current'])))

insert_  = int((float(_insert) - float(insert))/((now - float(ts))))
query_   = int((float(_query) - float(query))/((now - float(ts))))
update_  = int((float(_update) - float(update))/((now - float(ts))))
delete_  = int((float(_delete) - float(delete))/((now - float(ts))))
getmore_ = int((float(_getmore) - float(getmore))/((now - float(ts))))
command_ = int((float(_command) - float(command))/((now - float(ts))))
commandRepl_ =  int((float(_commandRepl) - float(commandRepl))/((now - float(ts))))

flushes_ =  int(_flushes)
vsize_   =  int(_vsize)
res_   =  int(_res)

q_readers_ = int(_q_readers)
q_writers_ = int(_q_writers)
a_readers_ = int(_a_readers)
a_writers_ = int(_a_writers)
net_in_    = int((float(_net_in) - float(net_in))/((now - float(ts))))
net_out_   = int((float(_net_out) - float(net_out))/((now - float(ts))))
conn_      = int(_conn)

# Save opcounters
try:
    #f = open("/tmp/" + mongohost + "-mongos-opcounters", 'w')
    f = open("C:\\Users\\hammer\\" + mongohost + "-mongos-opcounters", 'w')
    f.write(str(int(now)) + ' ' + str(_insert) + ' ' + str(_query) + ' ' + str(_update) + ' ' + \
            str(_delete) + ' ' + str(_getmore) + ' ' + str(_command) + ' ' + str(_commandRepl) + ' ' +\
            str(_flushes) + ' ' + str(_vsize) + ' ' + str(_res) + ' ' +\
            str(_q_readers) + ' ' + str(_q_writers) + ' ' + str(_a_readers) + ' ' + str(_a_writers)  + ' ' +\
            str(_net_in) + ' ' + str(_net_out) + ' ' + str(_conn)
            )
    f.close()
except Exception as e:
    print("Can't update stats!")
    print(e)

print( insert_, query_, update_, delete_, getmore_, command_, commandRepl_, flushes_, vsize_, res_, q_readers_, q_writers_, a_readers_, a_writers_, net_in_, net_out_, conn_)

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
packet.append(ZabbixMetric(zbhost, "mongos_insert", int(flushes_)))
packet.append(ZabbixMetric(zbhost, "mongos_insert", int(vsize_)))
packet.append(ZabbixMetric(zbhost, "mongos_insert", int(res_)))
packet.append(ZabbixMetric(zbhost, "mongos_insert", int(q_readers_)))
packet.append(ZabbixMetric(zbhost, "mongos_insert", int(q_writers_)))
packet.append(ZabbixMetric(zbhost, "mongos_insert", int(a_readers_)))
packet.append(ZabbixMetric(zbhost, "mongos_insert", int(a_writers_)))
packet.append(ZabbixMetric(zbhost, "mongos_netin", int(net_in_)))
packet.append(ZabbixMetric(zbhost, "mongos_netout", int(net_out_)))
packet.append(ZabbixMetric(zbhost, "mongos_conn", int(conn_)))
packet.append(ZabbixMetric(zbhost, "mongos_total_ops", int(0)))

try:
    t = ZabbixSender(zabbix_port = ZBPORT, zabbix_server = ZBSERVER).send(packet)
except Exception as e:
    print("Can't send daat to zabbix server")
    print(e)
    sys.exit(1)

print(t)
