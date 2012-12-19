import sys
import time
from MQTT import MQTT

from nyamuk import nyamuk
from nyamuk import nyamuk_const as NC
import json

def handle_payload(ev):
    msg = ev.msg
    c = 0
    print "\ttopic : " + msg.topic
    if msg.payload is not None:
        data = msg.payload
        data = str(data)
        print "payload = ", data
        try:
            data2 = json.loads(data)
        except ValueError:
            return
        
#        update mote list
        print "updating list"
        db_connect()
        database_data = db_subscribe()
        database_data = str(database_data)
        try:
            pilist = json.loads(database_data)
            pis = pilist["pi"]
            print pis
            mote_list = []
            for i in pis:
                if str(i['id']) == pi_id:
                    motes = i['mote']
                    for j in motes:
                        mote_data = str(j['id']) + ' ' + str(j['TS'])
                        mote_list.append(mote_data)
                    break
            print mote_list
            for i in mote_list:
                avg.append([0,0.0])
        except ValueError:
            print "Error"
        
#        averaging code
        transmit = 0
        for i in mote_list:
            if data2['id'] == str(i[:7]):
                field_id = str(i[8:(len(i))])
                datastream = data2['id']
#                averaging code
                if avg[c][0] < 10:
                    avg[c][0] += 1
                    avg[c][1] += float(data2['value'])
                    transmit = 0
                else:
                    value = str((avg[c][1])/(avg[c][0]))
                    avg[c][0] = 0
                    avg[c][1] = 0
                    transmit = 1
                break
            else:
                c += 1
        
        if transmit:
#        transmit data to cosm
            print datastream
            print value
            MQTT.packet['id'] = datastream
            MQTT.packet['value'] = str(value)
            data = json.dumps(MQTT.packet)
            start = time.time()
            print data
            while 1:
                if (MQTT.threed_pub.publish(MQTT.topic_3d, data, retain = True) == 0):
                    print "published = " + data
                    break
                if (time.time() - start > 30):
                    print "publish failed, Reconnecting"
                    start = connect()
            MQTT.threed_pub.loop()
            
def connect():
    while 1:
        MQTT.threed_pub = nyamuk.Nyamuk(MQTT.client_3bpub, None, None, MQTT.server)
        rcpub = MQTT.threed_pub.connect()
        if (rcpub != NC.ERR_SUCCESS):
            print "Can't connect"
            time.sleep(30)
        else:
            print "Publish Connection successful"
            return
            
def db_subscribe():
    while 1:
        topic_db = MQTT.topic_db + pi_id[0]
        rdb =MQTT.database3.subscribe(topic_db,0)
        if rdb == NC.ERR_SUCCESS:
            ev = MQTT.database3.pop_event()
            if ev != None: 
                if ev.type == NC.CMD_PUBLISH:
                    payload = ev.msg.payload
                    if payload is not None and str(ev.msg.topic) == topic_db:
                        ev = None
                        break
                elif ev.type == 1000:
                    print "Network Error. Msg = ", ev.msg
            rdb = MQTT.database3.loop()
    return payload

def db_connect():
    while 1:
        MQTT.database3 = nyamuk.Nyamuk(MQTT.client_db3d, None, None, MQTT.server)
        rdb = MQTT.database3.connect()
        if (rdb != NC.ERR_SUCCESS):
            print "Can't connect"
            time.sleep(30)
        else:
            print "Connection successful"
            return

def start_nyamuk(server, client_id, topic, username = None, password = None):
    connect()
    while 1:
        threed = nyamuk.Nyamuk(client_id, username, password, server)
        rc = threed.connect()
        
        if rc != NC.ERR_SUCCESS:
            print "Can't connect"
            sys.exit(-1)
        
        rc = threed.subscribe(topic, 0)
        
        while rc == NC.ERR_SUCCESS:
            ev = threed.pop_event()
            if ev != None: 
                if ev.type == NC.CMD_PUBLISH:
                    handle_payload(ev)
                elif ev.type == 1000:
                    print "Network Error. Msg = ", ev.msg
                    break
            rc = threed.loop()
            if rc == NC.ERR_CONN_LOST:
                threed.logger.info("subscriber reconnecting in 30sec")
                time.sleep(30)
                break
            
if __name__ == '__main__':
    pi_id = sys.argv[1]
    level = pi_id[0]
    db_connect()
    database_data = db_subscribe()
    avg = []
    start_nyamuk(MQTT.server, MQTT.client_3d, MQTT.topic_temp)
    