import sys
import time
from MQTT import MQTT

from nyamuk import nyamuk
from nyamuk import nyamuk_const as NC
import json

def handle_payload(ev):
    msg = ev.msg
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
        mote_list = []
        for i in range(1,5):
            database_data = db_subscribe(i)
            update(database_data, mote_list)
        
        print "mote_list", mote_list
        for i in mote_list:
            if data2['id'] == str(i[0]):
                datastream = data2['id']
                value = data2['value']
                
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
            
def db_subscribe(level):
    while 1:
        topic_db = MQTT.topic_db + str(level)
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
    
def update(database_data, mote_list):
    try:
        pilist = json.loads(str(database_data))
        pis = pilist["pi"]
        print pis
        for i in pis:
            motes = i['mote']
            for j in motes:
                mote_data = [str(j['id']),str(j['TS']),str(j['sen'])]
                mote_list.append(mote_data)
    except ValueError:
        print "Error"   
             
if __name__ == '__main__':
    start_nyamuk(MQTT.server, MQTT.client_3d, MQTT.topic_temp)
    
    