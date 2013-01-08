import sys
import time
from MQTT import MQTT

from nyamuk import nyamuk
from nyamuk import nyamuk_const as NC
import json
from clouding import OpenSensePacket, OpenSense

SENSE_API_KEY = "erJRoQ7XtQ6KCFoUR4f1rA"

def handle_payload(ev):
    msg = ev.msg
    if msg.payload is not None:
        data = msg.payload
        data = str(data)
        try:
            data2 = json.loads(data)
        except ValueError:
            return
        
#        update mote list
        db_connect()
        mote_list = []
        for i in range(1,5):
            database_data = db_subscribe(i)
            update(database_data, mote_list)
        
        print "mote_list = ", mote_list
        for i in mote_list:
            if data2['id'] == i[0]:
                sen_id = i[2]
                value = data2['value']
                print sen_id
                datastream = data2['id']
                if sen_id not in OpenSense.feed_ids:
                    length = len(OpenSense.feed_ids)
                    OpenSense.feed_ids[length] = sen_id
                
#        transmit data to cosm
                packet = OpenSensePacket(SENSE_API_KEY, [(sen_id,value)])
                print "Sen.se status = ", packet.push()
                print datastream
                print value
        
def db_subscribe(level):
    while 1:
        topic_db = MQTT.topic_db + str(level)
        rdb =MQTT.database4.subscribe(topic_db,0)
        if rdb == NC.ERR_SUCCESS:
            ev = MQTT.database4.pop_event()
            if ev != None: 
                if ev.type == NC.CMD_PUBLISH:
                    payload = ev.msg.payload
                    if payload is not None and str(ev.msg.topic) == topic_db:
                        ev = None
                        break
                elif ev.type == 1000:
                    print "Network Error. Msg = ", ev.msg
            rdb = MQTT.database4.loop()
    return payload

def db_connect():
    while 1:
        MQTT.database4 = nyamuk.Nyamuk(MQTT.client_dbsense, None, None, MQTT.server)
        rdb = MQTT.database4.connect()
        if (rdb != NC.ERR_SUCCESS):
            print "Can't connect"
            time.sleep(30)
        else:
            print "Connection successful"
            return

def start_nyamuk(server, client_id, topic, username = None, password = None):
    while 1:
        ts = nyamuk.Nyamuk(client_id, username, password, server)
        rc = ts.connect()
        
        if rc != NC.ERR_SUCCESS:
            print "Can't connect"
            sys.exit(-1)
        
        rc = ts.subscribe(topic, 0)
        
        while rc == NC.ERR_SUCCESS:
            ev = ts.pop_event()
            if ev != None: 
                if ev.type == NC.CMD_PUBLISH:
                    handle_payload(ev)
                elif ev.type == 1000:
                    print "Network Error. Msg = ", ev.msg
                    break
            rc = ts.loop()
            if rc == NC.ERR_CONN_LOST:
                ts.logger.info("subscriber reconnecting in 30sec")
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
                if j != []:
                    mote_data = [str(j['id']),str(j['TS']),str(j['sen'])]
                    mote_list.append(mote_data)
    except ValueError:
        print "Error"   

if __name__ == '__main__':
    avg = []
    mote_list = []
    db_connect()
    mote_list = []
#    for i in range(1,5):
#        database_data = db_subscribe(i)
#        update(database_data)
    start_nyamuk(MQTT.server, MQTT.client_sense, MQTT.topic_temp)
            
