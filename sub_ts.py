import sys
import time
from MQTT import MQTT

from nyamuk import nyamuk
from nyamuk import nyamuk_const as NC
import json
from clouding import ThingSpeakPacket

THINGSPEAK_API_KEY = "L2B57DZ1PLNXDWTG"

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
        mote_list = []
        for i in range(1,5):
            database_data = db_subscribe(i)
            update(database_data, mote_list)
        
        print "mote_list", mote_list

        for i in mote_list:
            if data2['id'] == i[0]:
                field_id = i[1]
                value = data2['value']
                datastream = data2['id']
                packet = ThingSpeakPacket(THINGSPEAK_API_KEY, [(field_id, value)])
                print "Thingspeak status = ", packet.push()
                print field_id
                print datastream
                print value
            
def db_subscribe(level):
    while 1:
        topic_db = MQTT.topic_db + str(level)
        rdb =MQTT.database2.subscribe(topic_db,0)
        if rdb == NC.ERR_SUCCESS:
            ev = MQTT.database2.pop_event()
            if ev != None: 
                if ev.type == NC.CMD_PUBLISH:
                    payload = ev.msg.payload
                    if payload is not None and str(ev.msg.topic) == topic_db:
                        ev = None
                        break
                elif ev.type == 1000:
                    print "Network Error. Msg = ", ev.msg
            rdb = MQTT.database2.loop()
    return payload

def db_connect():
    while 1:
        MQTT.database2 = nyamuk.Nyamuk(MQTT.client_dbts, None, None, MQTT.server)
        rdb = MQTT.database2.connect()
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
                mote_data = [str(j['id']),str(j['TS']),str(j['sen'])]
                mote_list.append(mote_data)
    except ValueError:
        print "Error"
                    
if __name__ == '__main__':
    start_nyamuk(MQTT.server, MQTT.client_ts, MQTT.topic_temp)
    