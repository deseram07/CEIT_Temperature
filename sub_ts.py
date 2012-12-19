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
        database_data = db_subscribe()
        database_data = str(database_data)
        try:
            pilist = json.loads(database_data)
            pis = pilist["pi"]
            print pis
            for i in pis:
                if str(i['id']) == pi_id:
                    motes = i['mote']
                    break
        except ValueError:
            print "Error"
        
#        averaging code
        transmit = 0
        for i in motes:
            if str(data2['id']) not in id_list:
                id_list.append(str(data2['id']))
                avg.append([0,0.0])
            if data2['id'] == i['id']:
                field_id = i['TS']
                sen_id = i['sen']
                datastream = data2['id']
                c = 0
                for m in id_list:
                    if m == str(data2['id']):
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
                    c += 1
        
        if transmit:
#        transmit data to cosm
            packet = ThingSpeakPacket(THINGSPEAK_API_KEY, [(field_id, value)])
            print "Thingspeak status = ", packet.push()
            print field_id
            print datastream
            print value
            
def db_subscribe():
    while 1:
        topic_db = MQTT.topic_db + pi_id[0]
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
            
if __name__ == '__main__':
    pi_id = sys.argv[1]
    level = pi_id[0]
    db_connect()
    database_data = db_subscribe()
    avg = []
    id_list = []
    start_nyamuk(MQTT.server, MQTT.client_ts, MQTT.topic_temp)
    