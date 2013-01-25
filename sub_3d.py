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
        
        datastream = data2['id']
        value = data2['value']
        print datastream
        print value
        MQTT.packet['id'] = datastream
        MQTT.packet['value'] = str(value)
        data = json.dumps(MQTT.packet)
        start = time.time()
        print data
        connect()
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
        MQTT.threed_pub = nyamuk.Nyamuk("aadfvb23456", None, None, MQTT.server)
        rcpub = MQTT.threed_pub.connect()
        if (rcpub != NC.ERR_SUCCESS):
            print "Can't connect"
            time.sleep(30)
        else:
            print "Publish Connection successful"
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
    start_nyamuk(MQTT.server, "fhgfhgf", MQTT.topic_temp)
    
    