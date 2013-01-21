import sys
import time
from MQTT import MQTT

from nyamuk import nyamuk
from nyamuk import nyamuk_const as NC
import json
from clouding import ThingSpeakPacket
from ConfigParser import SafeConfigParser

def handle_payload(ev):
    msg = ev.msg
    
    parser = SafeConfigParser()
    parser.read('data.ini')
    section = parser.sections()
    
    
    print "\ttopic : " + msg.topic
    if msg.payload is not None:
        data = msg.payload
        data = str(data)
        print "payload = ", data
        try:
            data2 = json.loads(data)
        except ValueError:
            return
        
        for name in parser.items(section[0]):
            if name[0] == data2['id']:
                field_id = name[1][:7]
                api_key = name[1][8:]
                value = data2['value']
                datastream = data2['id']
                packet = ThingSpeakPacket(api_key, [(field_id, value)])
                print "Thingspeak status = ", packet.push()
                print field_id
                print datastream
                print value

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
    start_nyamuk(MQTT.server, MQTT.client_ts, MQTT.topic_temp)
    