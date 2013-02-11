################################################################
__author__="Buddhika De Seram"
__date__="04/02/2012"
################################################################

#This program receives the raw temperature data from MQTT and
#posts the data up on cosm

import sys
import time
from MQTT import MQTT

from nyamuk import nyamuk
from nyamuk import nyamuk_const as NC
import json
from clouding import PachubePacket
from ConfigParser import SafeConfigParser
import random

def handle_payload(ev):
    msg = ev.msg
    print "\ttopic : " + msg.topic
    
    parser = SafeConfigParser()
    parser.read('data.ini')
    
    if msg.payload is not None:
        data = msg.payload
        data = str(data)
     
        print "payload = ", data
        try:
            data2 = json.loads(data)
        except ValueError:
            return

        value = data2['value']
        datastream = data2['id']
#        transmit data to cosm
        api = parser.get('API', 'cosm')
        feed = parser.get('API', 'cosm_feed')
        packet = PachubePacket(api, feed, [(datastream, value)])
        print "Cosm status = ", packet.push()
        print datastream
        print value
    

def start_nyamuk(server, client_id, topic, username = None, password = None):
    while 1:
        cosm = nyamuk.Nyamuk(client_id, username, password, server)
        rc = cosm.connect()
        
        if rc != NC.ERR_SUCCESS:
            print "Can't connect"
            sys.exit(-1)
        
        rc = cosm.subscribe(topic, 0)
        
        while rc == NC.ERR_SUCCESS:
            ev = cosm.pop_event()
            if ev != None: 
                if ev.type == NC.CMD_PUBLISH:
                    handle_payload(ev)
                elif ev.type == 1000:
                    print "Network Error. Msg = ", ev.msg
                    break
            rc = cosm.loop()
            if rc == NC.ERR_CONN_LOST:
                cosm.logger.info("subscriber reconnecting in 30sec")
                time.sleep(30)
                break
       
if __name__ == '__main__':
    start_nyamuk(MQTT.server, (MQTT.client_cosm + str(random.randint(1000, 10000))), MQTT.topic_temp)
    