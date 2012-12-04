'''
Nyamuk subscriber example
Copyright Iwan Budi Kusnanto
'''
import sys
import logging
from MQTT import MQTT

from nyamuk import nyamuk
from nyamuk import nyamuk_const as NC
from nyamuk import event
import json
from pprint import pprint
from clouding import PachubePacket, ThingSpeakPacket
import time

COSM_API_KEY = "48c-kZyDpkvAjKN4dFB73ANSnQOSAKxiL20xUUY5Y3pPST0g"
THINGSPEAK_API_KEY = "L2B57DZ1PLNXDWTG"
COSM_FEED_ID = "88817"
#DATASTREAM = []
#NO_ID = 0

def handle_publish(ev):
    msg = ev.msg
    c = 0
    print "\ttopic : " + msg.topic
    if msg.payload is not None:
        data = msg.payload
        data = str(data)
        data2 = json.loads(data)
        for i in node_list:
            if data2['id'] == str(i[:7]):
                field_id = str(i[8:(len(i)-1)])
                datastream = data2['id']
                break
            else:
                c += 1
            if c == len(node_list):
                print "Add new node onto node list"
                sys.exit(-1)
        value = str(data2['value'])
#        transmit data to cosm
        packet = PachubePacket(COSM_API_KEY, COSM_FEED_ID, [(datastream, value)])
        print "Cosm status = ", packet.push()
        
#        transmit data to thingspeak
        packet = ThingSpeakPacket(THINGSPEAK_API_KEY, [(field_id, value)])
        print "Thingspeak status = ", packet.push()
        print field_id
        print datastream
        print value
        
def start_nyamuk(server, client_id, topic, username = None, password = None):
    MQTT.ny = nyamuk.Nyamuk(client_id, username, password, server)
    rc = MQTT.ny.connect()
    if rc != NC.ERR_SUCCESS:
        print "Can't connect"
        sys.exit(-1)
    
    rc = MQTT.ny.subscribe(topic, 0)
    
    while rc == NC.ERR_SUCCESS:
        ev = MQTT.ny.pop_event()
        if ev != None: 
            if ev.type == NC.CMD_PUBLISH:
                handle_publish(ev)
            elif ev.type == 1000:
                print "NConnectionetwork Error. Msg = ", ev.msg
                sys.exit(-1)
        rc = MQTT.ny.loop()
        if rc == NC.ERR_CONN_LOST:
            MQTT.ny.logger.fatal("Connection to server closed")
            
    MQTT.ny.logger.info("subscriber exited")
    
if __name__ == '__main__':
    filename = sys.argv[1]
    server = MQTT.server
    client_id = MQTT.client_sub
    topic = MQTT.topic_temp
    node = open(filename, "r")
    print "Name of the file: ", node.name
    node_list = node.readlines()
#    print test_list[0][:7]
    start_nyamuk(server, client_id, topic)
    
    
