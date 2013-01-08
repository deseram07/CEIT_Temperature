#########################################################################
#
# SwapManager
#
# Copyright (c) 2012 Daniel Berenguer <dberenguer@usapiens.com>
#
# This file is part of the lagarto project.
#
# lagarto  is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
#
# lagarto is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with panLoader; if not, write to the Free Software
# Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301
# USA
#
#########################################################################
__author__="Daniel Berenguer"
__date__  ="$Jan 23, 2012$"
#########################################################################

from swap.SwapInterface import SwapInterface
from swap.protocol.SwapDefs import SwapState
from swap.xmltools.XmlSettings import XmlSettings
from MQTT import MQTT

from nyamuk import nyamuk
import nyamuk.nyamuk_const as NC
import json

class SwapManager(SwapInterface):
    """
    SWAP Management Class
    """
    def newMoteDetected(self, mote):
        """
        New mote detected by SWAP server
        
        @param mote: Mote detected
        *****************************
        need to send shit to the server here
        """
        if self._print_swap == True:
            print "New mote with address " + str(mote.address) + " : " + mote.definition.product + \
            " (by " + mote.definition.manufacturer + ")"


    def newEndpointDetected(self, endpoint):
        """
        New endpoint detected by SWAP server
        
        @param endpoint: Endpoint detected
        """
        if self._print_swap == True:
            print "New endpoint with Reg ID = " + str(endpoint.getRegId()) + " : " + endpoint.name


    def moteStateChanged(self, mote):
        """
        Mote state changed
        
        @param mote: Mote having changed
        ******************************
        need to add shit here, needs to send shit to the server
        """
        if self._print_swap == True:
            print "Mote with address " + str(mote.address) + " switched to \"" + \
            SwapState.toString(mote.state) + "\""     


    def moteAddressChanged(self, mote):
        """
        Mote address changed
        
        @param mote: Mote having changed
        
        """
        if self._print_swap == True:
            print "Mote changed address to " + str(mote.address)


    def registerValueChanged(self, register):
        """
        Register value changed
        
        @param register: register object having changed
        **********************
        not sure what this does, think it returns the temperature
        """
        # Skip config registers
        if register.isConfig():
            return
        
        if self._print_swap == True:
            print  "Register addr= " + str(register.getAddress()) + " id=" + str(register.id) + " changed to " + register.value.toAsciiHex()
        
        status = []
        # For every endpoint contained in this register
        for endp in register.parameters:
            strval = endp.getValueInAscii()
            if endp.valueChanged:
                if self._print_swap:
                    if endp.unit is not None:
                        strval += " " + endp.unit.name
                    print endp.name + " in address " + str(endp.getRegAddress()) + " changed to " + strval
                               
                if endp.display:
                    endp_data = endp.dumps()
                    if endp_data is not None:
                        status.append(endp_data)
        
        if len(status) > 0:
#            publish data onto the server LIB/level4/climate_raw        
            data = json.dumps(status)
            L = len(data)
            data = data[1:L-1]
#            print data
#            if (endp.name == "Temperature" || endp.name == "Voltage"):
            print "updating list"
            self.db_connect()
            database_data = self.db_subscribe()
            database_data = str(database_data)
            
            try:
                pilist = json.loads(database_data)
                pis = pilist["pi"]
                for i in pis:
                    if str(i['id']) == MQTT.pi_id:
                        motes = i['mote']
                        for j in motes:
                            if str(j['id']) == str(endp.id):
                                print "****************\n****************\n****************\n****************\n"
                                if(MQTT.ny.publish(MQTT.topic_temp, data, retain = True) == 0,):
                                    print "published = " + data
                                else:
                                    print "publish failed"
                                MQTT.ny.loop()
            except ValueError:
                print "Error"
            
    def get_status(self, endpoints):
        """
        Return network status as a list of endpoints in JSON format
        Method required by LagartoServer
        
        @param endpoints: list of endpoints being queried
        
        @return list of endpoints in JSON format
        """
        status = []
        if endpoints is None:
            for mote in self.network.motes:
                for reg in mote.regular_registers:
                    for endp in reg.parameters:
                        status.append(endp.dumps())
        else:
            for item in endpoints:
                endp = self.get_endpoint(item["id"], item["location"], item["name"])
                if endp is not None:
                    status.append(endp.dumps()) 
        
        return status
            
  
    
    def stop(self):
        """
        Stop SWAP manager
        """
        # Stop SWAP server
        self.server.stop()

    def db_subscribe(self):
        while 1:
            topic_db = MQTT.topic_db + MQTT.pi_id[0]
            rdb =MQTT.database5.subscribe(topic_db,0)
            if rdb == NC.ERR_SUCCESS:
                ev = MQTT.database5.pop_event()
                if ev != None: 
                    if ev.type == NC.CMD_PUBLISH:
                        payload = ev.msg.payload
                        if payload is not None and str(ev.msg.topic) == topic_db:
                            ev = None
                            break
                    elif ev.type == 1000:
                        print "Network Error. Msg = ", ev.msg
                rdb = MQTT.database5.loop()
        return payload
    
    def db_connect(self):
        while 1:
            MQTT.database5 = nyamuk.Nyamuk(MQTT.client_dblib, None, None, MQTT.server)
            rdb = MQTT.database5.connect()
            if (rdb != NC.ERR_SUCCESS):
                print "Can't connect"
                time.sleep(30)
            else:
                print "Connection successful"
            return

    def __init__(self, swap_settings=None):
        """
        Class constructor
        
        @param swap_settings: path to the main SWAP configuration file
        @param verbose: Print out SWAP frames or not
        @param monitor: Print out network events or not
        """
        # MAin configuration file
        self.swap_settings = swap_settings
        # Print SWAP activity
        self._print_swap = False
        
        try:
            # Superclass call
            SwapInterface.__init__(self, swap_settings)
        except:
            raise

        # Lagarto server constructor
#        LagartoServer.__init__(self, working_dir)

        if XmlSettings.debug == 2:
            self._print_swap = True
