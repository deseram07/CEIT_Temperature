#!/usr/bin/env python

# This example demonstrates how you could use the txpachube module to
# help upload sensor data (in this scenario a CurrentCost device) to
# Pachube.
# A txpachube.Environment data structure is generated and populated
# with current value data. All the implemented data structures
# support encoding to JSON (default) and XML (EEML).
#
# In this example the CurrentCost sensor object is derived from the
# separate txcurrentcost package. If you want to run this script
# you would need to obtain that package.
#
from __future__ import print_function
from twisted.internet import reactor
import txpachube
import txpachube.client

# Paste your Pachube API key here
API_KEY = "c77vbSNSGy5y3386jMnQkrFK6vWSAKxGaTFQbHV3QjRrND0g"
  
# Paste the feed identifier you wish to be DELETED here
FEED_ID = "TEST1"



class Monitor(object):

    def __init__(self):
        self.temperature_datastream_id = "10.12.0"
        self.energy_datastream_id = "11.12.0"
        self.pachube = txpachube.client.Client(api_key=API_KEY, feed_id=FEED_ID)
        self.start = self.handleCurrentCostPeriodicUpdateData('13:23:06', 10.4, 15)
       
    def start(self):
        """ Start sensor """
        self.sensor.start()
        
    def stop(self):
        """ Stop the sensor """
        self.sensor.stop()
        
    def handleCurrentCostPeriodicUpdateData(self, timestamp, temperature, watts_on_channels):
        """ Handle latest sensor periodic update """

        # Populate a txpachube.Environment data structure object with latest data

        environment = txpachube.Environment(title = "TEST1", version="1.0.0")
        environment.setCurrentValue(self.temperature_datastream_id, "%.1f" % temperature)
        environment.setCurrentValue(self.energy_datastream_id, str(watts_on_channels))

        # Update the Pachube service with latest value(s)
        d = self.pachube.update_feed(api_key=API_KEY,feed_id=FEED_ID, data=environment.encode())
        d.addCallback(lambda result:print("Pachube updated"))
        d.addErrback(lambda reason: print("Pachube update failed: %s" % str(reason)))
if __name__ == "__main__":
    monitor = Monitor()
    reactor.callWhenRunning(monitor.start)
    reactor.run()        