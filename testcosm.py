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

from twisted.internet import reactor
import txpachube
import txcurrentcost.monitor

# Paste your Pachube API key here
API_KEY = "c77vbSNSGy5y3386jMnQkrFK6vWSAKxGaTFQbHV3QjRrND0g"

# Paste the feed identifier you wish to be DELETED here
FEED_ID = "Test data"

CurrentCostMonitorConfigFile = "/path/to/your/config/file"


class MyCurrentCostMonitor(txcurrentcost.monitor.Monitor):
    """
    Extends the txcurrentCost.monitor.Monitor by implementing periodic update
    handler to call a supplied data handler.
    """

    def __init__(self, config_file, periodicUpdateDataHandler):
        super(MyCurrentCostMonitor, self).__init__(config_file)
        self.periodicUpdateDataHandler = periodicUpdateDataHandler

    def periodicUpdateReceived(self, timestamp, temperature, sensor_type, sensor_instance, sensor_data):
        if sensor_type == txcurrentcost.Sensors.ElectricitySensor:
            if sensor_instance == txcurrentcost.Sensors.WholeHouseSensorId:
                self.periodicUpdateDataHandler(timestamp, temperature, sensor_data)


class Monitor(object):

    def __init__(self, config):
        self.temperature_datastream_id = "temperature"
        self.energy_datastream_id = "energy"
        self.pachube = txpachube.client.Client(api_key=API_KEY, feed_id=FEED_ID)
        currentCostMonitorConfig = txcurrentcost.monitor.MonitorConfig(CurrentCostMonitorConfigFile)
        self.sensor = txcurrentcost.monior.Monitor(currentCostMonitorConfig,
                                                   self.handleCurrentCostPeriodicUpdateData)
        
    def start(self):
        """ Start sensor """
        self.sensor.start()
        
    def stop(self):
        """ Stop the sensor """
        self.sensor.stop()
        
    def def handleCurrentCostPeriodicUpdateData(self, timestamp, temperature, watts_on_channels):
        """ Handle latest sensor periodic update """

        # Populate a txpachube.Environment data structure object with latest data

        environment = txpachube.Environment(version="1.0.0")
        environment.setCurrentValue(self.temperature_datastream_id, "%.1f" % temperature)
        environment.setCurrentValue(self.energy_datastream_id, str(watts_on_channels[0]))

        # Update the Pachube service with latest value(s)

        d = self.pachube.update_feed(data=environment.encode())
        d.addCallback(lambda result: print "Pachube updated")
        d.addErrback(lambda reason: print "Pachube update failed: %s" % str(reason))


if __name__ == "__main__":
    monitor = Monitor()
    reactor.callWhenRunning(monitor.start)
    reactor.run()        
        