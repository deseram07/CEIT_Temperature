#########################################################################
__author__="Buddhika De Seram"
__date__  ="$Dec 02, 2012$"
__appname__= "lib_temp"
__version__= "0.1"
#########################################################################

from swap.SwapException import SwapException
from swapmanager import SwapManager

import sys
import os
import time
import signal
from MQTT import MQTT

from nyamuk import nyamuk
import nyamuk.nyamuk_const as NC
from nyamuk import event


def signal_handler(signal, frame):
    """
    Handle signal received
    """
    swap_manager.stop()
    sys.exit(0)

if __name__ == '__main__':
    
    settings = os.path.join(os.path.dirname(sys.argv[0]), "config", "settings.xml")

    # Catch possible SIGINT signals
    signal.signal(signal.SIGINT, signal_handler)
    
    # if no sigint
    MQTT.ny = nyamuk.Nyamuk(MQTT.client, server = MQTT.server)
    rc = MQTT.ny.connect()
    if rc != NC.ERR_SUCCESS:
        print "Can't connect"
        sys.exit(-1)
#    index = 0
    print "Connected to MQTT"
    try:      
        # SWAP manager
        swap_manager = SwapManager(settings)     
    except SwapException as ex:
        ex.display()
        ex.log()

    signal.pause()
