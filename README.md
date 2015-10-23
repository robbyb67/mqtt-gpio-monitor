mqtt-gpio-monitor
=================

Python script for sending/receiving commands to/from GPIO pins and values from
the ADC chip MCP3004 via MQTT messages.

This was written for use on a RaspberryPi, with either the PiFace extension board, or just raw access to the GPIO pins. 

The example INI file contains the only configuration required. You must define the module to use, either GPIO or PFIO. Depending on which you will need to ensure the appropriate Python module is installed.

If using the PiFace extension board you will need to follow the instructions [here](http://piface.github.io/pifacedigitalio/installation.html) to install the digital IO libraries;

    sudo apt-get install python-pifacedigitalio

If just using the raw GPIO pins then the RPi.GPIO module should be installed as part of Raspbian.

You will also need an MQTT client, in this case [Paho](https://pypi.python.org/pypi/paho-mqtt/0.9) 
and the [Spidev](https://pypi.python.org/pypi/spidev) package for MCP3004 ADC on 
[LinkerKit Baseboard](http://www.elv.de/linker-kit-baseboard-fuer-raspberry-pi.html). To install;

    sudo apt-get install python-dev
    sudo pip install paho-mqtt
    sudo pip install spidev

You should now be ready to run the script. It will listen for incoming messages on {topic}/in/+ (where {topic} is specified in the INI file). The incoming messages need to arrive on {topic}/in/{pin} with a value of either 1 or 0. 

E.g. a message arriving on {topic}/in/3 with value 1 will set pin 3 to HIGH. 

Depending on what is set for MONITOR_PINS in the INI file, the script will also monitor these pins and if there are any changes publish a message on {topic}/out/{pin} with a value of either 1 or 0.

E.g. if pin 7 changes from 1 to 0 a message would be published to {topic}/out/7 with a value of 1.

So you are able to both monitor pins as well as set pins HIGH/LOW. Obviously you can't do both monitor and update for the same pin.

If you use the LinkerKit Baseboard you will have to set GPIO_MODE to "bcm", otherwise
 "board" if you use the GPIO pins directly on the Raspi board.

The last option in the INI file is MONITOR_REFRESH, which is a special topic the script will subscribe to if specified. Any message arriving on that topic will trigger the script to re-send publishes with the current state of all monitored pins. This is useful for requesting the current state of all pins if the calling system is restarted for example.

To start the script by the init system ensure alls files are within _/root/mqtt-gpio-monitor/_, then copy or link 

    sudo ln -s /root/mqtt-gpio-monitor/mqtt-gpio-monitor /etc/init.d/mqtt-gpio-mon

and update the system start scripts

    sudo update-rc.d mqtt-gpio-mon defaults



