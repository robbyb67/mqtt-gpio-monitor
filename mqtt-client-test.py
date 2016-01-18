#!/usr/bin/env python

__author__ = "Robert Bausdorf"
__copyright__ = "Copyright (C) Robert Bausdorf"

import logging
import os
import signal
import socket
import sys
import time
import ssl

import ConfigParser
import paho.mqtt.client as mqtt

# Script name (without extension) used for config/logfile names
APPNAME = os.path.splitext(os.path.basename(__file__))[0]
INIFILE = os.getenv('INIFILE', APPNAME + '.ini')
LOGFILE = os.getenv('LOGFILE', APPNAME + '.log')

# Read the config file
config = ConfigParser.RawConfigParser()
config.read(INIFILE)

# Use ConfigParser to pick out the settings
MODULE = config.get("global", "module")
DEBUG = config.getboolean("global", "debug")

MQTT_HOST = config.get("global", "mqtt_host")
MQTT_PORT = config.getint("global", "mqtt_port")
MQTT_USERNAME = config.get("global", "mqtt_username")
MQTT_PASSWORD = config.get("global", "mqtt_password")
MQTT_CLIENT_ID = config.get("global", "mqtt_client_id")
MQTT_TOPIC = config.get("global", "mqtt_topic")
MQTT_QOS = config.getint("global", "mqtt_qos")
MQTT_RETAIN = config.getboolean("global", "mqtt_retain")
MQTT_CLEAN_SESSION = config.getboolean("global", "mqtt_clean_session")
MQTT_LWT = config.get("global", "mqtt_lwt")
MQTT_SSL_CERT = config.get("global", "mqtt_ssl_cert")
MQTT_SSL_INSECURE = config.get("global", "mqtt_ssl_insecure")

# Initialise logging
LOGFORMAT = '%(asctime)-15s %(levelname)-5s %(message)s'

if DEBUG:
    logging.basicConfig(filename=LOGFILE,
                        level=logging.DEBUG,
                        format=LOGFORMAT)
else:
    logging.basicConfig(filename=LOGFILE,
                        level=logging.INFO,
                        format=LOGFORMAT)

logging.info("Starting " + APPNAME)
logging.info("INFO MODE")
logging.debug("DEBUG MODE")
logging.debug("INIFILE = %s" % INIFILE)
logging.debug("LOGFILE = %s" % LOGFILE)

MQTT_TOPIC_IN = MQTT_TOPIC + "/#"

mqttc = mqtt.Client(MQTT_CLIENT_ID, clean_session=MQTT_CLEAN_SESSION)

# MQTT callbacks
def on_connect(mosq, obj, result_code):
    """
    Handle connections (or failures) to the broker.
    This is called after the client has received a CONNACK message
    from the broker in response to calling connect().
    The parameter rc is an integer giving the return code:

    0: Success
    1: Refused . unacceptable protocol version
    2: Refused . identifier rejected
    3: Refused . server unavailable
    4: Refused . bad user name or password (MQTT v3.1 broker only)
    5: Refused . not authorised (MQTT v3.1 broker only)
    """
    if result_code == 0:
        logging.info("Connected to %s:%s" % (MQTT_HOST, MQTT_PORT))

        # Subscribe to our incoming topic
        logging.debug("Subscribe to %s, QOS %d" % (MQTT_TOPIC_IN, MQTT_QOS))
        mqttc.subscribe(MQTT_TOPIC_IN, qos=MQTT_QOS)
        
        # Publish retained LWT as per http://stackoverflow.com/questions/19057835/how-to-find-connected-mqtt-client-details/19071979#19071979
        # See also the will_set function in connect() below
        mqttc.publish(MQTT_LWT, "1", qos=0, retain=True)

    elif result_code == 1:
        logging.info("Connection refused - unacceptable protocol version")
    elif result_code == 2:
        logging.info("Connection refused - identifier rejected")
    elif result_code == 3:
        logging.info("Connection refused - server unavailable")
    elif result_code == 4:
        logging.info("Connection refused - bad user name or password")
    elif result_code == 5:
        logging.info("Connection refused - not authorised")
    else:
        logging.warning("Connection failed - result code %d" % (result_code))

def on_disconnect(mosq, obj, result_code):
    """
    Handle disconnections from the broker
    """
    if result_code == 0:
        logging.info("Clean disconnection from broker")
    else:
        logging.info("Broker connection lost. Retrying in 5s...")
        time.sleep(5)

def on_message(mosq, obj, msg):
    """
    Handle incoming messages
    """
    logging.debug("Incoming message for topic %s: %s" % (msg.topic, msg.payload))
# End of MQTT callbacks


def cleanup(signum, frame):
    """
    Signal handler to ensure we disconnect cleanly
    in the event of a SIGTERM or SIGINT.
    """
    # Publish our LWT and cleanup the MQTT connection
    logging.info("Disconnecting from broker...")
    mqttc.publish(MQTT_LWT, "0", qos=0, retain=True)
    mqttc.disconnect()
    mqttc.loop_stop()

    # Exit from our application
    logging.info("Exiting on signal %d" % (signum))
    sys.exit(signum)

def connect():
    """
    Connect to the broker, define the callbacks, and subscribe
    This will also set the Last Will and Testament (LWT)
    The LWT will be published in the event of an unclean or
    unexpected disconnection.
    """
    # Add the callbacks
    mqttc.on_connect = on_connect
    mqttc.on_disconnect = on_disconnect
    mqttc.on_message = on_message

    # Set the login details
    if MQTT_USERNAME:
        mqttc.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)

    if MQTT_SSL_CERT:
        logging.debug("Use ca_certs file: %s" %(MQTT_SSL_CERT))
        mqttc.tls_set(MQTT_SSL_CERT, None, None, ssl.CERT_REQUIRED, ssl.PROTOCOL_SSLv23, ciphers=None)
        logging.debug("Use insecure SSL: %s" % (MQTT_SSL_INSECURE))
        if MQTT_SSL_INSECURE == 'True':
            mqttc.tls_insecure_set(True)
        else:
            mqttc.tls_insecure_set(False)

    # Set the Last Will and Testament (LWT) *before* connecting
    mqttc.will_set(MQTT_LWT, payload="0", qos=0, retain=True)

    # Attempt to connect
    count = 1
    while True:
        logging.debug("Connecting (try %d) to %s:%d..." % (count, MQTT_HOST, MQTT_PORT))
        try:
            mqttc.connect(MQTT_HOST, MQTT_PORT, 60)
            break
        except Exception, e:
            logging.error("Error connecting to %s:%d: %s" % (MQTT_HOST, MQTT_PORT, str(e)))
            count += 1
            time.sleep(3)
            if count > 3:
                sys.exit(2)
    
    # Let the connection run forever
    mqttc.loop_start()

def poll():
    """
    The main loop in which we monitor the state of the PINs
    and publish any changes.
    """
    while True:
        time.sleep(10000);

connect()
poll()

