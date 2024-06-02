# General stuff
import os, sys, time, re, json
from urllib.parse import quote_plus
import configparser
from dotenv import load_dotenv
import logging, traceback
import logging.handlers # For RotatingFileHandle
# Specifics
import rtmidi
import psycopg2

# Process config.ini
config_file_location = "." + os.sep + "config" + os.sep + "config.ini"
config = configparser.ConfigParser()
config.read(config_file_location)
# We parse a few main arguments that we use often, for convenience
listen_interval_ms = int(config["capture"]["listen_interval_ms"])
buffer_interval_s = int(config["capture"]["buffer_interval_s"])
db_table = config.get("database", "dest_table", fallback="midi_drums_raw")

load_dotenv()

# Logging setup
logger = logging.getLogger() # Root logger
log_file_name = "logs" + os.sep + "rtmidi_logging_to_database.log"
log_formatter = logging.Formatter("%(asctime)s [%(threadName)s] [%(levelname)s]  %(message)s")
log_file_handler = logging.handlers.RotatingFileHandler(log_file_name, maxBytes=1000000, backupCount=5)
log_console_handler = logging.StreamHandler(sys.stdout)
log_file_handler.setFormatter(log_formatter)
log_console_handler.setFormatter(log_formatter)
logger.setLevel(logging.INFO)
logger.addHandler(log_file_handler)
logger.addHandler(log_console_handler)

db_connection_dict = {
    "user": os.environ["POSTGRES_USER"] or 'rtmltd',
    "password": quote_plus(os.environ["POSTGRES_PASSWORD"] or ''),
    "host": os.environ["POSTGRES_HOST"] or 'localhost',
    "port": os.environ["POSTGRES_PORT"] or '5432',
    "dbname": os.environ["POSTGRES_DB"] or 'rtmltd'
}

try:
    print(f"Connecting to Postgres database at { db_connection_dict['host'] }/{ db_connection_dict['dbname'] }")
    global db_connection
    db_connection = psycopg2.connect(**db_connection_dict)
    db_connection.autocommit = True
except Exception as e:
    # Maybe the quote_plus bit was not a good idea. But maybe it's sometimes necessary. In Docker, it didn't work, so let's remove it in this specific case and try again.
    try:
        db_connection_dict["password"] = os.environ["POSTGRES_PASSWORD"]
        print(f"Second try with differently encoded password, testing connection to Postgres database at { db_connection_dict['host'] }/{ db_connection_dict['dbname'] }")
        db_connection = psycopg2.connect(**db_connection_dict)
        db_connection.autocommit = True
        print("Connected successfully.")
    except Exception as e:
        raise Exception(f"Failed to connect to the TabMove database during the step where we create the database engine.\n\t{e}\n\t{traceback.format_exc()}")


# rtmidi setup
midiin = rtmidi.RtMidiIn()

def parse_midi_message(midi):
    
    if midi.isNoteOn():
        return { "timestamp": time.time(), "midi-data-type": "note on", "value": midi.getMidiNoteName(midi.getNoteNumber()), "velocity": midi.getVelocity() }
    elif midi.isNoteOff():
        return { "timestamp": time.time(), "midi-data-type": "note off", "value": midi.getMidiNoteName(midi.getNoteNumber()) }
    elif midi.isController():
        return { "timestamp": time.time(), "midi-data-type": "controller", "number": midi.getControllerNumber(), "value": midi.getControllerValue() }

ports = range(midiin.getPortCount())
logging.info("Checking rtmidi available ports")
if ports:
    for i in ports:
        logging.info("Port {}: {}".format(i, midiin.getPortName(i)))
    matching_device_port_number = [port for port in ports if midiin.getPortName(port) == config["capture"]["listen_midi_device"]][0]
    logging.info("Opening port number: {}".format(matching_device_port_number))
    midiin.openPort(matching_device_port_number)

    midi_buffer = []
    midi_silence_ms = 0

    # Start listening and logging to database when the buffer time interval is hit
    while True:
        m = midiin.getMessage(listen_interval_ms) # Some timeout in ms. Original was 250ms, we provide a value from our config file.
        if m:
            logging.info(parse_midi_message(m))
            midi_buffer.append(parse_midi_message(m))
            # Reset silence, too
            midi_silence_ms = 0
        else: # No note played means we count silence...
            midi_silence_ms += listen_interval_ms
            # logging.info("No input for {}ms...".format(midi_silence_ms))
            # Empty buffer if we've hit a long enough silence
            if midi_silence_ms / 1000 >= buffer_interval_s:
                logging.debug("Silence long enough ({}s) to attempt logging, if we heard anything!".format(buffer_interval_s))
                # Reset counters first
                midi_silence_ms = 0
                if len(midi_buffer) > 0:
                    # Log data!
                    logging.info("Logging {} notes to database".format(len(midi_buffer)))
                    try:
                        cursor = db_connection.cursor()
                        # Concatenate the buffer into a long string we'll insert. Hopefully, the database accepts this and won't run into length problems?
                        midi_buffer_for_insert = "('" + "'), ('".join(map(json.dumps, midi_buffer)) + "');"
                        cursor.execute(f"INSERT INTO { db_table } (v) VALUES { midi_buffer_for_insert }")
                        cursor.close()
                        # Clear the buffer after it was written
                        midi_buffer = []
                    except Exception as e:
                        logging.error("Failed to write data to database. Keeping data in buffer.")
                        logging.error(e)
                else:
                    # We did not hear anything
                    logging.debug("No data recorded, resetting counters.")
else:
    logging.error("NO MIDI INPUT PORTS!")