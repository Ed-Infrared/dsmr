#!/usr/bin/python3

from configparser import ConfigParser
import serial
import sqlalchemy as db
import sys


class Dsmr:
    def __init__(self):
        config_object = ConfigParser()
        config_object.read('config.ini')
        db_info = config_object['Database']
        self.db_url = f'mysql://{db_info["login"]}:{db_info["password"]}@{db_info["ip"]}/{db_info["database"]}'
        self.engine = db.create_engine(self.db_url)
        self.connection = self.engine.connect()
        self.metadata = db.MetaData()

        self.telegram = ''
        self.timestamp = ''
        self.power_low = ''
        self.power_high = ''
        self.gas = ''

    def read_telegram(self):
        # set up serial port
        ser = serial.Serial()
        ser.baudrate = 115200
        ser.bytesize = serial.EIGHTBITS
        ser.parity = serial.PARITY_NONE
        ser.stopbits = serial.STOPBITS_ONE
        ser.xonxoff = 1
        ser.rtscts = 0
        ser.timeout = 12
        ser.port = "/dev/ttyUSB0"

        # open serial port
        try:
            ser.open()
        except serial.SerialException as e:
            print('Error:', e)
            sys.exit(1)

        # find begin of next telegram
        ser.flushInput()
        while '!' not in str(ser.readline()):
            pass

        # read telegram
        self.telegram = ''
        while '!' not in self.telegram:
            self.telegram += ser.readline().decode('ascii')

        # end serial communication
        ser.close()

    def decode_telegram(self):
        telegram = self.telegram.split('\n')
        for line in telegram:
            if '0-0:1.0.0' in line:
                line = line.split('(')[1]
                self.timestamp = f'20{line[:2]}-{line[2:4]}-{line[4:6]} {line[6:8]}:{line[8:10]}:{line[10:12]}'
            if '1-0:1.8.1' in line:
                self.power_low = float(line.split('(')[1].split('*')[0])
            if '1-0:1.8.2' in line:
                self.power_high = float(line.split('(')[1].split('*')[0])
            if '0-1:24.2.1' in line:
                self.gas = float(line.split('(')[2].split('*')[0])

    def store_in_db(self):
        meterreadings = db.Table('meterreadings', self.metadata, autoload=True, autoload_with=self.engine)
        query = meterreadings.insert().values(datestamp=self.timestamp,
                                              power_low=self.power_low,
                                              power_high=self.power_high,
                                              gas=self.gas)
        self.connection.execute(query)


def main():
    dsmr = Dsmr()
    dsmr.read_telegram()
    dsmr.decode_telegram()
    dsmr.store_in_db()


if __name__ == '__main__':
    main()
