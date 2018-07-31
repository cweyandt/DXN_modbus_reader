import pymodbus
import serial
from pymodbus.pdu import ModbusRequest
from pymodbus.constants import Endian
from pymodbus.client.sync import ModbusSerialClient as ModbusClient #initialize a serial RTU client instance
from pymodbus.transaction import ModbusRtuFramer

import logging
logging.basicConfig()
log = logging.getLogger()
log.setLevel(logging.DEBUG)

client= ModbusClient(method = "rtu", port="/dev/ttyUSB0",stopbits = 1, bytesize = 8, parity = 'E', baudrate= 9600)
connection = client.connect()
print connection

result= client.read_holding_registers(0x0552,1,unit= 0xF7)
print("result = ", result.registers[0]/10.0)
client.close()