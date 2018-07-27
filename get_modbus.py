
# coding: utf-8

# In[61]:


#!/usr/bin/env python
'''
Pymodbus Synchrnonous Client Test with Dynasonic DXN Energy Meter
--------------------------------------------------------------------------

The following is an example of how to use the synchronous modbus client
implementation from pymodbus. This has been adapted from a sample script 
at https://pythonhosted.org/pymodbus/examples/synchronous-client.html

_Additional Note from sample script:
It should be noted that the client can also be used with
the guard construct that is available in python 2.5 and up::

    with ModbusClient('127.0.0.1') as client:
        result = client.read_coils(1,10)
        print result
        
           
***Created 2018-07-22 by Chris Weyandt        
'''

#---------------------------------------------------------------------------# 
# import the required server implementation
#---------------------------------------------------------------------------# 
from pymodbus.client.sync import ModbusTcpClient as ModbusClient
#from pymodbus.client.sync import ModbusUdpClient as ModbusClient
#from pymodbus.client.sync import ModbusSerialClient as ModbusClient

#additional imports for conversions
from pymodbus.constants import Endian
from pymodbus.payload import BinaryPayloadDecoder
import configparser

#---------------------------------------------------------------------------# 
# configure the client logging
#---------------------------------------------------------------------------# 
# import logging
# logging.basicConfig()
# log = logging.getLogger()
# log.setLevel(logging.INFO)


class Get_Modbus_Data(object):
	def __init__(self):
		Config = configparser.ConfigParser()
		Config.read("config.ini")

		try:
			modbus_config = Config["modbus"]
			self.IP_ADDRESS = modbus_config.get("ip")
			self.START_REGISTER = modbus_config.getint("BASE_ADDRESS")
			self.NUM_REGISTERS = modbus_config.getint("NUM_REGISTERS")
			self.UNIT_ID = modbus_config.getint("UNIT_ID")
			self.names = modbus_config.get("names").split(',')
			self.start_offsets = modbus_config.get("start_offsets").split(',')
			self.lengths = modbus_config.get("lengths").split(',')
			# if 
		except Exception as e:
			# self.logger.error("unexpected error while setting configuration from config_file=%s, error=%s"%(self.config_file, str(e)))
			raise e

	def initialize_modbus(self):
		self.client = ModbusClient(self.IP_ADDRESS)

	def get_data(self):
		output = {}
		for i in range(len(self.names)):
			rr = self.client.read_holding_registers(
				self.START_REGISTER+int(self.start_offsets[i])-1, 
				int(self.lengths[i]), 
				unit=self.UNIT_ID)
			dec = BinaryPayloadDecoder.fromRegisters(
				rr.registers,
				byteorder=Endian.Big,
				wordorder=Endian.Little)
			output[self.names[i]] = dec.decode_32bit_float()
		# print(output)
		return output

	def kill_modbus(self):
		self.client.close()