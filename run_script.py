from get_modbus import Get_Modbus_Data
from local_db import Influx_Database_class
import time

obj = Get_Modbus_Data()
obj.initialize_modbus()

db = Influx_Database_class(config_type="local")
remote_db = Influx_Database_class(config_type="remote")
i=0
while True:
	data = obj.get_data()
	db.push_json_to_db(data=data)
	time.sleep(1)

	if i%2 == 0 and i!=0:
		time_now = time.time()
		data = db.read_from_db(time_now=time_now)

		##### TODO: update_push_to_remote
		# data.index.name='time'
		# data = data.reset_index()
		# print("length before deletion = ",data)
		# remote_db.push_df_to_db(df=data)

		remote_data = remote_db.read_from_db(time_now=time_now)
		print("length after remote push = ",len(remote_data))

		# db.delete_from_db(time_now=time_now)
		data = db.read_from_db(time_now=time_now)
		print(len(data))
	i+=1
