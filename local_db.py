import logging
from logging.handlers import TimedRotatingFileHandler
import os
import json
import configparser
import datetime
from influxdb import InfluxDBClient
from Influx_Dataframe_Client import Influx_Dataframe_Client
import time

# @author : Marco Pritoni <mpritoni@lbl.gov>
# @author : Anand Prakash <akprakash@lbl.gov>
 

class Influx_Database_class(object):
    """
    This class saves the data from pandas dataframe to a local db (currently sqlite3 on disk) as buffer while pushing data
    to another DB/API.
    """
    
    def __init__(self, config_type="local"):

        Config = configparser.ConfigParser()
        Config.read("config.ini")

        try:
            self.ip = Config.get("modbus", "ip")
            if config_type == "local":
                db_config = Config["local_database_config"]
            else:
                db_config = Config["remote_database_config"]

            self.host = db_config.get("host")
            self.port = db_config.get("port")
            self.database_name = db_config.get("database")
            # self.username = db_config.get("username")
            # self.password = db_config.get("password")
            # self.client = InfluxDBClient(host=self.host,
            #                             port=self.port,
            #                             username=self.username, 
            #                             password=self.password,
            #                             database=self.database_name)
            self.influx_obj=Influx_Dataframe_Client(config_file="config.ini", db_section="local_database_config")
            self.client=self.influx_obj.expose_influx_client()
            self.measurement_name = db_config.get("measurement_name")
            self.sensor_id = db_config.get("sensor_id")
            self.tag_names = db_config.get("tags").split(",")
            self.tag_values = [db_config.get(self.tag_names[i]) for i in range(len(self.tag_names))]
            self.field_names = db_config.get("fields").split(",")
        except Exception as e:
            # self.logger.error("unexpected error while setting configuration from config_file=%s, error=%s"%(self.config_file, str(e)))
            raise e

    def push_json_to_db(self, data): 
        tags = {}
        for i in range(len(self.tag_names)):
            tags[self.tag_names[i]] = self.tag_values[i]

        fields = data
        pushData = [
                    {
                        "measurement": self.measurement_name,
                        "tags": tags,
                        "fields": fields
                    }
                ]
        self.influx_obj.t(json=pushData, database=self.database_name)
        # self.client.write_points(pushData)

    def push_df_to_db(self, df):
        self.influx_obj.write_data(data=df,
            tags=self.tag_names,
            fields=self.field_names,
            measurement=self.measurement_name,
            database=self.database_name)

    def read_from_db(self, time_now=None):
        if time_now == None:
            time_now=time.time()*1000000000

        return self.influx_obj.specific_query(database=self.database_name,
                measurement=self.measurement_name,
                end_time=int(time.time()*1000000000)
            )

    def delete_from_db(self, time_now=None):
        if time_now == None:
            time_now=time.time()*1000000000

        self.influx_obj.delete_based_on_time(database=self.database_name,
                measurement=self.measurement_name,
                end_time=int(time_now*1000000000)
            )