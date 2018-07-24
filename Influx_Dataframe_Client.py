import configparser
import pandas as pd
import numpy as np
from influxdb import InfluxDBClient
from influxdb import DataFrameClient

'''
When making queries, identifiers may be put into Double quotes depending on the
characters they contain. String literals i.e. tag values must be in single quotes!

class influxdb.InfluxDBClient(host=u'localhost',
 port=8086, username=u'root', password=u'root',
 database=None, ssl=False, verify_ssl=False,
 timeout=None, retries=3, use_udp=False,
 udp_port=4444, proxies=None)

class influxdb.DataFrameClient(host=u'localhost',
 port=8086, username=u'root', password=u'root',
 database=None, ssl=False, verify_ssl=False,
 timeout=None, retries=3, use_udp=False,
 udp_port=4444, proxies=None)

Take dataframe like the following example:
time                AP_count ap_name             building_number floor room
----                -------- -------             --------------- ----- ----
1459494000000000000 1        ap135-100-103d-r177 100             1     03d
1459494000000000000 1        ap135-100-149b-r177 100             1     49b
1459494000000000000 5        ap135-100-140-r177  100             1     140
1459494000000000000 6        ap135-100-139-r177  100             1     139
1459494000000000000 3        ap135-100-121-r177  100             1     121

Where AP_count is a field, ap_name, building_number, floor, and room are all tags
time is the timestamp associated with each row

Each columnn in dataframe grab column name first and parse name
add all points as a list of dictionaries of json
Each column of the dataframe is turned into a json dictionary and added to a list
of json dictionaries which can be given to InfluxDBClient

i.e.


            pushData = [
                    {
                        "measurement": measurement,
                        "tags": {
                            tags: current_ap_name,
                            "building_number": 90,
                            "floor": 2,
                            "room": 50
                        },
                        "time": pushTime,
                        "fields": {
                            fields: ap_value
                        }
                    },
                    {
                        "measurement": measurement,
                        "tags": {
                            tags: current_ap_name,
                            "building_number": 90,
                            "floor": 2,
                            "room": 50
                        },
                        "time": pushTime,
                        "fields": {
                            fields: ap_value
                        }
                    }
                ]

'''
'''
def transform_to_dict(s, tags):
    #print(s)
    dic = {}
    for tag in tags:
        dic[tag] = s[tag]
    #print(dic)
    return dic
'''

def transform_to_dict(s, key):
    dic = {}
    for tag in key: # List of keys wanting to turn into dictionary
        dic[tag] = s
    return dic




class Influx_Dataframe_Client(object):
    #Connection details
    host = ""
    port = ""
    username = ""
    password = ""
    database = ""
    ssl= ""
    verify_ssl = ""
    #clients for influxDB both DataFrameClient and the InfluxDBClient
    client = None
    df_client = None
    data = None


    def __init__(self, config_file, db_section=None):
        '''
        Constructor reads credentials from config file and establishes a connection
        '''
        # read from config file
        Config = configparser.ConfigParser()
        Config.read(config_file)
        if db_section != None:
            self.db_config = Config[db_section]
        else:
            self.db_config = Config["DB_config"]

        self.host = self.db_config.get("host")
        self.username = self.db_config.get("username")
        self.password = self.db_config.get("password")
        self.database = self.db_config.get("database")
        self.protocol = self.db_config.get("protocol")
        self.port = self.db_config.get("port")
        self.use_ssl = self.db_config.get("use_ssl")
        self.verify_ssl = self.db_config.get("verify_ssl")
        self.make_client()


    def make_client(self):
    # setup client both InfluxDBClient and DataFrameClient
    # DataFrameClient is for queries and InfluxDBClient is for writes
        self.client = InfluxDBClient(host=self.host, port=self.port, 
                    username=self.username, password=self.password, 
                    database=self.database,ssl=self.use_ssl, verify_ssl=self.verify_ssl)
        self.df_client = DataFrameClient(host=self.host, port=self.port, 
                    username=self.username, password=self.password, 
                    database=self.database,ssl=self.use_ssl, verify_ssl=self.verify_ssl)

    def expose_influx_client(self):
        #Expose InfluxDBClient to user so they utilize all functions of InfluxDBClient
        return self.client

    def expose_data_client(self):
        #Expose DataFrameClient to user so they can utilize all functions of DataFrameClient
        return self.df_client
    '''
    def build_json(self,data, tags, fields, measurement):

        #add relevant fields for conversion to json
        data['measurement'] = measurement
        #tags and fields must be converted to dictionaries
        data["tags"] = data.apply(transform_to_dict, tags=tags, axis=1)
        data["fields"] = data.apply(transform_to_dict, tags=fields, axis=1)


        #build a list of dictionaries containing json data to give to client
        #only take relevant columns from dataframe
        json = data[["measurement","time", "tags", "fields"]].to_dict("records")

        return json
        '''

    def build_json(self,data, tags, fields, measurement):

        data['measurement'] = measurement
        data['tags'] = data.iloc[:,1].apply(transform_to_dict, key=tags) # Turn tags into dictionary and apply to column 'tags'
        data["fields"] = data.iloc[:,2].apply(transform_to_dict, key=fields)

        #build a list of dictionaries containing json data to give to client
        #only take relevant columns from dataframe
        json = data[["measurement","time", "tags", "fields"]].to_dict("records")

        #print(json)
        #print(data.head())
        return json

    def post_to_DB(self,json,database=None):
        ret = self.client.write_points(json,database=database,batch_size=16384)
        return ret

    def write_data(self,data,tags,fields,measurement,database=None):
        json = self.build_json(data,tags,fields,measurement)
        self.post_to_DB(json,database=database)

    def list_DB(self):
        '''
        Returns a list of all the names of the databases on the influxDB server
        '''
        list_to_return = []
        DB_dict_list = self.client.get_list_database()

        for x in range(len(DB_dict_list)):
            list_to_return.append(DB_dict_list[x]['name'])

        return list_to_return

    def list_retention_policies(self):

        '''
        Returns a list of dictionaries with all the databases
        on the influxDB server and their associated retention policies
        '''
        DB_list = self.list_DB()
        dict_list = []
        for x in range(len(DB_list)):
            temp_dict = {}
            temp_dict[DB_list[x]] = self.client.get_list_retention_policies(DB_list[x])
            dict_list.append(temp_dict)
        return dict_list

    def query_data(self,query):
        df = self.df_client.query(query, database='wifi_data8',chunked=True, chunk_size=256)
        return df

    def query(self, query, use_database = None):
        query_result = self.client.query(query, database=use_database)
        return query_result.raw

    def show_meta_data(self, database, measurement):
        '''
        Returns a list of TAG KEYS for specified measurement in specified database
        Equivalent query is below
        SHOW TAG KEYS FROM "MEASUREMENT_ARGUMENT"
        '''

        result_list = []
        #generate query string and make query
        query_string = 'SHOW TAG KEYS FROM ' +'\"' + measurement + "\""
        query_result = self.client.query(query_string, database=database)
        #add all of the tag values into a list to be returned
        #query result is a generator
        for temp_dict in query_result.get_points():
            result_list.append(temp_dict['tagKey'])
        return result_list

    def get_meta_data(self,database, measurement,tag):
        '''
        Returns a list of TAG VALUES for specified measurement in specified database
        Equivalent query is below
        SHOW TAG VALUES FROM "MEASUREMENT_ARGUMENT" WITH KEY IN = "TAG_ARGUMENT"
        '''
        result_list = []
        #generate query string and make query
        query_string = 'SHOW TAG VALUES FROM ' + '\"' + measurement + '\"' + 'WITH KEY = \"' + tag + '\"'
        query_result = self.client.query(query_string, database=database)

        #add all of the tag values into a list to be returned
        #query result is a generator
        for temp_dict in query_result.get_points():
            result_list.append(temp_dict['value'])

        return result_list
    def get_meta_data_time_series(self,database, measurement, tags,start_time=None,end_time=None):
        '''
        Returns tags along with the time stamps
        '''

        #get all data with from measurement
        df = self.specific_query(database,measurement,start_time=start_time,end_time=end_time)
        return df[tags]

    def specific_query(self,database,measurement,fields=None,start_time=None,end_time=None,tags=None,values=None,groupList=None,groupTime=None):
        '''
        This function returns a dataframe with the results of the specified query

        '''
        tag_string = ""
        time_string = ""
        group_string = ""
        df = {}
        #Create base query with fields and measurement
        query_string = "SELECT "
        if (fields == None):
            query_string = query_string + '* '
        else:
            for x in range(len(fields)):
                if (x > 0):
                    query_string = query_string + " ,"
                query_string = query_string + "\"" + fields[x] + "\""
        query_string = query_string + " FROM \"" + measurement + "\""

        #Create time portion of query if it is specified
        if (start_time != None or end_time != None ):
            if (start_time != None):
                #Must have a start_time for our query
                #Check to see format of time that was specified
                time_string = time_string + "time > "
                if type(end_time) == str:
                    time_string = time_string + "\'" + start_time + '\''
                if(type(end_time) == int): 
                    time_string = time_string + str(start_time)

            if (end_time != None):
                #Must have a end_time for our query
                #Check to see format of time that was specified
                if (time_string != ""):
                    time_string = time_string + " AND "
                time_string = time_string + "time < "

                if type(end_time) == str:
                    time_string = time_string + "\'" + end_time + '\''

                if type(end_time) == int: 
                    time_string = time_string + str(end_time)
                

        #Create tag portion of query if it is specified
        if (tags != None and values != None):
            try:
                if (len(tags) != len(values)):
                    print("Tags and values do not match raise exception later!")
                    raise BaseException
                else:
                    tag_string = ""
                    for x in range(len(tags)):
                        if (x > 0):
                            tag_string = tag_string + ' AND '
                        tag_string = tag_string + '\"' + tags[x] + "\" = \'" + values[x] + "\'"
            except BaseException:
                print("Tags and values do not match")
                return pd.DataFrame()
        if (groupList != None):
            query_string = query_string + "GROUP BY"
            for x in range(len(groupList)):
                if (x > 0):
                    query_string = query_string + ","
                if (groupList[x] == "time"):
                    query_string = query_string + "time(" + groupTime + ")"
                else:
                    query_string = query_string + "\""+groupList[x]+"\""

        #Add optional parts of query
        if (time_string != "" or tag_string != ""):
            query_string = query_string + " WHERE "
            if (time_string != ""):
                query_string = query_string + time_string
            if (tag_string != ""):
                if (time_string != ""):
                    query_string = query_string + " AND "
                query_string = query_string + tag_string
        if (group_string != ""):
            query_string = query_string + group_string

        print(query_string)
        df = self.df_client.query(query_string, database=self.database,chunked=True, chunk_size=256)

        if (measurement in df):
            return df[measurement]
        else:
            #Must have an empty result make empty dataframe
            df = pd.DataFrame()
        return df

    def delete_based_on_time(self,database,measurement,start_time=None,end_time=None):
        time_string = ""
        query_string = "DELETE FROM %s "%measurement

        if (start_time != None):
            #Must have a start_time for our query
            #Check to see format of time that was specified
            time_string = time_string + "time > "
            if type(end_time) == str:
                time_string = time_string + "\'" + start_time + '\''
            if type(end_time) == int: 
                time_string = time_string + str(start_time)

        if (end_time != None):
            #Must have a end_time for our query
            #Check to see format of time that was specified
            if (time_string != ""):
                time_string = time_string + " AND "
            time_string = time_string + "time < "

            if type(end_time) == str:
                time_string = time_string + "\'" + end_time + '\''

            if type(end_time) == int: 
                time_string = time_string + str(end_time)

        if time_string != "":
            query_string = query_string + " WHERE "
            if (time_string != ""):
                query_string = query_string + time_string
        
        print(query_string)
        df = self.df_client.query(query_string, database=self.database,chunked=True, chunk_size=256)