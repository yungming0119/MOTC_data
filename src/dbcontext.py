
# Author : @jimmg35

import datetime
import schedule
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT


class Storer():
    """
        Storing processed data from Parser.
    """
    data_list = []
    storage = {}
    def __init__(self, dbcontext):
        self.dbcontext = dbcontext

    def insert(self, data, name: str):
        self.storage[name] = data
        self.data_list.append(name)

    def import2Database(self, item: str, y=None, sm=None, em=None):
        if item == "ProjectData" and self.importGate(item):
            self.dbcontext.ImportProjectMeta(self.storage[item])
        if item == "DeviceMeta" and self.importGate(item):
            self.dbcontext.ImportDeviceMeta(self.storage[item])
        if item == "SensorMeta" and self.importGate(item):
            self.dbcontext.ImportSensorMeta(self.storage[item])
        if item == "FixedData" and self.importGate(item):
            self.dbcontext.ImportFixedSensorData(self.storage[item], y, sm, em)
        
    
    def importGate(self, item):
        if self.data_list.index(item) != -1:
            return True
        else:
            print("Data is not accessible!")
            return False


class Dbcontext():
    """
        Importing data into database.
    """   
    def __init__(self, PGSQL_user_data, database):
        # PostgreSQL server variable.
        self.PGSQL_user_data = PGSQL_user_data
        # Connect to local Postgresql server.
        self.cursor = self.ConnectToDatabase(database)
            
    def ConnectToDatabase(self, database):
        """
            Connect to PostgreSQL database.
        """
        conn = psycopg2.connect(database=database, 
                                user=self.PGSQL_user_data["user"],
                                password=self.PGSQL_user_data["password"],
                                host=self.PGSQL_user_data["host"],
                                port=self.PGSQL_user_data["port"])
        conn.autocommit = True
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        print(f'Successfully connected to local PostgreSQL server| User: @{self.PGSQL_user_data["user"]}')
        print(f'                        Currently connected to database: @{database}')
        cursor = conn.cursor()
        return cursor 

    def ImportProjectMeta(self, projectMeta):
        """
            Import porject metadata into database.
        """
        for projID in list(projectMeta.keys()):
            keys_arr: str = "'{"
            for index, i in enumerate(projectMeta[projID]["keys"]):
                if index == (len(projectMeta[projID]["keys"])-1):
                    keys_arr += '"' + str(i) + '"' + "}'"
                    break
                keys_arr += '"' + str(i) + '"' + ','
            query = '''INSERT INTO projectmeta (projectid, projectname, projectkeys) 
                                VALUES({}, \'{}\', {});'''.format(str(projID), 
                                                                  projectMeta[projID]["name"],
                                                                  keys_arr)
            self.cursor.execute(query)
        print("Project Metadata has been stored into database!")
    
    def ImportDeviceMeta(self, deviceMeta):
        """
            Import device meta into database.
        """
        column_str = "("
        query = "select column_name from information_schema.columns where table_name = 'devicemeta';"
        self.cursor.execute(query)
        column = [i[0] for i in self.cursor.fetchall()]
        for index, i in enumerate(column):
            if index == (len(column)-1):
                column_str += i + ")"
                break
            column_str += i + ","
        for index, i in enumerate(deviceMeta):
            values = self.bulidDeviceMetaQuery(i, index)
            query = "INSERT INTO devicemeta " + column_str + values
            self.cursor.execute(query)
        print("Device Metadata has been stored into database!")

    def ImportSensorMeta(self, SensorMeta):
        """
            Import metadata of sensor of each device into database.
        """
        ids = 1
        for device in SensorMeta:
            sensor_id = "'{"
            for index, i in enumerate(device[2]):
                if index == (len(device[2])-1):
                    sensor_id += '"' + str(i) + '"' + "}'"
                    break
                sensor_id += '"' + str(i) + '"' + ','
            query = '''INSERT INTO sensormeta (id, deviceid, projectkey, sensor_id)
                        VALUES({}, \'{}\', \'{}\', {});'''.format(ids, device[0], device[1], sensor_id)
            self.cursor.execute(query)
            ids += 1
        print("Sensor Metadata has been stored into database!")

    def bulidDeviceMetaQuery(self, device, count):
        """
            Helper function of ImportDeviceMeta(), 
            for handling exception.
        """
        output = " VALUES(" + str(count) + "," + device["id"] + ","
        for index, i in enumerate(list(device.keys())):
            if index == (len(list(device.keys())) - 1):
                output += "'" + str(device[i]) + "')"
                break
            if i == "id":
                continue
            if str(device[i]) == "旗山區公所前'":
                output += "'" + "旗山區公所前" + "',"
                continue
            output += "'" + str(device[i]) + "',"
        return output
    
    def queryDeviceSensorMeta_fixed(self):
        """
            query specific metadata from database.
        """
        query = '''SELECT projectid, projectkey, deviceid, sensor_id FROM sensormeta INNER JOIN projectmeta ON sensormeta.projectkey = ANY(projectmeta.projectkeys) WHERE projectid IN ('528','671','672','673','674',
            '675','677','678','680','709','756','1024','1025','1027','1029','1032','1034','1035','1036','1048',
            '1058','1071','1072','1075','1079','1084','1085','1102','1120','1145','1147','1162','1167','1184','1189','1192','1207');'''
        self.cursor.execute(query)
        return self.cursor.fetchall()
    
    def ImportFixedSensorData(self, FixedSensorData, year, start_m, end_m):

        print("=================== Import into database ===================")
        table_dict = {"1": "minute", "60": "hour"}
        for interval in list(FixedSensorData.keys()):
            for projectid in list(FixedSensorData[interval].keys()):
                # get biggest id in that table
                table_name = table_dict[interval] + "_" + projectid + "_" + str(year) + "_" + str(int(start_m)) + "to" + str(int(end_m)+1)
                print(table_name)
                if self.getBiggestId(table_name) == None:
                    id_for_proj = 1
                else:
                    id_for_proj = self.getBiggestId(table_name) + 1
                # insert data into table
                for a_row in FixedSensorData[interval][projectid]:
                    try:
                        query = '''INSERT INTO {} (id, deviceid, 
                        voc_avg, voc_max, voc_min, voc_median, 
                        pm2_5_avg, pm2_5_max, pm2_5_min, pm2_5_median,
                        humidity_avg, humidity_max, humidity_min, humidity_median,
                        temperature_avg, temperature_max, temperature_min, temperature_median,
                        year, month, day, hour, minute, second, time) 
                        VALUES({},\'{}\',{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},\'{}\');
                        '''.format(table_name, id_for_proj, a_row[0], a_row[1], a_row[2], a_row[3], a_row[4],a_row[5], a_row[6], a_row[7], a_row[8],
                                    a_row[9], a_row[10], a_row[11], a_row[12],a_row[13], a_row[14], a_row[15], a_row[16], a_row[17], a_row[18], a_row[19],
                                    a_row[20],a_row[21], a_row[22], a_row[23])
                        self.cursor.execute(query)
                        id_for_proj += 1
                    except:
                        print("insert exception at  ->   interval:{} projectid:{} ".format(interval, projectid))
                print("insert complete  ->  {}".format(table_name))


    def getBiggestId(self, table_name):
        query = '''SELECT max(id) FROM {};'''.format(table_name)
        self.cursor.execute(query)
        return self.cursor.fetchall()[0][0]    
    
    def queryDeviceSensorMeta_spacial(self):
        query = '''SELECT projectid, projectkey, deviceid FROM devicemeta WHERE projectid IN ('1156', '565', '624', '891');'''
        self.cursor.execute(query)
        return self.cursor.fetchall()

    def queryMinuteMetadata(self, project):
        query = '''SELECT deviceid, projectkey FROM sensormeta INNER JOIN 
        projectmeta ON sensormeta.projectkey = 
        ANY(projectmeta.projectkeys) WHERE projectid = '{}';'''.format(project)
        self.cursor.execute(query)
        data = self.cursor.fetchall()
        return [[i[0], i[1]]for i in data]
        
    def ImportMinuteData(self, deviceid, data, date, time, project, start_month):
        """ 將時間區段內的一台感測器資料輸入至資料庫 """

        table_name = "minute_{}_{}to{}".format(project, start_month, start_month+1)

        if self.getBiggestId(table_name) == None:
            ids = 1
        else:
            ids = self.getBiggestId(table_name) + 1

    
        for i in range(0, len(deviceid)):
            query = '''INSERT INTO {} (id, deviceid, voc, pm2_5, humidity, temperature, date, hour, minute, second) 
                        VALUES({}, \'{}\', \'{}\', \'{}\', \'{}\', \'{}\', \'{}\', \'{}\', \'{}\', \'{}\');
                        '''.format(table_name, ids, deviceid[i], data[i]["voc"], data[i]["pm2_5"],
                                    data[i]["humidity"], data[i]["temperature"],
                                    date[i], time[i][0], time[i][1], time[i][2])
            self.cursor.execute(query)
            ids += 1

    def launchPatch(self):
        queries = ['''DELETE FROM devicemeta WHERE projectid 
                    NOT IN ('528','671','672','673','674',
                    '675','677','678','680','709',
                    '756','1024','1025','1027','1029',
                    '1032','1034','1035','1036','1048',
                    '1058','1071','1072','1075','1079',
                    '1084','1085','1102','1120','1145',
                    '1147','1162','1167','1184','1189',
                    '1192','1207','1156','565','624','891');''']
        for index, i in enumerate(queries):
            print("Patch {} has been applied to database!".format(index))
            self.cursor.execute(i)

    def ImportHourData(self, total_chunk_np, meta):

        table_name = "hour_{}_{}".format(meta["porjectId"], meta["startMonth"])

        ids = 1 if self.getBiggestId(table_name) == None else self.getBiggestId(table_name) + 1

        
        for i in range(0, total_chunk_np.shape[0]):
            query = '''INSERT INTO {} (id, deviceid, voc, pm2_5, humidity, temperature, date, hour) 
                        VALUES({}, \'{}\', \'{}\', \'{}\', \'{}\', \'{}\', \'{}\', \'{}\');
                        '''.format(table_name, ids, total_chunk_np[i][0], total_chunk_np[i][1], total_chunk_np[i][2],
                                    total_chunk_np[i][3], total_chunk_np[i][4],
                                    total_chunk_np[i][5], total_chunk_np[i][6])
            self.cursor.execute(query)
            ids += 1
    
    def getRealTimeDataMeta(self):
        query = '''SELECT projectid, projectkeys[1]  FROM projectmeta;'''
        self.cursor.execute(query)
        return self.cursor.fetchall()
        