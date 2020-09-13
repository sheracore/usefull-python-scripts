import re
import sys
import csv
import psycopg2

from clickhouse_driver import Client

class Update:
    def __init__(self,postgres, client,*args,**kwargs):
        print(sys.argv[1], sys.argv[2], sys.argv[3])
        self.conn = psycopg2.connect(postgres_txt)
        self.province = args[0]
        self.region = kwargs["region"]
        self.file_name = kwargs["file_name"]


    def get_file_data(self):
        cursor = self.conn.cursor()

        data = list()
        with open(self.file_name) as f:
            csv_reader = csv.reader(f, delimiter=',')
            for row in csv_reader:
                data.append(row)

        for row in data[1:]:
            device_name = row[0]
            slot  = row[3]
            port  = row[5]
            subtype = row[7]
            port_level = row[9]
            cursor.execute("select distinct(resource_name) from mtn_transmission_mapping where device_name = '%s' " %(device_name))
            resources = cursor.fetchall()
            
            for resource in resources:
                pattern_slot = re.compile(r'%s-(.?)'%(device_name))
                pattern_port = re.compile(r'-(.?)\(')
                
                if resource[0] == None:
                    continue

                slot_no = pattern_slot.findall(resource[0])
                port_no = pattern_port.findall(resource[0])
                if len(slot_no) == 0 or len(port_no) == 0:
                    continue 
                if slot_no[0] == slot and port_no[0] == port:
                    #print(resource[0]) 
                    cursor.execute("Update mtn_transmission_mapping set port_level='%s', subtype='%s' , type = 'FE/GE', region='%s', province='%s' where resource_name = '%s'" %(port_level,subtype,self.region,self.province,resource[0]))        
                    print("Update mtn_transmission_mapping set port_level='%s', subtype='%s' , type = 'FE/GE', region='%s', province='%s' where resource_name = '%s'" %(port_level,subtype,self.region,self.province,resource[0]))

                self.conn.commit()


if __name__ == '__main__':
    postgres_txt = "host='localhost' dbname='rpat' user='postgres' password='123456'"
    client = Client('localhost')
    client.execute('use rpat')
    
    object = Update(postgres_txt,client, sys.argv[3], region = sys.argv[2], file_name = sys.argv[1])
    object.get_file_data()


