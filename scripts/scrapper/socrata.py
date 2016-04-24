import sys
import time
import datetime
import requests
import os

path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))

if not path in sys.path:
    sys.path.insert(1, path)
del path

from utils.configParser import parse
import database.mysql as db

def date_to_timestamp(stime):
    stime = stime.split('T')
    date = stime[0]
    temp = date.split("-")
    a = []
    a.append(int(temp[0]))
    a.append(int(temp[1]))
    a.append(int(temp[2]))
    date = stime[1].split(':')
    for i in date:
        a.append(int(i))
    a = datetime.datetime(a[0], a[1], a[2], a[3], a[4], a[5]).timetuple()
    # year, month, day, hour, minute, second, microsecond, and tzinfo.
    return int(time.mktime(a))

class Socrata():
    """Class to fetch data using socrata API"""

    def __init__(self, limit, config_path):
        
        self.conn = db.connect(config_path)
        self.cursor = self.conn.cursor()
        self.url = "https://data.cityofchicago.org/resource/ijzp-q8t2.json"
        self.limit = limit
        config = parse(config_path)
        self.socrata_key = config["socrata.key"]
    
    def fetch_json(self, offset=0):
        payload = {'$limit': self.limit, '$offset': offset, '$$app_token':self.socrata_key}
        
        try :
            r = requests.get(self.url, params=payload)
        except requests.exceptions.ChunkedEncodingError:
            print payload
            return self.fetch_json(offset = offset)

        to_save = ['latitude', 'longitude', 'id', 'primary_type','date', 'x_coordinate', 'y_coordinate', 
        'district', 'ward', 'community_area', 'fbi_code']
        print r.url
        if r.json():
            sql = "INSERT INTO dataset ("
            for i in to_save:
                sql+=i+" , "
            sql=sql[:-2]
            sql+= ") VALUES "
            for i in r.json():
                to_insert = "( "
                for j in to_save:
                    if j not in i.keys():
                        i[j] = "\'\'"
                    else:
                        if j == 'date':
                            i[j] = str(date_to_timestamp(i[j]))
                        i[j] = "\'"+i[j]+"\'"   
                    to_insert+=i[j]+", "
                to_insert = to_insert[:-2]
                to_insert+='), '
                sql+=to_insert
            sql = sql[:-2]
            db.write(sql, self.cursor, self.conn)
            return 1
        else:
            return 0

    def fetch_all(self, offset = 0):
        while(self.fetch_json(offset = offset)):
            offset+=self.limit
            print offset, " elements inserted in db."



if __name__ == '__main__':
    config_path = "src/main/resources/reference.conf"
    a = Socrata(limit = 1, config_path = config_path)
    offset = 4870000
    a.fetch_json(offset)
    # a.fetch_all(offset = offset) 