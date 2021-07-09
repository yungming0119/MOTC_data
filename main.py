# import essential modules
import os 
import sys
import json
import requests
import pandas as pd
from os import listdir, stat
from typing import List, Dict
from colorama import init, Fore, Back, Style
import datetime
import schedule
init(convert=True)

# import main functionality
from src.Parser import GeoJsonParser 
from src.dbcontext import Dbcontext
from src.requester import Requester
from src.utils import UrlBundler, Key


sensor_id = "pm2_5"


if __name__ == "__main__":
    # initialize basic object.
    myKey: Key = Key()
    myBundler = UrlBundler()
    myReq = Requester(myBundler, myKey)

    # initialize dbcontext
    myDBcontext = Dbcontext({"user":str(sys.argv[1]), 
                            "password":str(sys.argv[2]), 
                            "host":str(sys.argv[3]), 
                            "port":str(sys.argv[4])}, "sensordata")
    staticProjectMeta = myDBcontext.getRealTimeDataMeta()

    
    ids = 0
    
    def crawlData(staticProjectMeta):
        global ids
        os.mkdir(r"data\data_{}".format(ids))
        for project in staticProjectMeta:
            data = myReq.getRealTimeProjectData(
                project[1], 
                sensor_id
            )
            GeoJsonParser.parseProjectRealTime2GeoJson(
                data, 
                str(project[0]),
                r"data\data_{}".format(ids)
            )
        ids += 1

    
    schedule.every(300).seconds.do(lambda: crawlData(staticProjectMeta))

    
    while True:
        schedule.run_pending()
        
        #project[0]
        