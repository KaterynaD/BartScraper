import os
import requests
import xmltodict
from dateutil import parser
import dateutil
import pandas as pd
import time, datetime
import traceback
import logging
import pandas as pd
import subprocess
import threading
import Queue

stations=['12th',
'16th',
'19th',
'24th',
'ashb',
'balb',
'bayf',
'cast',
'civc',
'cols',
'colm',
'conc',
'daly',
'dbrk',
'dubl',
'deln',
'plza',
'embr',
'frmt',
'ftvl',
'glen',
'hayw',
'lafy',
'lake',
'mcar',
'mlbr',
'mont',
'nbrk',
'ncon',
'oakl',
'orin',
'pitt',
'phil',
'powl',
'rich',
'rock',
'sbrn',
'sfia',
'sanl',
'shay',
'ssan',
'ucty',
'wcrk',
'wdub',
'woak']

class Store(object):
    def save(self):
        pass

    def read(self):
        pass

class CSV(Store):
    def __init__(self, filename):
        self.filename = filename
        if not os.path.isfile(self.filename):
            with open(self.filename, "wb") as output:
                output.write('TimeMark,ScheduledTime,Delay,Station,Destination,Line,TrainLength,Bikeflag\n')

    def save(self, data):
        with open(self.filename, 'a') as output:
            for l in data:
                if l['duplicate_flg']=='0':
                    output.write(l['time_mark']+','+l['scheduled_time']+','+l['delay']+','+ l['station']+','+l['destination']+','+l['line']+','+l['train_length']+','+l['bikeflag']+'\n')

class Display(Store):
    def __init__(self):
        print 'TimeMark,ScheduledTime,Delay,Station,Destination,Line,TrainLength,Bikeflag'

    def save(self, data):
        for l in data:
            if l['duplicate_flg']=='0':
                print l['time_mark']+','+l['scheduled_time']+','+l['delay']+','+ l['station']+','+l['destination']+','+l['line']+','+l['train_length']+','+l['bikeflag']


class Scraper(object):
    def __init__(self, api_key, stores=None, schedule_filename='stations_schedule.csv', origin_destination_filename='origin_destination.csv'):
        self.api_key = api_key
        self.last_fetch_df = None
        self.last_fetch = None
        self.stores = stores
        self.schedule_filename=schedule_filename
        self.origin_destination_filename=origin_destination_filename
        #origin-destination
        self.df_od=None
        self.sync_origin_destination(queue=None)
        #stations schedule
        self.df_ssch=None
        self.sync_schedule(queue=None)
        self.start_time=pd.to_datetime(self.df_ssch['ScheduledTime']).min()
        self.end_time=pd.to_datetime(self.df_ssch['ScheduledTime']).max()
    def sync_schedule(self,queue):
        print 'Syncing Stations Schedule...'
        station_schedule=[]
        try:
            for s in stations:
                url = 'http://api.bart.gov/api/sched.aspx?key={api_key}&cmd=stnsched&orig={station}'.format(api_key=self.api_key, station=s)
                resp_xml = requests.get(url)
                resp_dict=xmltodict.parse(resp_xml.text)
                for i in resp_dict['root']['station']['item']:
                    scheduled_time={}
                    scheduled_time['Station']=resp_dict['root']['station']['abbr']
                    scheduled_time['Line']= i['@line']
                    scheduled_time['Destination']= i['@trainHeadStation']

                    struct_time = time.strptime(i['@origTime'],'%I:%M %p')
                    h=struct_time.tm_hour
                    if h<4:
                        tm = parser.parse(resp_dict['root']['date']  + ' ' + i['@origTime']) + datetime.timedelta(days=1)
                    else:
                        tm = parser.parse(resp_dict['root']['date']  + ' ' + i['@origTime'])
                        scheduled_time['ScheduledTime']=datetime.datetime.strftime(tm, '%Y-%m-%d %H:%M:%S')
                        station_schedule.append(scheduled_time)
            df_ssch=pd.DataFrame.from_records(station_schedule)
            if queue:
                queue.put(df_ssch)
            else:
                self.df_ssch=df_ssch
        except Exception as e:
            logging.error('Fetch failed.\n\t Response: {resp} \n\t Error: {error}'.format(resp=resp_xml.text, error=e.message))

    def sync_origin_destination(self,queue):
        print 'Syncing Routes Info (Origins and Destinations)...'
        Origin_Destination=[]
        try:
            url = 'http://api.bart.gov/api/route.aspx?key={api_key}&cmd=routeinfo&route=all'.format(api_key=self.api_key)
            resp_xml = requests.get(url)

            resp_dict=xmltodict.parse(resp_xml.text)
            routes_num=len(resp_dict['root']['routes']['route'])
            for i in range(0,routes_num-1):
                Origin_Destination.append([resp_dict['root']['routes']['route'][i]['origin'],resp_dict['root']['routes']['route'][i]['destination']])
            df_od=pd.DataFrame(Origin_Destination,columns=['Origin','Destination'])
            if queue:
                queue.put(df_od)
            else:
                self.df_od=df_od
        except Exception as e:
            logging.error('Fetch failed.\n\t Response: {resp} \n\t Error: {error}'.format(resp=resp_xml.text, error=e.message))

    def fetch(self):
        url = 'http://api.bart.gov/api/etd.aspx?key={api_key}&cmd=etd&orig=all'.format(api_key=self.api_key)
        real_departure_data=[]
        if self.last_fetch is not None:
            self.last_fetch_df=pd.DataFrame(self.last_fetch, columns=['time_mark','station','destination','direction','train_length','bikeflag','color','duplicate_flg'])
        #.......................................................................
        def get_estimates (time_mark, station, destination,estimates):
            result={}
            if type(estimates) != list:
                estimates = [estimates]
            for e in estimates:
                if e['minutes']=='Leaving':
                    result['time_mark']=datetime.datetime.strftime(time_mark, '%Y-%m-%d %H:%M:%S')
                    result['station']=station
                    result['destination']=destination
                    result['direction']=e['direction']
                    result['train_length']=e['length']
                    result['bikeflag']=e['bikeflag']
                    result['color']=e['color']
                    #exclude duplicates due to bart train time spent on a station (loading)
                    if self.last_fetch_df is not None:
                        already_leaving=self.last_fetch_df.loc[((self.last_fetch_df['station']==station) &
                                             (self.last_fetch_df['destination']==destination) &
                                             (self.last_fetch_df['direction']==e['direction']) &
                                             (self.last_fetch_df['train_length']==e['length']) &
                                             (self.last_fetch_df['bikeflag']==e['bikeflag'])
                                             )]
                        if len(already_leaving)==0:
                            result['duplicate_flg']='0' #previous time leaving event
                        else:
                            result['duplicate_flg']='1' #current time leaving event record,
                    else:
                        result['duplicate_flg']='0' #we do not have data about previous departures
                    #try to get schedule time if the record will be recorded (not duplicate)
                    if ((result['duplicate_flg']=='0') & (self.df_ssch is not None) ):
                        scheduled_time=self.df_ssch.loc[((self.df_ssch['Station']==station) &
                            (self.df_ssch['Destination']==destination) &
                            (pd.to_datetime(self.df_ssch['ScheduledTime'])>=pd.to_datetime(datetime.datetime.strftime(time_mark, '%Y-%m-%d %H:%M:%S')))
                            ),['ScheduledTime']]
                        st1=scheduled_time.min().iloc[0]

                        scheduled_time=self.df_ssch.loc[((self.df_ssch['Station']==station) &
                            (self.df_ssch['Destination']==destination) &
                            (pd.to_datetime(self.df_ssch['ScheduledTime'])<=pd.to_datetime(datetime.datetime.strftime(time_mark, '%Y-%m-%d %H:%M:%S')))
                            ),['ScheduledTime']]
                        st2=scheduled_time.max().iloc[0]
                        if not pd.isnull(st1) and not pd.isnull(st2):
                            diff1 = abs(parser.parse(result['time_mark'])-parser.parse(st1))
                            diff1_min=(diff1.days * 24 * 60) + (diff1.seconds/60)
                            diff2 = abs(parser.parse(result['time_mark'])-parser.parse(st2))
                            diff2_min=(diff2.days * 24 * 60) + (diff2.seconds/60)
                            if min(diff1_min,diff2_min)==diff1_min:
                                result['scheduled_time']=st1
                            else:
                                result['scheduled_time']=st2
                        elif pd.isnull(st1) and pd.isnull(st2):
                            result['scheduled_time']=result['time_mark']
                        elif not pd.isnull(st1) and pd.isnull(st2):
                            result['scheduled_time']=st1
                        else:
                            result['scheduled_time']=st2
                        #is origin-destination
                        isOriginDestination=len(self.df_od.loc[((self.df_od['Origin']==station) &
                         (self.df_od['Destination']==destination))])
                        try:
                             #line(route) for station, destination and time
                             result['line']=self.df_ssch.loc[((self.df_ssch['Station']==station) &
                             (self.df_ssch['Destination']==destination) &
                             (pd.to_datetime(self.df_ssch['ScheduledTime'])==result['scheduled_time'])
                             ),['Line']].iloc[0]['Line']
                        except:
                            result['line']='Not in Schedule'
                        #delay in minutes
                        #less then 1 min delay is due to the scraper and API delays
                        diff = parser.parse(result['time_mark'])-parser.parse(result['scheduled_time'])
                        diff_min=(diff.days * 24 * 60) + (diff.seconds/60)-1
                        if ((diff_min<=1) or (isOriginDestination==1)):
                           result['delay']='0'
                        elif (diff_min>30):
                           result['delay']='-1'
                        else:
                           result['delay']=str(diff_min)

                    else:
                        result['scheduled_time']=result['time_mark']
                        result['line']='None'
                        result['delay']='0'
                    real_departure_data.append(result)
        #.......................................................................
        try:
            resp_xml = requests.get(url)
            resp_dict=xmltodict.parse(resp_xml.text)
            time_mark = parser.parse(resp_dict['root']['date'] + ' ' + resp_dict['root']['time'])
            for s in resp_dict['root']['station']:
                if type(s['etd']) != list:
                    get_estimates(time_mark,s['abbr'], s['etd']['abbreviation'], s['etd']['estimate'])
                else:
                    for d in s['etd']:
                        get_estimates(time_mark,s['abbr'],d['abbreviation'],d['estimate'])
        except Exception as e:
            logging.error('Fetch failed.\n\t Response: {resp} \n\t Error: {error}'.format(resp=resp_xml.text, error=e.message))
        self.last_fetch = real_departure_data


    def sync(self):
        for store in self.stores:
            store.save(self.last_fetch)

if __name__ == '__main__':
    import yaml,os,sys
    try:
        ResourceFile=sys.argv[2]
    except:
        ResourceFile="ProjectResources.yml"
    resources_filepath = os.path.abspath(os.path.expanduser(ResourceFile))
    # Check if the resource file exists.
    if not os.path.exists(resources_filepath):
        sys.exit("ProjectResources.yml file is required to run the application")
    try:
        with open(ResourceFile, "r") as f:
            res = yaml.load(f)
            #data collection params
            bart_api_key = res["bart_api_key"]
            bart_scraper_log = res["bart_scraper_log"]
            real_time_data = res["real_time_data"]
            stations_schedule = res["stations_schedule"]
    except KeyError or IOError:
        sys.exit("Wrong parameters")
    logging.basicConfig(filename=bart_scraper_log,format='%(asctime)s %(levelname)s: %(message)s', level=logging.ERROR)
    SaveToCsv = CSV(filename=real_time_data)
    PrintOut=Display()
    scraper = Scraper(api_key=bart_api_key, stores=[SaveToCsv,PrintOut], schedule_filename=stations_schedule)
    #scraper.fetch()
    #scraper.sync()
    #scraper.fetch()
    #scraper.sync()
    #sys.exit()
    print scraper.start_time.strftime("%d-%m-%Y")
    scraper.df_ssch.to_csv(stations_schedule.format(schedule_date=scraper.start_time.strftime("%d-%m-%Y")))
    print 'Now: ',
    print  datetime.datetime.now().strftime("%d-%m-%Y %H:%M:%S")
    print 'Shedule Start Time: ',
    print  scraper.start_time
    print 'Shedule Last Time: ',
    print  scraper.end_time

    i=0;

    queue_od = Queue.Queue()
    queue_ssch = Queue.Queue()
    while True:
        start_time = time.time()
        try:
            #check the trains till the end of the schedule + a few min
            if (scraper.start_time <= datetime.datetime.now() <= scraper.end_time + datetime.timedelta(minutes = 10)):
                scraper.fetch()
                scraper.sync()
            #update schedule and route info periodically
            #do not reset the schedule till the last train for the day arrive to the destination
            i=i+1
            #if ((i>=10) & (not(pd.to_datetime(scraper.start_time.strftime("%d-%m-%Y")) + datetime.timedelta(days = 1) <= datetime.datetime.now() <= scraper.end_time ))):
            if (i>=10) :
                Threads=[]
                Threads.append(threading.Thread(target=scraper.sync_origin_destination, args=(queue_od,)))
                Threads.append(threading.Thread(target=scraper.sync_schedule, args=(queue_ssch,)))
                for t in Threads:
                    t.start()
                i=0
            if not queue_od.empty():
                print 'Refreshing main Origin Destination Store'
                scraper.df_od=pd.DataFrame(queue_od.get())
            if not queue_ssch.empty():
                print 'Refreshing main Station Schedule Store'
                scraper.df_ssch=pd.DataFrame(queue_ssch.get())
                scraper.start_time=pd.to_datetime(scraper.df_ssch['ScheduledTime']).min()
                scraper.end_time=pd.to_datetime(scraper.df_ssch['ScheduledTime']).max()
                scraper.df_ssch.to_csv(stations_schedule.format(schedule_date=scraper.start_time.strftime("%d-%m-%Y")))
                print 'Now: ',
                print  datetime.datetime.now().strftime("%d-%m-%Y %H:%M:%S")
                print 'Shedule Start Time: ',
                print  scraper.start_time
                print 'Shedule Last Time: ',
                print  scraper.end_time
        except Exception as e:
            logging.error(traceback.format_exc())
        end_time = time.time()
        duration = end_time - start_time
        time.sleep(max([0, 30 - duration]))
