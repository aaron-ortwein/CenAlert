import datetime
import random
import time
import typing
import logging
import os
import urllib

import secrets
import string

import pandas as pd


#debug
import json


from datetime import timedelta
from datetime import date
from dateutil.relativedelta import relativedelta

DATE_FORMAT_MONTH = "%Y-%m"
DATE_FORMAT = "%Y-%m-%d"
DATE_FORMAT_TIME = "%Y-%m-%d %H:%M:%S"
COUNTRY_REGION = "country"
START_DATE = "start_date"
END_DATE = "end_date"
PREFIX = "gt_"
HOURLY = "hourly"
DELIMITER = "\n"
GT_URL_PREFIX = "https://trends.google.com/trends/explore?"
VPN_GT_TOPIC_CODE = "/m/012t0g"
PROXY_SERVER_GT_TOPIC_CODE = "/m/0kjbp"

# used to get overall popular queries/topics
EMPTY_GT_TOPIC_CODE = ""

SERVER = 'https://trends.googleapis.com'
API_VERSION = 'v1beta'
DISCOVERY_URL_SUFFIX = '/$discovery/rest?version=' + API_VERSION
DISCOVERY_URL = SERVER + DISCOVERY_URL_SUFFIX

# Unused for now
# def build_gt_service(api_key: str):
#     return build('trends', API_VERSION, developerKey=api_key,
#                  discoveryServiceUrl=DISCOVERY_URL)


class SpikeInfo:

    def __init__(self,
                 peak_date: datetime.datetime,
                 peak_value: float,
                 local_left_min_date: datetime.datetime,
                 local_left_min_value: float,
                 global_left_min_date: datetime.datetime,
                 global_left_min_value: float,
                 local_right_min_date: datetime.datetime,
                 local_right_min_value: float,
                 global_right_min_date: datetime.datetime,
                 global_right_min_value: float,
                 threshold: float):
        assert peak_date is not None and peak_value >= 0, f"Peak data must exist {peak_date}, {peak_value}"
        self.peak_date = peak_date
        self.peak_value = peak_value
        self.threshold = threshold

        # if there are no local/global values, default to the peak
        self.local_left_min_date = local_left_min_date or peak_date
        self.local_left_min_value = local_left_min_value
        self.global_left_min_date = global_left_min_date or peak_date
        self.global_left_min_value = global_left_min_value
        self.local_right_min_date = local_right_min_date or peak_date
        self.local_right_min_value = local_right_min_value
        self.global_right_min_date = global_right_min_date or peak_date
        self.global_right_min_value = global_right_min_value

        # make sure if the local/global dates are the same as the peak_date, then it must have same peak_value
        if self.local_left_min_date == peak_date:
            self.local_left_min_value = peak_value
        if self.global_left_min_date == peak_date:
            self.global_left_min_value = peak_value
        if self.local_right_min_date == peak_date:
            self.local_right_min_value = peak_value
        if self.global_right_min_date == peak_date:
            self.global_right_min_value = peak_value




def random_sleep(min_sec: int = 0, max_sec: int = 2):
    sleep_duration = random.uniform(min_sec, max_sec)
    time.sleep(sleep_duration)




def find_threshold_and_local_min(timeline_df: pd.DataFrame,
                                 peak_value: float,
                                 threshold: float,
                                 date_column_name: str,
                                 value_column_name: str) -> typing.Tuple[
    datetime.datetime, float, datetime.datetime, float]:
    local_min_found = False
    local_min_date = None
    local_min_value = 0

    global_date = None
    global_value = 0

    if len(timeline_df) > 0:
        local_min_value = peak_value
        global_value = peak_value

        # this method always loops through the timeline from top to bottom
        for index, row in timeline_df.iterrows():
            if pd.notna(row[value_column_name]):
                if not local_min_found and row[value_column_name] > threshold:
                    if row[value_column_name] <= local_min_value:
                        local_min_date = row[date_column_name]
                        local_min_value = row[value_column_name]
                    else:
                        # once the value is larger, the local_min is found
                        local_min_found = True

                if row[value_column_name] <= threshold:
                    global_date = row[date_column_name]
                    global_value = row[value_column_name]

            # once we found this, we are done
            if global_date:
                if not local_min_found:
                    local_min_date = global_date
                    local_min_value = global_value
                    local_min_found = True
                break

        if not global_date and not local_min_found:
            # default to first row in timeline
            if pd.notna(timeline_df.iloc[0][value_column_name]):
                global_date = timeline_df.iloc[0][date_column_name]
                global_value = timeline_df.iloc[0][value_column_name]
        if local_min_found and not global_date:
            global_date = local_min_date
            global_value = local_min_value
        if global_date and not local_min_found:
            local_min_date = global_date
            local_min_value = global_value

    return local_min_date, local_min_value, global_date, global_value


def find_left_right_of_peak(timeline_df: pd.DataFrame,
                            peak_date: datetime.datetime,
                            peak_value: float,
                            threshold: float,
                            date_column_name: str,
                            value_column_name: str) -> SpikeInfo:
    df_for_left = timeline_df.copy()
    df_for_left[date_column_name] = pd.to_datetime(df_for_left[date_column_name], format=DATE_FORMAT)
    df_for_left = df_for_left[df_for_left[date_column_name] < peak_date]

    # reverse the DataFrame for the left
    df_for_left = df_for_left.iloc[::-1]

    local_left_min_date, local_left_min_value, global_left_date, global_left_value = \
        find_threshold_and_local_min(df_for_left,
                                     peak_value,
                                     threshold,
                                     date_column_name,
                                     value_column_name)

    if len(df_for_left) > 0:
        if df_for_left[df_for_left[date_column_name] == 
local_left_min_date][value_column_name].any():
            assert df_for_left[df_for_left[date_column_name] == 
local_left_min_date][value_column_name].iloc[
                       0] == local_left_min_value, \
                f"{local_left_min_date}: local_left_min Values do not match: expected {df_for_left[df_for_left[date_column_name].iloc[0] == local_left_min_date][value_column_name]} but got {local_left_min_value}"
        if df_for_left[df_for_left[date_column_name] == global_left_date][value_column_name].any():
            assert df_for_left[df_for_left[date_column_name] == global_left_date][value_column_name].iloc[
                       0] == global_left_value, \
                f"{global_left_date}: global_left_min Values do not match: expected {df_for_left[df_for_left[date_column_name] == global_left_date][value_column_name].iloc[0]} but got {global_left_value}"

    # find the right side
    df_for_right = timeline_df.copy()
    df_for_right[date_column_name] = pd.to_datetime(df_for_right[date_column_name], format=DATE_FORMAT)

    df_for_right = df_for_right[df_for_right[date_column_name] > 
peak_date]

    local_right_min_date, local_right_min_value, global_right_date, global_right_value = \
        find_threshold_and_local_min(df_for_right,
                                     peak_value,
                                     threshold,
                                     date_column_name,
                                     value_column_name)

    if len(df_for_right) > 0:
        if df_for_right[df_for_right[date_column_name] == local_right_min_date][value_column_name].any():
            assert df_for_right[df_for_right[date_column_name] == local_right_min_date][value_column_name].iloc[
                       0] == local_right_min_value, \
                f"{local_right_min_date}: local_right_min Values do not match: expected {df_for_right[df_for_right[date_column_name] == local_right_min_date][value_column_name].iloc[0]} but got {local_right_min_value}"

        if df_for_right[df_for_right[date_column_name] == global_right_date][value_column_name].any():
            assert df_for_right[df_for_right[date_column_name] == global_right_date][value_column_name].iloc[
                       0] == global_right_value, \
                f"{global_right_date}: global_right_min Values do not match: expected {df_for_right[df_for_right[date_column_name] == global_right_date][value_column_name].iloc[0]} but got {global_right_value}"

    return SpikeInfo(peak_date, peak_value,
                     local_left_min_date, local_left_min_value, 
global_left_date, global_left_value,
                     local_right_min_date, local_right_min_value, 
global_right_date, global_right_value, threshold)


WIDEN_WINDOWS = [30, 60, 90, 180, 360, 720]
CURRENT_DATE_STR = date.today().strftime("%Y-%m-%d")
DEP_DATE_STR = "2024-03-27"

def widen_date_range(start_date: datetime.datetime,
                     end_date: datetime.datetime,
                     days: int = 30,
                     date_format: str = 
DATE_FORMAT_MONTH,deprecated=False) -> typing.Tuple[str, str]:
    start_date_str = (start_date - 
timedelta(days=days)).strftime(date_format)
    new_end = end_date + timedelta(days=days)

    # double checking data is NOT in the future
    today = datetime.datetime.strptime(CURRENT_DATE_STR, DATE_FORMAT) # datetime.datetime.today()
    if deprecated:
        today = datetime.datetime.strptime(DEP_DATE_STR, DATE_FORMAT) # datetime.datetime.today()
    # UPDATE THIS
    if new_end.year > today.year or (new_end.year == today.year and 
new_end.month > today.month):
        new_end = datetime.datetime(year=today.year, month=today.month, 
day=1)
    end_date_str = new_end.strftime(date_format)
    return start_date_str, end_date_str


###################################################################################### Graph Download ########################################################################################
def generate_random_string(length=20):
    characters = string.ascii_lowercase + string.digits
    return ''.join(secrets.choice(characters) for _ in range(length))



def get_google_trends_urls_hourly_data(country_code: str,
                                       topic_code: str = VPN_GT_TOPIC_CODE,
                                       start_date: str = None,
                                       end_date: str = None,
                                       logger: logging = None,
                                       start_timezone: str = "T5",
                                       end_timezone: str = "T5") -> typing.Tuple[str, list, list]:
    topic_code = urllib.parse.quote_plus(topic_code)

    urls = []
    keys = []
    
    try:
        

        if start_date and end_date:
            start_datetime = datetime.datetime.strptime(start_date, DATE_FORMAT)
            end_datetime = datetime.datetime.strptime(end_date, DATE_FORMAT)
            range = datetime.timedelta(days=7)

            while start_datetime <= end_datetime - range:
                range_end = start_datetime + range
                _start_str = start_datetime.strftime(DATE_FORMAT)
                _range_end_str = range_end.strftime(DATE_FORMAT)
                date_range = f'{_start_str}{start_timezone} {_range_end_str}{end_timezone}'
                urls.append(f"{GT_URL_PREFIX}date={urllib.parse.quote_plus(date_range)}&geo={country_code}&q={topic_code}&hl=en")
                
                keys.append(f"{country_code}_{_start_str}_{_range_end_str}_{HOURLY}")

                # increment
                start_datetime += datetime.timedelta(days=1)

    except (AttributeError, LookupError) as e:
        logger.warning(f"Could not get google trends urls for {country_code}")
    print(urls)
    return country_code, urls, keys


def get_google_trends_urls(country_code: str,
                           start_date: str = None,
                           end_date: str = None,
                           topic: str = VPN_GT_TOPIC_CODE):

    topic_code = urllib.parse.quote_plus(topic)
    start_datetime = datetime.datetime.strptime(start_date, DATE_FORMAT_MONTH)
    end_datetime = datetime.datetime.strptime(end_date, DATE_FORMAT_MONTH)

    end_datetime = end_datetime + relativedelta(months=1) - relativedelta(days=1)

    date_range = start_datetime.strftime(DATE_FORMAT) + " " + end_datetime.strftime(DATE_FORMAT)


    # find last day of month
    url = f"{GT_URL_PREFIX}date={urllib.parse.quote_plus(date_range)}&geo={country_code}&q={topic_code}&hl=en"
    
    #https://trends.google.com/trends/explore?date=2021-10-01%202021-12-01&geo=BF&q=%2Fm%2F012t0g&hl=en-US

    return url


def error_check(logger, error, delay=60):
    logger.error(error.args)
    if len(error.args) > 1:
        if '"code": 429' in str(error.args[1]):
            logger.error(f"HTTPError: Ran out of queries to the Google Trends API, initiating {delay} sec sleep")
            time.sleep(delay)
    return 0


def tlvl_gparse(graph, dest, fname):
    response = graph.execute()
    if(len(response) > 0):
        datanorm = response['item']
        data = pd.json_normalize(datanorm)
        data.to_csv(os.path.join(dest,fname),index=False)
        return 1
    else:
        return 0


def get_geo_map(service, logger, dest, start_date_str, end_date_str, 
country_code, topic: str = VPN_GT_TOPIC_CODE):
    try:
        graph = service.regions().list(term=topic, 
                                       restrictions_startDate=start_date_str, restrictions_endDate=end_date_str, 
                                       restrictions_geo=country_code)
        response = graph.execute()
        if(len(response) > 0):
            datanorm = response['regions']
            data = pd.json_normalize(datanorm)
            data.to_csv(os.path.join(dest,"geoMap.csv"),index=False)
    except Exception as error:
        error_check(logger, error)
        logger.warning(f"Unable to retrieve geoMap.csv file for {country_code} with start date {start_date_str} and end date {end_date_str}")


def get_multi_timeline(service, logger, dest,fname, start_date_str, 
end_date_str, country_code, topic: str = VPN_GT_TOPIC_CODE, attempts=10):
    # append a random string to force api refresh
    random_str = generate_random_string(20)
    topics = f"{topic} + {random_str} + {topic.upper()}"
    for i in range(attempts):
        try:
            graph = service.getGraph(terms=topics, 
                                     restrictions_startDate=start_date_str, restrictions_endDate=end_date_str, 
                                     restrictions_geo=country_code)
            response = graph.execute()
            if(len(response) > 0):
                datanorm = response['lines'][0]['points']
                data = pd.json_normalize(datanorm)
                data.to_csv(os.path.join(dest,fname),index=False)
            return
        except Exception as error:
            error_check(logger, error, (i + 1) * 61)
    time.sleep(1)
    logger.critical(f"Unable to retrieve multiTimeline file for {country_code} with start date {start_date_str} and end date {end_date_str}")
            

def get_multi_timeline_windows(service, logger, dest, start_date_str, window_s_str,
end_date_str, country_code, topic=VPN_GT_TOPIC_CODE, window=8, overlap=1):
    start_datetime = datetime.datetime.strptime(start_date_str, '%Y-%m')
    end_datetime = datetime.datetime.strptime(end_date_str, '%Y-%m')
    today = datetime.datetime.strptime(CURRENT_DATE_STR, DATE_FORMAT)
    
    if end_datetime > today:
        end_datetime= today
    window_start =  datetime.datetime.strptime(window_s_str, '%Y-%m')
    done = False
    while window_start <= end_datetime: #and window_start <= today
        window_end = window_start + relativedelta(months=(window - 1))

        if window_end.year > today.year or (window_end.year == today.year and window_end.month > today.month):
            window_end = datetime.datetime(year=today.year, 
                                           month=today.month, day=1)

        if window_end >= end_datetime:
            window_end = end_datetime
            done = True

        window_s_str = window_start.strftime(DATE_FORMAT_MONTH)
        window_e_str = window_end.strftime(DATE_FORMAT_MONTH)
        fname =  "{0}_multiTimeline.csv".format(window_s_str)
        
        
        get_multi_timeline(service, logger, dest, fname, window_s_str, 
                           window_e_str, country_code, topic)
        
            
        get_multi_timeline(service, logger, dest, 
                            f"{window_s_str}_coarseMultiTimeline.csv", start_date_str, window_e_str, 
                            country_code, topic)

        window_start += relativedelta(months=(window - overlap))


        if done:
            # delete the last day of data
            file_path = os.path.join(dest, fname)
            if os.path.exists(file_path):
                try:
                    df = pd.read_csv(file_path)
                    if len(df) > 0:
                        df = df.iloc[:-1]
                        df.to_csv(file_path, index=False)
                        logger.info(f"Removed last row from final window file: {file_path}")
                except Exception as e:
                    logger.warning(f"Failed to clean last row of {file_path}: {e}")
            break
    # Check if all windows are correctly stored
    # check_multi_timeline_windows(dest, start_date_str, end_date_str, country_code, window, overlap)


def check_multi_timeline_windows(dest, start_date_str, 
end_date_str, country_code, window=8, overlap=1):
    start_datetime = datetime.datetime.strptime(start_date_str, '%Y-%m')
    end_datetime = datetime.datetime.strptime(end_date_str, '%Y-%m')
    today = datetime.datetime.strptime(CURRENT_DATE_STR, DATE_FORMAT)
    missings = []

    if end_datetime > today:
        end_datetime= today
    window_start = start_datetime
    done = False
    while window_start <= end_datetime: #and window_start <= today:
        window_end = window_start + relativedelta(months=(window - 1))
        
        if window_end.year > today.year or (window_end.year == today.year and window_end.month > today.month):
            window_end = datetime.datetime(year=today.year, month=today.month, day=1)
        
        if window_end >= end_datetime:
            window_end = end_datetime
            
            done = True

        start_str = window_start.strftime('%Y-%m')
        end_str = window_end.strftime('%Y-%m')
        fname =  "{0}_multiTimeline.csv".format(start_str)
        mpath = os.path.join(dest,fname)

        coarse_fname = "{0}_coarseMultiTimeline.csv".format(start_str)
        coarse_mpath = os.path.join(dest, coarse_fname)

        
        if not os.path.exists(mpath) and not os.path.exists(coarse_mpath):
            
            missings.append({
                "country_code": country_code,
                "start_month": start_str,
                "end_month": end_str
            })



        window_start += relativedelta(months=(window - overlap))
        
        if done:
            break

    missing_file = 'missing_windows.csv'
    if missings:
        df = pd.DataFrame(missings)
        
    else:
        
        return

    if not os.path.exists(missing_file):
        # If missings is empty, write to CSV
        df.to_csv(missing_file, index=False)
       
    else:
        df.to_csv(missing_file, mode='a', index=False, header=False)
        


    


