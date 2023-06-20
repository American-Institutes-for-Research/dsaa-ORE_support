import os
import re
import pandas as pd
import urllib.parse
import urllib.robotparser
import math
from googlesearch import search
import ssl
import socket
from requests.exceptions import SSLError, InvalidURL, ConnectionError
import retry
import json
import sys
import time
import reppy
from reppy.robots import Robots
import multiprocessing
import time
import warnings
warnings.filterwarnings("ignore")

socket.setdefaulttimeout(5) #How many seconds to wait before skipping a website
ssl._create_default_https_context = ssl._create_unverified_context #fixed a bug involving certificates


def parallel_can_fetch(url):
    return can_fetch(url)


def can_fetch(url):
    try:
        print(url)
        if url in ['https://www.sandiegocounty.gov/parks/picnic/snapdragon.html', 
                   'https://www.sdparks.org/content/sdparks/en/park-pages/SantaYsabel.html', 
                   'https://www.teamusa.org/usa-softball/about/hall-of-fame/illinois-hall-of-fame']:
            return 'site skipped'
        robot_url = Robots.robots_url(url)
        robots = Robots.fetch(robot_url)
        return robots.allowed(url, '*')
    except reppy.exceptions.ConnectionException:
        return 'invalid URL'
    except reppy.exceptions.SSLException:
        if "https://" in url:
            return can_fetch(url.replace("https://", "http://"))
        else:
            return 'SSL Error'
    except (reppy.exceptions.ExcessiveRedirects, reppy.exceptions.BadStatusCode):
        # Robots.txt file does not exist
        return True
    except reppy.exceptions.MalformedUrl:
        return 'malformed URL'
    except reppy.exceptions.ContentTooLong:
        return 'content too long'


@retry.retry(ConnectionError, tries=3, delay=1)
def check_scrapability(url):
    """
    Check robots.txt to see if a site can be scraped

    Parameters:
    - url (str): The URL of the webpage to scrape.

    Returns:
    - Tuple: A tuple containing a boolean for if the website can be scraped, and an int for if the site was skipped (0 for not skipped, 1 for skipped, 2 for invalid url)
    """
    try:
        rp = urllib.robotparser.RobotFileParser()
        rp.set_url(url+'robots.txt')
        rp.read()
        return rp.can_fetch("*", url), 0
    except SSLError as e:
        try:
            url = url.replace("https://", "http://")
            rp.set_url(url+'robots.txt')
            rp.read()
            return rp.can_fetch("*", url), 0
        except InvalidURL as e:
            return False, 2
        except:
            return False, 1
    except InvalidURL as e:
        return False, 2
    except:
        return False, 1
    

def read_file(file_path):
    try:
        if file_path.lower().endswith('.csv'):
            return pd.read_csv(file_path)
        elif file_path.lower().endswith('.xlsx'):
            return pd.read_excel(file_path)
        else:
            raise ValueError("Unsupported file format, please provide a .csv or .xlsx file: {}".format(file_path))
    except FileNotFoundError:
        print("File not found: {}".format(file_path))
        sys.exit(1)
    except pd.errors.ParserError:
        print("Error parsing the file: {}".format(file_path))
        sys.exit(1)
    except:
        print("An error occurred while reading the file: {}".format(file_path))
        sys.exit(1)


def fix_df(df):
    renamed = [False, False]
    if 'Website' in df.columns:
        df = df.rename(columns = {'Website' : 'website'})
        renamed[0] = True
    if 'website' not in df.columns:
        print("Error: The file must contain a 'website' column.")
        sys.exit(1)
    if 'Scrapability' in df.columns:
        df = df.rename(columns = {'Scrapability' : 'scrapability'})
        renamed[1] = True
    if 'scrapability' not in df.columns:
        df['scrapability'] = None
    cleaned_urls = []
    for url in df['website']:
        if not pd.isna(url):
            url = url.strip().split(' ')[0]
            if not url.startswith('https://') and not url.startswith('http://'):
                url = 'https://' + url
        cleaned_urls.append(url)
    df['website'] = cleaned_urls
    return df


def get_scrapability(df, output_fp, overwrite=False, use_reppy=True):
    start_time = time.time()
    checked = 0
    pool = multiprocessing.Pool()
    processes = {}
    for i in df.index:
        # if i % 50 == 0:
        #     print(f"Current progress: {i} / {df.shape[0]}         Elapsed time: {round(time.time() - start_time, 1)} secs")
        if not overwrite and not pd.isna(df.loc[i, 'scrapability']) and df.loc[i, 'scrapability'] != 'Malformed URL':
            continue
        url = df.loc[i, 'website']
        if pd.isna(url):
            df.loc[i, 'scrapability'] = 'site skipped'
            continue
        if use_reppy:
            p = pool.apply_async(parallel_can_fetch, args=(url, ))
            processes[i] = p
        else:
            df.loc[i, 'scrapability'] = can_fetch(url)
            checked += 1
            if checked % 10 == 0:
                df.to_excel('scrapability.xlsx', index=False)

    if use_reppy:
        for i, p in processes.items():
            try:
                if i % 50 == 0:
                    print(f"Current progress: {i} / {df.shape[0]}         Elapsed time: {round(time.time() - start_time, 1)} secs")
                    df.to_excel(output_fp, index=False)
                df.loc[i, 'scrapability'] = p.get(10)
            except multiprocessing.TimeoutError:
                df.loc[i, 'scrapability'] = 'timed out'
            
    print(f"Complete.         Elapsed time: {round(time.time() - start_time, 1)} secs\n\n")
    df.to_excel(output_fp, index=False)
        

def main():
    #Read in the data
    if len(sys.argv) < 3:
        print("Please provide an input and output filepath. The input file must contain a 'website' column and a 'scrapability' column for which sites can be scraped (T/F).")
        sys.exit(1)
    file_path = sys.argv[1]
    output_fp = sys.argv[2]

    df = read_file(file_path)
    df = fix_df(df)
    get_scrapability(df, output_fp)


if __name__ == '__main__':
    main()
