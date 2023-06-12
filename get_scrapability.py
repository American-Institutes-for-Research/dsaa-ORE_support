import os
import re
import pandas as pd
import urllib.parse
import urllib.robotparser
import math
from googlesearch import search
import ssl
import socket
from requests.exceptions import SSLError, HTTPError, ConnectionError
import retry
import json
import sys
import time

socket.setdefaulttimeout(10) #How many seconds to wait before skipping a website
ssl._create_default_https_context = ssl._create_unverified_context #fixed a bug involving certificates

@retry.retry(ConnectionError, tries=3, delay=1)
def check_scrapability(url):
    """
    Check robots.txt to see if a site can be scraped

    Parameters:
    - url (str): The URL of the webpage to scrape.

    Returns:
    - Tuple: A tuple containing a boolean for if the website can be scraped, and a boolean for if the site was skipped
    """
    try:
        rp = urllib.robotparser.RobotFileParser()
        rp.set_url(url+'robots.txt')
        rp.read()
        return rp.can_fetch("*", url), False
    except SSLError as e:
        try:
            url = url.replace("https://", "http://")
            rp.set_url(url+'robots.txt')
            rp.read()
            return rp.can_fetch("*", url), False
        except:
            return False, True
    except:
        return False, True
    
#Read in the data
if len(sys.argv) < 2:
    print("Please provide a filename or path. The file must contain a 'website' column and a 'scrapability' column for which sites can be scraped (T/F).")
    sys.exit(1)
file_path = sys.argv[1]

try:
    if file_path.lower().endswith('.csv'):
        df = pd.read_csv(file_path)
    elif file_path.lower().endswith('.xlsx'):
        df = pd.read_excel(file_path)
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

urls = df['website'].tolist()
scrapability = []
print(f"Number of websites to check: {len(urls)}")
start_time = time.time()
print(f"Current progress: 0 / {len(urls)}         Elapsed time: 0 secs")
#for each website
for i in range(len(urls)):
    if i % 100 == 0 and i != 0:
        print(f"Current progress: {i} / {len(urls)}         Elapsed time: {round(time.time() - start_time, 1)} secs")

    url = urls[i]
    if pd.isna(url):
        scrapability.append('site skipped')
        continue
    can_scrape, skipped = check_scrapability(url)
    if not can_scrape:
        if skipped:
            scrapability.append('site skipped')
        else:
            scrapability.append(False)
        continue
    scrapability.append(True)
print(f"Complete.         Elapsed time: {round(time.time() - start_time, 1)} secs\n\n")

df['scrapability'] = scrapability

df.to_excel('scrapability.xlsx', index=False)

print("Output file 'scrapability.xlsx' created.")
