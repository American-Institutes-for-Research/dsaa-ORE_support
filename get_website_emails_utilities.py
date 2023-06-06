import pandas as pd
import urllib.robotparser
from bs4 import BeautifulSoup
import requests
import socket


#clean URL
def clean_url(series):
    """
    Cleans URL to ensure final character is '/' 
    
    Args: 
        series - pandas series of one URL per row
        
    Returns:
        websites - list of cleaned websites that can be appended to original dataframe
    """
    websites = []
    for site in series:
        if site[-1] == '.':
                site = site[:-1]
                #site = site+'/'
                websites.append(site)
        elif site[-1] != '/':
            site = site+'/'
            websites.append(site)
        else:
            websites.append(site)
    return websites


#determine if website is scrapable
def get_scrapability(series):
    """
    Iterates through a pandas series of URLs to determine if the URL is scrapable. Print statements can be uncommented in order to track progress. Timeout is set to ten seconds to not spend more than that amount of time trying to fetch or read a URL.
    
    Args:
        series - a pandas series containing one URL per row
        
    Returns:
        list - A list of the results, which can be True, False, or 'site skipped' if there was a fetching or reading error; list can be appended to the original dataframe
    """
    socket.setdefaulttimeout(10) 
    scrapability = []
    for i, site in enumerate(series):
        try:
            rp = urllib.robotparser.RobotFileParser()
            rp.set_url(site+'robots.txt')
            rp.read()
            #print(site, rp.can_fetch("*", site))
            scrapability.append(rp.can_fetch("*", site))
            #print(i, site)
        except:
            #print(site, 'site skipped')
            scrapability.append('site skipped')
    return scrapability


#get regex patterns from scrapable websites
def get_homepage_addresses(series, regex):
    """
    Iterates through a series of URLs and scrapes link text for a regex pattern on that URL. Print statements can be uncommented to track progress. Timeout is set to ten seconds to not spend more than that amount of time trying to read a URL.
    
    Args:
        series - a pandas series containing one URL per row
        regex - regex search pattern
        
    Returns:
        sites - list of URLs; may be duplicates if a website has more than one of the regex pattern
        address - list of found regex patterns; originally used for email address patterns
    """
    socket.setdefaulttimeout(10)
    sites = []
    address = []
    for i, site in enumerate(series):
        try:
            response = requests.get(site, headers={'User-Agent': '*'})
            soup = BeautifulSoup(response.text, 'html.parser')
            links = soup.findAll('a')
            for link in links:
                addresses = re.findall(regex, link.text)
                if addresses != []:
                    #print(i, addresses)
                    sites.append(site)
                    address.append(addresses)
        except:
            #print(i, 'website error')
            sites.append(site)
            address.append('website error')
    return address, sites


def merge_regex_patterns(sites, address, original_df):
    """
    Merges sites and addresses with original dataframe after dropping duplicates
    
    Args:
        sites - list output from get_homepage_addresses
        address - list output from get_homepage_addresses
        original_df - dataframe the iterable series came from in get_homepage_addresses
        
    Returns:
        original dataframe with new column for email addresses (or other regex pattern)
    """
    df = pd.DataFrame()
    df['website'] = sites
    df['regex_pattern'] = address

    #Drop website error rows
    df = df.loc[df["regex_pattern"] != 'website error']

    #extract strings from list
    df['pattern'] = [item[0].strip() for item in df['regex_pattern']]

    #drop initial emails column that is a list of strings
    df = df.drop(columns=['regex_pattern'])

    #drop duplicate emails
    df = df.drop_duplicates(ignore_index=True)

    #merge with original dataframe on 'website'
    df2 = pd.merge(original_df, df, on='website', how='outer')

    df2 = df2.drop(columns=['Unnamed: 0']).reset_index(drop=True)
    return df2