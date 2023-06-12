import os
import re
import pandas as pd
import urllib.parse
import urllib.robotparser
import requests
from bs4 import BeautifulSoup
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


#Scrape webpage for email addresses
@retry.retry(ConnectionError, tries=3, delay=1)
def find_email_addresses(url):
    """
    Scrape a webpage to find email addresses.

    Parameters:
    - url (str): The URL of the webpage to scrape.

    Returns:
    - Tuple: A tuple containing a list of email addresses found on the webpage and the response status code.
    """
    # Send a GET request to the URL and retrieve the HTML content
    response = requests.get(url, headers={'User-Agent': '*'})
    if response.status_code != 200:
        return [], response.status_code
    
    html_content = response.text

    # Create a Beautiful Soup object to parse the HTML
    soup = BeautifulSoup(html_content, 'html.parser')

    # Find all text nodes in the HTML
    text_nodes = soup.find_all(text=True)

    # Regular expression pattern to match email addresses
    email_pattern = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b'

    # Extract email addresses from the text nodes
    email_addresses = []
    for node in text_nodes:
        matches = re.findall(email_pattern, node)
        email_addresses.extend(matches)

    return email_addresses, response.status_code

#Crawls a webpage looking for contact pages
@retry.retry(ConnectionError, tries=3, delay=1)
def crawl_page(url):
    """
    Crawl a webpage to find contact pages.

    Parameters:
    - url (str): The URL of the webpage to crawl.

    Returns:
    - List: A list of URLs representing the contact pages found on the webpage.
    """
    url_base = urllib.parse.urlparse(url).scheme + "://" + urllib.parse.urlparse(url).hostname

    response = requests.get(url, headers={'User-Agent': '*'})
    if response.status_code != 200:
        return []
    soup = BeautifulSoup(response.text, 'html.parser')
    links = [a.get('href') for a in soup.find_all('a', href=True)]
    keep = ["contact" in link or "about" in link for link in links]
    contact_links = [links[i] for i in range(len(links)) if keep[i]]
    new_links = []
    for link in contact_links:
        new_links.append(urllib.parse.urljoin(url_base, link))
    return new_links



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

#Check column names
full_dataset = df
renamed = [False, False, False]
if 'Website' in df.columns:
    df = df.rename(columns = {'Website' : 'website'})
    renamed[0] = True
if 'website' in df.columns:
    df = df.dropna(subset=['website'])
else:
    print("Error: The file must contain a 'website' column.")
    sys.exit(1)
if 'Emails' in df.columns:
    df = df.rename(columns = {'Emails' : 'emails'})
    renamed[1] = True
if 'emails' in df.columns:
    user_input = input("Warning: 'emails' column will be overwritten. Press enter to continue, or 'q' to quit: ")
    if (user_input == 'q'):
        sys.exit(1)
    df = df.drop('emails', axis=1)
    if renamed[1]:
        full_dataset = full_dataset.drop('Emails', axis=1)
    else:
        full_dataset = full_dataset.drop('emails', axis=1)
if 'Scrapability' in df.columns:
    df = df.rename(columns = {'Scrapability' : 'scrapability'})
    renamed[2] = True
if 'scrapability' in df.columns:
    df = df.loc[df['scrapability']==True]
else:
    print("Error: The file must contain a 'scrapability' column.")
    sys.exit(1)

df = df.reset_index(drop=True)


#Looking at valid (scrapable) urls
urls = df['website'].tolist()
results_dict = {}
code_dict = {}
website_mapping = {}
print(f"Number of websites to scrape: {len(urls)}")
start_time = time.time()
print(f"Current progress: 0 / {len(urls)}         Elapsed time: 0 secs")
#for each website
for i in range(len(urls)):
    url = urls[i]
    emails = []
    code = None
    #try to get the emails
    try:
        emails, code = find_email_addresses(url)
    except SSLError as e:
        try:
            url = url.replace("https://", "http://")
            emails, code = find_email_addresses(url)
        except:
            emails = []
            code = -1
    except:
        emails = []
        code = -1

    #remove duplicates
    emails = list(set(emails))
    results_dict[url] = emails
    code_dict[url] = code
    website_mapping.setdefault(i, []).append(url)

    #next, crawl the webpage for "contact" and "about" links
    if code==200:
        try:
            contact_urls = crawl_page(url)
        except:
            contact_urls = []

        for contact_url in contact_urls:
            try:
                emails, code = find_email_addresses(contact_url)
            except SSLError as e:
                try:
                    url = url.replace("https://", "http://")
                    emails, code = find_email_addresses(contact_url)
                except:
                    emails = []
                    code = -1
            except:
                emails = []
                code = -1
            #remove duplicates
            emails = list(set(emails))
            results_dict[contact_url] = emails
            code_dict[contact_url] = code
            website_mapping.setdefault(i, []).append(contact_url)
            
    if i % 100 == 0 and i != 0:
        print(f"Current progress: {i} / {len(urls)}         Elapsed time: {round(time.time() - start_time, 1)} secs")
    #Save periodically to be safe
    if i % 100 == 0 and i != 0:
        json.dump(results_dict, open("results_dict.json", 'w' ))
        json.dump(code_dict, open("code_dict.json", 'w' ))
        json.dump(website_mapping, open("website_mapping.json", 'w' ))

json.dump(results_dict, open("results_dict.json", 'w' ))
json.dump(code_dict, open("code_dict.json", 'w' ))
json.dump(website_mapping, open("website_mapping.json", 'w' ))

print(f"Scraping complete.         Elapsed time: {round(time.time() - start_time, 1)} secs\n\n")


#Clean up the results and put them into the dataframe
email_list = []
counts = []
for idx, sites in website_mapping.items():
    email_list.append([value for key, value in results_dict.items() if key in sites])
    counts.append(len([key for key in results_dict.keys() if key in sites])) #counts of all the urls scraped for a given base url for combining later
email_list = [emails for subset in email_list for emails in subset] #all emails are in a list of list, where the inner lists correspond to each url scraped

collapsed_list = []
for i, count in enumerate(counts):
    starting_point = sum(counts[0:i])
    group = email_list[starting_point:(starting_point+counts[i])]
    collapsed_list.append(list(set([emails for website in group for emails in website])))
df['emails'] = collapsed_list

#Remove fake emails
to_remove = ["example","test","domain","email","@sentry","wixpress","automattic"]
df['emails'] = df['emails'].apply(lambda lst: [elem for elem in lst if not any(substr in elem for substr in to_remove)])

#to get details on the response codes and the specific website urls that emails came from, look at the code_dict and results_dict, respectively (the keys are the urls)

if renamed[0]:
    df = df.rename(columns = {'website' : 'Website'})
if renamed[1]:
    df = df.rename(columns = {'emails' : 'Emails'})
if renamed[2]:
    df = df.rename(columns = {'scrapability' : 'Scrapability'})

columns_to_merge = df.columns.difference(['emails'])
full_dataset = full_dataset.merge(df[columns_to_merge], how='outer')

full_dataset.to_excel('crawled_emails.xlsx', index=False)

print("Output file 'crawled_emails.xlsx' created. \n\nFor information on the specific website urls that emails came from, the response codes from those websites, and the mapping from crawled websites to the provided urls, please see results_dict.json, code_dict.json, and website_mappings.json")