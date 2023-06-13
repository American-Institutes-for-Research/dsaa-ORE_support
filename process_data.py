import json
import sys
import pandas as pd
import numpy as np

#Read in the file
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

#Read in the generated data files
try:
    with open('results_dict.json') as json_file:
        results_dict = json.load(json_file)
except:
    print("Error loading results_dict.json")
    sys.exit(1)
try:
    with open('website_mapping.json') as json_file:
        website_mapping = json.load(json_file)
except:
    print("Error loading website_mapping.json")
    sys.exit(1)

#Verify column names
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

df = df.iloc[:len(collapsed_list)]
df['emails'] = collapsed_list

#Remove fake emails
to_remove = ["example","test","domain","email","@sentry","wixpress","automattic"]
df['emails'] = df['emails'].apply(lambda lst: [elem for elem in lst if not any(substr in elem for substr in to_remove)])

if renamed[0]:
    df = df.rename(columns = {'website' : 'Website'})
if renamed[1]:
    df = df.rename(columns = {'emails' : 'Emails'})
if renamed[2]:
    df = df.rename(columns = {'scrapability' : 'Scrapability'})


full_dataset = full_dataset.merge(df, how='outer')

full_dataset.to_excel('crawled_emails_from_json.xlsx', index=False)
