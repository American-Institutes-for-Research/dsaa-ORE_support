# dsaa-ORE_support
Scraping email addresses from web pages
Project Tier: 1

Project Lead: Mike Trinh

Client: ORE Support Services

Project Summary: Code to scrape email addresses from web homepages or related pages

get_website_emails_utilities.py:
    Utility functions to clean URL, determine whether a URL is scrapable, extract a regex pattern from the links on a webpage (intended for email addresses, but can be any regex pattern), and adding the regex patterns back to the original dataframe. 

email_crawler.py:
    Python script that takes in a .csv or .xlsx file containing websites (in a 'website' column) and scrapes the provided urls for emails, crawling one layer deep to any 'contact' or 'about' pages. This file must also contain a 'scrapability' column indicating which websites are able to be scraped. To run this script (on Windows), use the command prompt:
        ```
        py email_crawler.py [PATH/TO/FILENAME.xlsx]
        ```

    Email crawler output files:
        results_dict.json: Dictionary with urls as keys and scraped emails as values
            This allows users to see the crawled website urls that emails came from
        code_dict.json: Dicitonary with urls as keys and response codes as values
            This allows users to see the crawled website response codes
        website_mappings.json: Dicitonary with indexes as keys and urls as emails
            This allows users to map the crawled urls to the urls provided in the original file

            All three of these files are generated and saved as the script is running to ensure no data is lost if the program is terminated early.
        
        crawled_emails.xlsx: Excel file with the scraped emails for each website

process_data.py:
    Python script to process the .json files outputted by email_crawler.py into a more usable format (the same as crawled_emails.xlsx). That way, if the script for email_crawler.py is terminated early or if a user would like to check on the results without interrupting the code, they are able to view them easily. This script is run the same way as email_crawler.py, and requires the same .csv or .xlsx file, as well as results_dict.json and website_mappings.json. 
    
    crawled_emails_from_json.xlsx: Excel file with the scraped emails for each website