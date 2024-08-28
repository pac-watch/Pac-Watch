######
# Welcome to Pac Watch! This is a bot that tweets out the most recent 
# independent expenditures, aka PAC contributions that are not expressly
# on behalf of a particular candidate, donated to candidates running for 
# the US Congress. It pulls contributions from OpenSecrets using the 
# IndependentExpenditures endpoint, tabulates the amount donated by a given
# PAC to a given candidate over some prior time period, and tweets out major 
# information to the @PAC_watch twitter account. 


###=============================================================================
# Imports
import boto3
import datetime
import io
import json
import os
import pandas as pd
from pathlib import Path
import requests
import tweepy
from time import sleep


###=============================================================================
# Initialize an empty records dataframe
def initialize_records(colnames=['cmteid','pacshort','suppopp','candname',
                                 'district','amount','note','party','payee',
                                 'date','origin','source']):
  records_df = pd.DataFrame(columns=colnames)
  return records_df
  
  
###=============================================================================
# Read records
# Either load records file if it exists or initialize it
def get_records(s3=None, bucket_name=None, file_name=None, verbose=False):
  
  try:
    obj = s3.get_object(Bucket=bucket_name, Key=file_name)
    if verbose:
      print("Loading records")
    records_df = pd.read_csv(io.BytesIO(obj['Body'].read()))
  except s3.exceptions.ClientError as e:
    if verbose:
      print("Initializing records")
    records_df = initialize_records()
  
  #if file_name is None:
  #  records_df = initialize_records()
  #else:
  #  obj = s3.get_object(Bucket=bucket_name, Key=file_name)
  #  records_df = pd.read_csv(io.BytesIO(obj['Body'].read()))
  return records_df
  
  
###=============================================================================
# Write records
def write_records(records_df, s3, bucket_name, file_name):
  csv_string = records_df.to_csv(index=False)
  s3.put_object(Bucket=bucket_name, Key=file_name, Body=csv_string.encode('utf-8'))
  

###=============================================================================
# Function to set up client needed for twitter calls
# Returns the client object
def get_twitter_client():
  consumer_key = os.getenv("TWT_CONSUMER_KEY")
  consumer_secret = os.getenv("TWT_CONSUMER_SECRET")
  access_token = os.getenv("TWT_ACCESS_TOKEN")
  access_secret = os.getenv("TWT_ACCESS_SECRET")
  client = tweepy.Client(consumer_key=consumer_key, \
                         consumer_secret=consumer_secret, \
                         access_token=access_token, \
                         access_token_secret=access_secret)
  return client


###=============================================================================
# Function to safely pull data from a website
# attempt to call requests.get on a url
# Returns the web content of the url, or None in case of any request error
def get_check_errors(url, headers = {}):

  headers['User-Agent'] = 'Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US; rv:1.9.0.7) Gecko/2009021910 Firefox/3.0.7'
  headers['Accept'] = 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8'

  try:
    
    # Make the HTTP request
    response = requests.get(url, headers=headers)
    response.raise_for_status()  # Check for errors

    # Parse the JSON data
    json_data = response.json()

  except requests.exceptions.HTTPError as errh:
    print(f"HTTP Error: {errh}")
    return None
  except requests.exceptions.RequestException as err:
    print(f"An error occurred: {err}")
    return None

  return json_data


###=============================================================================
# Function to safely pull json data from a website
# Returns the json, or None in case of a request or json decoding error
def get_json(url, args={}, n_retries = 10, wait_time = 1):
  
  # pull url data
  data = get_check_errors(url, args)
  
  # total of `n_retries` attempts to get data
  for _ in range(n_retries - 1):
    if data is not None:
      break
    sleep(wait_time)
    data = get_check_errors(url, args)
    
  # Return pulled data
  return data

          
###=============================================================================
# Function that returns latest contributions from OpenSecrets
def get_latest_expenditures(n_retries=10):
  api_key=os.environ["OPSEC_ACCESS_KEY"]
  latest_url = ("https://www.opensecrets.org/api/"
                "?method=independentExpend"
                f"&apikey={api_key}"
                "&output=json")
  data = get_json(latest_url, n_retries = n_retries)
  return data
  
  
###=============================================================================
# Function that returns all new contributions from OpenSecrets that 
# haven't yet been processed in a previous iteration of this script
# Returns a pandas dataframe with 0 to 50 donations, or None on an error
def get_latest_data(records_df, n_retries = 10, wait_time = 1):
  
  # Get latest expenditures
  latest_json = get_latest_expenditures(n_retries)
  if latest_json is None or not isinstance(latest_json, dict):
    return None
  
  # Convert JSON data to pandas dataframe
  latest_df = pd.DataFrame([d['@attributes'] \
                            for d in latest_json['response']['indexp']])

  # Throw out rows missing key data
  key_columns = ["pacshort","suppopp","candname","amount"] # Columns that are absolutely essential
  latest_df.replace(r'^\s*$', pd.NA, regex=True, inplace=True) # Replace whitespace strings with NaN
  latest_df.dropna(subset=key_columns, inplace=True) # Drop rows with NaN values in specified columns
                            
  # Set data types of certain columns
  latest_df["amount"] = pd.to_numeric(latest_df["amount"])
  latest_df['date'] = pd.to_datetime(latest_df['date'])
  
  # Only keep latest donations that have not already been seen
  merged_df = latest_df.merge(records_df, how='left', indicator=True)
  new_df = merged_df[merged_df['_merge'] == 'left_only']
  new_df = new_df.drop('_merge', axis=1)
  
  # Add a timestamp to all new contributions signifying when they were pulled
  # Also sort data for neater visibility when inspecting the records file
  if new_df.shape[0] > 0:
      new_df["timestamp"] = str(datetime.datetime.now())
      new_df = new_df.sort_values(by=["date","amount"], ascending=[True, False])
  
  return new_df


###=============================================================================
# Function that constructs the text body of a tweet 
# detailing a single campaign contribution
# 
# Tweets are formatted as follows:
# [PAC] spends $[amount] on [purpose] [for/against] 
# [Candidate first and last name] ([Party]-[District]).
# 
# If the PAC has previously spend money on this candidate in the past 
# `n_months` months, we report that too on a new line:
# They have now spent $[cumulative amount] [for/against] 
# [Candidate lastname] in the past `n_prev_days` days.
#
# Returns the tweet body as a string
def get_tweet_body(row, sum_prev_contribution, n_prev_days=30, char_limit = 280):
  
  pac = row["pacshort"]
  amount = row["amount"]
  purpose = row["note"]
  suppopp = row["suppopp"]
  candidate = row["candname"]
  party = row["party"]
  district = row["district"]
  
  # fix anything that could be interpreted by twitter as a url
  if ".com" in pac:
    pac = pac.replace(".com", " dot com")
  if ".org" in pac:
    pac = pac.replace(".org", " dot org")
  if ".gov" in pac:
    pac = pac.replace(".gov", " dot gov")
  if ".net" in pac:
    pac = pac.replace(".net", " dot net")
  if ".edu" in pac:
    pac = pac.replace(".edu", " dot edu")
    
  amount_str = '{:,}'.format(int(amount))
  purpose = purpose.lower()
  suppopp = suppopp[:-1].lower()
  firstname = candidate.split(", ")[1]
  lastname = candidate.split(", ")[0]
  district = district[:2] if district[2] == "S" else district
  district_string = "-" + district
  
  body = f'{pac} spends ${amount_str} on {purpose} {suppopp} {firstname} ' + \
          f'{lastname} ({party}{district_string}).'
          
  if sum_prev_contribution is not None and sum_prev_contribution > amount:
    time_string = str(n_prev_days) + " days"
    record_amount = '{:,}'.format(int(sum_prev_contribution))
    body = body + f'\n\nThey have now spent ${record_amount} ' + \
                   f'{suppopp} {lastname} in the past {time_string}.'
   
  # if tweet is too long, remove purpose note
  if len(body) > char_limit:
    body = f'{pac} spends ${amount_str} {suppopp} {firstname} ' + \
            f'{lastname} ({party}{district_string}).'
    if sum_prev_contribution is not None:
      body = body + f'\n\nThey have now spent ${record_amount} ' + \
              f'{suppopp} {lastname} in the past {time_string}.'
                   
  # this should probably never happen
  # truncate tweet if it's over twitter's character limit
  if len(body) > char_limit:
    body = body[:char_limit]

  return body


###=============================================================================
# Function that posts a tweet to @PAC_watch
# On failure, sleeps for `wait_time` secs and tries again up to `n_retries` times
# Returns tweet result object on success or None on failure
def send_tweet(message, client, n_retries = 1, wait_time = 1):
  
  def try_send_tweet(message, client):
    try:
      tweet_result = client.create_tweet(text=message)
    except Exception as e:
      print(e)
      tweet_result = None
    return tweet_result
  
  tweet_result = try_send_tweet(message, client)
  while tweet_result is None:
    if n_retries == 0:
      break
    n_retries -= 1
    sleep(wait_time)
    tweet_result = try_send_tweet(message, client)
  
  return None if tweet_result is None else tweet_result
  

###=============================================================================
# Function that executes the main procedure of the script
def main(bucket_name="pac-watch-records", records_file_name="records.csv", \
         min_report_amt=0, between_tweets_time=5, n_prev_days=30, \
         verbose=True, tweet=True, record=True, report_sum_contributions=True):
  
  # Get start time for program execution
  curr_datetime = str(datetime.datetime.now())
  if verbose:
    print("Running at " + curr_datetime)
    
  # Get twitter client for interactions with Twitter API
  twitter_client = get_twitter_client()
  
  # Initialize connection to s3 bucket
  s3 = boto3.client('s3')
    
  # Get records file of previous donations
  # Initialize this file if it doesn't already exist
  records_df = get_records(s3, bucket_name, records_file_name, verbose=verbose)
  
  # Trim records to only include donations within previous `n_prev_days` days
  if records_df.shape[0] > 0:
    records_df['date'] = pd.to_datetime(records_df['date'])
    date_cutoff = datetime.datetime.now() - datetime.timedelta(days=n_prev_days)
    records_df = records_df[records_df['date'] >= date_cutoff]
    
  # Pull new donations from OpenSecrets
  latest_df = get_latest_data(records_df)
  
  # Check for success on pulling new data
  if latest_df is None:
    if verbose:
      print("Null latest data")
    return 1
  else:
    if verbose:
      print(str(latest_df.shape[0]) + " new contributions")

  # Potentially record new contributions, if recording
  if record:
    records_df = pd.concat([records_df, latest_df])
    
  # Update records file to only include contributions within the allowed time frame
  write_records(records_df, s3, bucket_name, records_file_name)

  # Group donations for neater reporting
  # i.e. combine sum total amount for redundant donations
  group_cols = ["pacshort","suppopp","candname","district","party"]
  grouped_records_df = records_df.groupby(group_cols, \
                                          as_index = False)["amount"].sum()
  grouped_latest_df = latest_df.groupby(group_cols + ["note"], \
                                        as_index = False)["amount"].sum()
  if verbose:
    print(str(grouped_latest_df.shape[0]) + " new grouped contributions")
    
  # Iterate over and potentially tweet about each new grouped contribution
  for i in range(grouped_latest_df.shape[0]):
    
    # get row at current iteration
    row = grouped_latest_df.iloc[i]
    
    # only proceed if this contribution meets the minimum amount for reporting
    if row["amount"] >= min_report_amt:
    
      sum_contribution = None
      if report_sum_contributions:
        match_row = grouped_records_df[(grouped_records_df["pacshort"] == row["pacshort"]) & 
                                       (grouped_records_df["suppopp"] == row["suppopp"]) &
                                       (grouped_records_df["candname"] == row["candname"])]
        if len(match_row) > 0:
          sum_contribution = match_row.iloc[0]["amount"]
    
      try:
        tweet_body = get_tweet_body(row, sum_contribution, n_prev_days=n_prev_days)
        if verbose:
          print(tweet_body)
          print("------------")
        if tweet:
          tweet_result = send_tweet(tweet_body, twitter_client)
          if tweet_result is None:
            if verbose:
              print("failed to tweet")
          sleep(between_tweets_time)
      except:
        print("Could not process input for row", str(i))
        print(row)
    
  ## Update records file to only include contributions within the allowed time frame
  ## Potentially record new contributions
  #if record:
  #  records_df = pd.concat([records_df, latest_df])
  #write_records(records_df, s3, bucket_name, records_file_name)
    
  return 0
 
 
###=============================================================================
# Lambda start point 
def lambda_handler(event, context):
  main(bucket_name = "pac-watch-records", records_file_name="records.csv")
  return {"statusCode": 200} 

