#!/usr/bin/env python
# coding: utf-8

# # Part I. ETL Pipeline for Pre-Processing the Files

# #### Import necessary Python packages 

# Import Python packages 
import pandas as pd
import cassandra
import re
import os
import glob
import numpy as np
import json
import csv


# #### Creating list of filepaths to process original event csv data files

# checking your current working directory
print(os.getcwd())

# Get your current folder and subfolder event data
filepath = os.getcwd() + '/event_data'

# Create a for loop to create a list of files and collect each filepath
for root, dirs, files in os.walk(filepath):
    
# join the file path and roots with the subdirectories using glob
    file_path_list = glob.glob(os.path.join(root,'*'))
    

# #### Processing the files to create the data file csv that will be used for Apache Casssandra tables

# initiating an empty list of rows that will be generated from each file
full_data_rows_list = [] 
    
# for every filepath in the file path list 
for f in file_path_list:

# reading csv file 
    with open(f, 'r', encoding = 'utf8', newline='') as csvfile: 
        # creating a csv reader object 
        csvreader = csv.reader(csvfile) 
        next(csvreader)
        
 # extracting each data row one by one and append it        
        for line in csvreader:
            #print(line)
            full_data_rows_list.append(line) 
            
# creating a smaller event data csv file called event_datafile_full csv that will be used to insert data into the \
# Apache Cassandra tables
csv.register_dialect('myDialect', quoting=csv.QUOTE_ALL, skipinitialspace=True)

with open('event_datafile_new.csv', 'w', encoding = 'utf8', newline='') as f:
    writer = csv.writer(f, dialect='myDialect')
    writer.writerow(['artist','firstName','gender','itemInSession','lastName','length','level','location','sessionId','song','userId'])
    for row in full_data_rows_list:
        if (row[0] == ''):
            continue
        writer.writerow((row[0], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[12], row[13], row[16]))

# check the number of rows in your csv file
with open('event_datafile_new.csv', 'r', encoding = 'utf8') as f:
    print(sum(1 for line in f))


# # Part II. Complete the Apache Cassandra coding portion of your project. 
# ## Reading the CSV file into a Pandas dataframe for easy handling

df = pd.read_csv('event_datafile_new.csv')
# We add useful columns like User and we round the length of the songs down to 2 decimals
df['User'] = df['firstName'] + ' ' + df['lastName']
df['length'] = df['length'].round(2)
# We check for the presence of NULL values
NULL_Values = df.isnull().sum().sum()
print(NULL_Values)
# We check the datatypes of the final column set
df.info()

# Creation of the final dataframe with the relevant datatypes
clean_data = df[['artist', 'itemInSession', 'length', 'sessionId', 'song', 'userId', 'User']]


# #### Creating a Cluster
# This should make a connection to a Cassandra instance your local machine 
# (127.0.0.1)

from cassandra.cluster import Cluster
try: 
    cluster = Cluster(['127.0.0.1']) #If you have a locally installed Apache Cassandra instance
    session = cluster.connect()
except Exception as e:
    print(e)

# #### Create Keyspace
try:
    session.execute("""
    CREATE KEYSPACE IF NOT EXISTS udacity 
    WITH REPLICATION = 
    { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }"""
)

except Exception as e:
    print(e)

# #### Set Keyspace
try:
    session.set_keyspace('udacity')
except Exception as e:
    print(e)


# ## DATA MODELLING

## TO-DO: Query 1: The artist, song title and song's length in the music app history that was heard during\ 
## sessionId = 338, and itemInSession = 4
## Creation of the table Sessions_Listens with a PK(sessionId, itemInSession) as those fields are needed for filtering and provide uniqueness

query = "CREATE TABLE IF NOT EXISTS Sessions_Listens"
query = query + "(sessionId int, itemInSession int, artist varchar, song_title varchar, song_length float, PRIMARY KEY (sessionId, itemInSession))"
try:
    session.execute(query)
except Exception as e:
    print(e)            


# #### We carry out the data insertion required iterating over the dataframe

for row in clean_data.itertuples(index=False):
    query = "INSERT INTO Sessions_Listens(sessionId, itemInSession, artist,song_title,song_length)"
    query = query + "values(%s,%s,%s,%s,%s)"
    session.execute(query, (row.sessionId, row.itemInSession, row.artist, row.song, row.length))


# ####SELECT to verify that the data have been inserted into the table

query = "SELECT artist, song_title, song_length FROM Sessions_Listens WHERE sessionId = 139 and itemInSession = 3"
try:
    rows = session.execute(query)
except Exception as e:
    print(e)
    
for row in rows:
    print (f"Artist: {row.artist}, Song: {row.song_title}, Length: {row.song_length}")


## Query 2: The artist, song (sorted by itemInSession) and user (first and last name)\
## for userid = 10, sessionid = 182
## Creation of the table Sessions_Artists with a PK(userId, sessionId, itemInSession) as those fields are needed for filtering, provide uniqueness and we
## need to include the itemInSession for the sorting requested

query = "CREATE TABLE IF NOT EXISTS Sessions_Artists"
query = query + "(userId int, sessionId int,artist varchar, song_title varchar, user varchar, itemInSession int, PRIMARY KEY (userId, sessionId, itemInSession))"
try:
    session.execute(query)
except Exception as e:
    print(e)     
                    
                    
# #### We carry out the data insertion required iterating over the dataframe

for row in clean_data.itertuples(index=False):
    query = "INSERT INTO Sessions_Artists(userId, sessionId, artist, song_title, user, itemInSession)"
    query = query + "values(%s,%s,%s,%s,%s,%s)"
    session.execute(query, (row.userId, row.sessionId, row.artist, row.song, row.User, row.itemInSession))
                    
                    
# ####SELECT to verify that the data have been inserted into the table
query = "SELECT artist, song_title, user FROM Sessions_Artists WHERE userId = 10 and sessionId = 182"
try:
    rows = session.execute(query)
except Exception as e:
    print(e)
    
for row in rows:
    print(f"Artist: {row.artist}, Song: {row.song_title}, User: {row.user}")
   

# Query 3: Every user name (first and last) in my music app history who listened to the song 'All Hands Against His Own'
## Creation of the table User_Listens with a PK(song_title, artist) as those fields are needed for filtering and provide uniqueness,
## song_title could have potentially not provided uniqueness on its own

query = "CREATE TABLE IF NOT EXISTS User_Listens"
query = query + "(song_title varchar, user varchar, artist varchar, PRIMARY KEY (song_title, artist))"
        
try:
    session.execute(query)
except Exception as e:
    print(e)     
                    


# #### We carry out the data insertion required iterating over the dataframe

for row in clean_data.itertuples(index=False):
    query = "INSERT INTO User_Listens(song_title, user, artist)"
    query = query + "values(%s,%s,%s)"
    session.execute(query, (row.song, row.User, row.artist))



# ####SELECT to verify that the data have been inserted into the table

query = "SELECT user FROM User_Listens WHERE song_title = 'All Hands Against His Own' "
try:
    rows = session.execute(query)
except Exception as e:
    print(e)
    
for row in rows:
    print(f" User: {row.user}")
  

# ### Drop the tables before closing out the sessions

for t in ["Sessions_Listens", "Sessions_Artists", "User_Listens"]:
    query = f"DROP TABLE {t}"
    try:
        rows = session.execute(query)
    except Exception as e:
        print(e)

# ### Close the session and cluster connection¶

session.shutdown()
cluster.shutdown()
