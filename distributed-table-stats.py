#!/usr/bin/python
# 
# This script collects distributed table's list from ClickHouse server (from system.tables) with database/table name and path on the filesystem, then counting amount of data in each directory.
# It's necessary for tracking data insertion delay in ClickHouse, when data was inserted to the distributed table, but haven't been written to the shards/replicas
#
# For authentificaion in ClickHouse username and password can be provided via JSON config, otherwise 'default' user with no password will be used.
# ClickHouse server must be available via HTTP interface.
# Result can be stored in a file in JSON or table format.
#

import requests
import json
import os
import argparse
from requests.exceptions import HTTPError

parser = argparse.ArgumentParser(description='Calculate size of data in directories of distributed tables')
parser.add_argument('file_name', metavar='FILENAME', help='Output file')
parser.add_argument('--format', default='json', choices=['json', 'table'],  help='Output file format')
parser.add_argument('--url', default='http://localhost:8123', help='ClickHouse HTTP endpoint address')
parser.add_argument('--config', help='Path to config file in JSON format containing user and password')

# parse arguments
args = parser.parse_args()

url = args.url
f_format = args.format
o_file = args.file_name

# Defaults
username = 'default'
password = ''

query = "select arrayStringConcat(array(database,name),'.') as table,data_path from system.tables where engine='Distributed' FORMAT TabSeparated"

# Functions
def checkDirectory(path):
  directory = os.path.dirname(path)
  try:
    os.stat(directory)
  except:
    os.makedirs(directory)

def saveAsJSON(file_path, data):
  checkDirectory(file_path)
  with open(file_path, 'w') as fp:
    json.dump(data, fp)

def saveAsTable(file_path, data):
  checkDirectory(file_path)
  with open(file_path, 'w') as fp:
    for key, value in data.items():
      fp.write("%s %s\n" % (key, value))

function_save = {
   'json': saveAsJSON,
   'table': saveAsTable
}

# Try to get username and password from config
try:
  config_file = open(args.config)
  j_config = json.load(config_file)
  username = j_config['username']
  password = j_config['password']
except:
  pass

# Get tables list with data paths from ClickHouse
try:
  response = requests.post(url, data=query, auth=(username,password), timeout=60, allow_redirects=False)
  response.raise_for_status()
except HTTPError as http_err:
  print (http_err)
except Exception as err:
  print (err)
else:
  # Iterate over table dierctories
  table_size = {}
  for line in response.text.split('\n'):
    if line.strip():
      table,path = line.split()

     # Calculate file sizes in directory    
      size = 0
      for dirpath, dirs, files in os.walk(path):
        for file in files:
          fp = os.path.join(dirpath, file)
          # count files only
          if os.path.isfile(fp):
            size += os.path.getsize(fp)
      table_size[table] = size

      if table_size:
        # Choose format to save
        write_result_func = function_save.get(f_format)
        write_result_func(o_file,table_size)
