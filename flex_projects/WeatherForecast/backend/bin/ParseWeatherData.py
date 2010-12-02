#!/usr/bin/python -w
# -*- coding: utf-8 -*-

# To pull down the weather data from QQ's website, then insert it into the tables.
# Version 1.0
# James Yu(blank-eyes@163.com) 12/02/2010

import sys
import httplib
from optparse import OptionParser
import ConfigParser
from BeautifulSoup import BeautifulSoup
import MySQLdb

#--------------------------------------------------------------------------------
# functions...
#--------------------------------------------------------------------------------
def create_conn(remote_host, username, password, database):
    # create a database connection
    try:
        conn = MySQLdb.connect(host   = remote_host,
                               user   = username,
                               passwd = password,
                               db     = database
            )
        return conn
    except MySQLdb.Error, e:
        print "Failed to connect to "+ remote_host + " - %d: %s" % (e.args[0], e.args[1])
        sys.exit(1)

def cleanup_data(cursor, table):
    # truncate the weather table
    try:
        cursor.execute("TRUNCATE " + table)
    except MySQLdb.Error, e:
        print "Failed to cleanup - %d: %s\n" % (e.args[0], e.args[1])
        sys.exit(1)

def add_data(cursor, table, id, name, overview, temprature, spec):
    # insert new entries into the weather table
    try:
        cursor.execute("""INSERT INTO """ + table + """ VALUES(%s, %s, %s, %s, %s, NOW())""", (id, name, overview, temprature, spec))
    except MySQLdb.Error, e:
        print "Failed to add entries - %d: %s\n" % (e.args[0], e.args[1])
        sys.exit(1)

def download_page(url, path):
    # pull down and parse the specified web page
    try:
        conn = httplib.HTTPConnection(url)
        conn.request("GET", path)
        r = conn.getresponse()
    except Exception, e:
        print "Failed to connect to %s - %d: %s\n" % (url, e.args[0], e.args[1])
        sys.exit(1)
    else:
        if r.status == 200:
            #soup = BeautifulSoup(r.read(), fromEncoding="gb2312")
            soup = BeautifulSoup(r.read())
            #print soup.prettify()
            conn.close()

            return soup
        else:
            print "Failed to pull data from %s\n" % url
            sys.exit(1)

# start process
#--------------------------------------------------------------------------------
# capture inputs from the command line
#--------------------------------------------------------------------------------
#print sys.getdefaultencoding()

parser = OptionParser()
parser.set_usage("python $0 -e ENVIRONMENT")

#parser.add_option("-v", "--verbose", dest="verbose", action="store_true", help="print out debugging messages")
parser.add_option("-r", "--run", dest="run", action="store_true", help="execute")
parser.add_option("-e", "--environment", dest="environment", action="store", help="assign the environment [development|test|production]")

(options, args) = parser.parse_args()

# verbose
#verbose = options.verbose

# run
run = options.run
if run != True:
    print "Run can not be omitted\n"
    sys.exit(1)

config_file_name = ''
config_file_path = '../conf/'

# environment
environment = options.environment
if environment != "development" and environment != "test" and environment != "production":
    print "Environment is NULL\n"
    sys.exit(1)
else:
    if environment == "development":
        config_file_name = 'development.conf'
    elif environment == "test":
        config_file_name = 'test.conf'
    else:
        config_file_name = 'production.conf'

config_file = config_file_path + config_file_name

#--------------------------------------------------------------------------------
# read configurations from the config file
#--------------------------------------------------------------------------------
try:
    config = ConfigParser.SafeConfigParser()
    config.readfp(open(config_file))

    # url
    base_url     = config.get('url', 'base_url')
    initial_path = config.get('url', 'initial_path')
    path_prefix  = config.get('url', 'path_prefix')
    path_suffix  = config.get('url', 'path_suffix')
    
    # db
    host     = config.get('db', 'host')
    username = config.get('db', 'username')
    password = config.get('db', 'password')
    database = config.get('db', 'database')
    table    = config.get('db', 'table')
except ConfigParser.Error, e:
    print "Failed to parse %s - %d: %s\n" % (config_file, e.args[0], e.args[1])
    sys.exit(1)

#--------------------------------------------------------------------------------
# create connection to the weather db
#--------------------------------------------------------------------------------
weather_db     = create_conn(host, username, password, database)
weather_cursor = weather_db.cursor()

#--------------------------------------------------------------------------------
# download QQ's website page, then parse out city ids and names
#--------------------------------------------------------------------------------
city_dict = dict()

city_soup = download_page(base_url, initial_path)
city_tags = city_soup('option')

for city_tag in city_tags:

    id   = city_tag['value']
    name = city_tag.contents[0].string

    city_dict[id] = name

#--------------------------------------------------------------------------------
# truncate the tables
#--------------------------------------------------------------------------------
cleanup_data(weather_cursor, table)

#--------------------------------------------------------------------------------
# download QQ's website page based on one city, then parse out data and insert
# it into the tables
#--------------------------------------------------------------------------------
for city_id, city_name in city_dict.items():

    path = path_prefix + city_id + path_suffix

    soup = download_page(base_url, path)
    weather_tags = soup('table', width="432", bgcolor="#d5e1ef")
    #print weather_tags[0].contents

    overview_weather = str(weather_tags[0].contents[3].contents[1].contents[1].string).strip()
    #print overview_weather
    temprature_weather = str(weather_tags[0].contents[3].contents[1].contents[2].string).strip()
    #print temprature_weather

    spec_weather = ''
    for content in weather_tags[0].contents[3].contents[3].contents[:-2]:
        if len(str(content).strip()) > 0:
            content_string = str(content).strip()
            spec_weather += content_string
    #print spec_weather

    add_data(weather_cursor, table, city_id, city_name, overview_weather, temprature_weather, spec_weather)