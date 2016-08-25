#!/usr/bin/python
import os
import sys 
sys.path.append("python-dateutil-1.5")
import getopt
import urllib2
import json 
import hashlib
from dateutil.parser import *
from dateutil.tz import *
from datetime import *
import time as t
import hashlib
import hmac 
import xml.etree.ElementTree as ET
import rfc3339
import dateutil
import pytz

CW_HEALTH_LOG="logs/health.txt"
CW_NOTIFICATION_FEED_DEV="test_feed.xml" 
CW_NOTIFICATION_FEED_PROD="http://www.cwtv.com/feed/mobileapp/notifications/schedule/?api_version=3" 
CW_NOTIFICATION_FEED=CW_NOTIFICATION_FEED_DEV

PROD_APPS = [{"device" : "android", "id": "65", "access_key" : "ae0eb93279506cb0e9bc35c3a91d8e52c8bf7ae0", "signature_key" : "71d3b440cdd6016b6a4ddf11e42190ad61a4b458"}, {"device" : "ios", "id": "66", "access_key" : "fc8106a95c311df193bb6230161345be8ace12f7", "signature_key" : "4a3c71d3ed3d63a4f592c808e58470d20d676d9e"}]
DEV_APPS = [{"device" : "android", "id":  "67", "access_key" : "002e6b80c9a172338967622088a79feaa6f42426", "signature_key" : "df192c33dc1ce7059b2d0dd8d45ee59bd8d725dd"}, {"device" : "ios", "id": "68", "access_key" : "33d3a83bd1a733af1b7244a6581ac69b42d33414", "signature_key" : "b5887f81c3445474112d311e1618c5817b2ac4c7"}]


MAAS_PUSH_ENDPOINT="http://alerts-api.phunware.com/v2.0/events"
MAAS_PUSH_SUBSCRIPTIONS_LIST="http://push-prod.s3.amazonaws.com/%s/config/subscriptions.json"
NOTIFICATION_HISTORY_LOCATION="notification_history.json"
VERBOSE = False
FIRST_RUN = True
APPS = DEV_APPS
APP_ENVIRONMENT="dev"
TEST_MODE=False
notificationsGenerated = 0

def log(message):
	global VERBOSE
	if VERBOSE:
		print message

def main(argv):  
	global FIRST_RUN
	global APPS
	global notificationsGenerated
	global VERBOSE
	global TEST_MODE
	global CW_NOTIFICATION_FEED
	global APP_ENVIRONMENT
	global APPS
	ARG_SHOULD_USE_FEED=False
	shouldCleanRun=False

	try:
		opts, args = getopt.getopt(argv,"g,p,v,t,e:")
	except getopt.GetoptError:
		print "Usage:  feedmonster.py"
		print "\t-g\t\tThis switch should only be used if you want to process against the real feed."
		print "\t-p\t\tPurge all notification history before parsing new notifications from CW XML feed"
		print "\t-t\t\tTest run mode.  Will parse notification feed, but never generate any notifications.  These notifications will not be added to the history."
		print "\t-v\t\tEnable verbose logging."
		print "\t-e [prod/dev]\t\tSpecify which subset of MaaS applications to use for this set of pushes."
		sys.exit(2)   
	for opt, arg in opts:
		if opt == '-p':
			log("PURGE")
			shouldCleanRun=True
		elif opt in ('-v'):
			VERBOSE = True
			log("Verbose mode enabled.")
		elif opt == "-g":
			print "Using production notification feed."
			ARG_SHOULD_USE_FEED=True
			CW_NOTIFICATION_FEED=CW_NOTIFICATION_FEED_PROD
		elif opt == "-t":
			print "Test mode enabled"
			TEST_MODE=True
		elif opt == "-e":
			APP_ENVIRONMENT=arg

			if APP_ENVIRONMENT not in ["dev", "prod"]:
				print "Error: Unknown environment. (%s)  Please use \"dev\" or \"prod\"." % APP_ENVIRONMENT
				sys.exit(2)

			if APP_ENVIRONMENT == "dev":
				APPS=DEV_APPS
			else:
				APPS=PROD_APPS

	try:
		os.makedirs(os.path.dirname(CW_HEALTH_LOG))
	except Exception, e2:
		# exception is raised if directory already exists.
		pass

	# read in static storage of last sync information
	notificationHistory = {}
	if not shouldCleanRun:
		try: 
			notificationHistoryFileStream=open(NOTIFICATION_HISTORY_LOCATION)
			
			try:
				notificationHistory = json.loads(notificationHistoryFileStream.read())
				FIRST_RUN = False
			except Exception, e2:
				print "Unable to parse notification history (%s). Most likely an empty json file. Starting over..." % (NOTIFICATION_HISTORY_LOCATION)
				print e2

		except Exception, e:
			print "Warning: No previous notification history found (%s).  Starting from scratch."  % (NOTIFICATION_HISTORY_LOCATION)
	 

		if FIRST_RUN:
			print "This is the first run of feed processor.  No pushes will be generated."
	else:
		log("Is: %s. Will ignore notificationHistoryFileStream" % shouldCleanRun)
		FIRST_RUN=False
	

	# get all of the known segments for each of the apps
	print "Downloading segments for %s MaaS applications..." % APP_ENVIRONMENT
	for app in APPS:
		app["segments"] = {}
		try: 
			subscriptionsUrl=MAAS_PUSH_SUBSCRIPTIONS_LIST % (app["id"])
			log("Trying "+subscriptionsUrl)

			segmentsRaw = urllib2.urlopen(subscriptionsUrl).read()

			try:
				app["segments"] = json.loads(segmentsRaw)["subscriptionGroups"]
 
			except Exception, e2:
				print "Unable to parse segments from MaaS.  Aborting."
				print e2
				sys.exit(2)
		except Exception, e:
			print "Error: Unable to fetch MaaS segments feed (%s)." % (subscriptionsUrl)
			print e
			fatalError()  

	# retrieve CW feed xml
	if ARG_SHOULD_USE_FEED:
		try: 
			socket = urllib2.urlopen(CW_NOTIFICATION_FEED)

			xmlTree = ET.parse(socket)
		except Exception, e:
			print "Error: Unable to fetch CW notification feed. (%s)" % CW_NOTIFICATION_FEED
			print e
			fatalError()   

		log("Finished loading XML from "+CW_NOTIFICATION_FEED)

		
	else:
		print "Parsing " + CW_NOTIFICATION_FEED + " for test notifications instead of remote feed..."
		xmlTree = ET.parse(open(CW_NOTIFICATION_FEED))

	xmlRoot = xmlTree.getroot() 

	# for each item in items/ look for a timestamp that has changed since the last time we ran the script
	
	for item in xmlRoot:
		cwItemTitle=item.find('title').text
		cwItemDescription=item.find('desc').text
		cwItemTimestamp=item.find('timestamp').text
		cwItemLink=item.find('link').text


		log("Checking "+cwItemTitle)

		# some shows (same slug name) can air multiple times (per day or per week) and as such can show up in the feed multiple times.
		# to make sure we can track these as unique entries, combine their slug (title) with their description and the timestamp
		cwItemIdentifier = cwItemTimestamp
		#cwItemIdentifier = hashlib.sha224(cwItemIdentifier).hexdigest()

		if cwItemIdentifier not in notificationHistory and not FIRST_RUN:
			iso_time = parse(cwItemTimestamp)

			# python stdlib hates timezones...so strip it out
			iso_time =  iso_time.replace(tzinfo=None)
		
			if iso_time > datetime.now():
				print "%s does not exist in notification history and should likely result in a push to segment {%s} with a description {%s}" % (cwItemIdentifier, cwItemTitle, cwItemDescription)
				
				generateMaaSNotification(cwItemTitle, cwItemDescription, cwItemLink, cwItemTimestamp)
			else:
				log("New item found in feed (%s), but its timestamp (%s) is older than right now. " % (cwItemTitle, cwItemTimestamp))

		cwShow = {"timestamp" : cwItemTimestamp, "title" : cwItemTitle, "desc" : cwItemDescription, "link" : cwItemLink}

		if not TEST_MODE:
			notificationHistory[cwItemIdentifier] = cwShow
		else:
			print "[Not adding to notification history due to test mode.]"

	# Prune history of notifications that are older than a certain threshold (1 day)
	print "Pruning notification history..."
	yesterday = date.today() - timedelta(1)
	yesterdayMidnight = datetime.combine(yesterday, time(0, 0))

	historyKeys = notificationHistory.keys()
	for historyKey in historyKeys: 
		historyItem = notificationHistory[historyKey]
		# Thu, 05 Sep 2013 20:00:00 -0800 
		iso_time = parse(historyItem["timestamp"])

		# python stdlib hates timezones...so strip it out
		iso_time =  iso_time.replace(tzinfo=None)

		if yesterdayMidnight > iso_time:
			log("Should prune "+historyItem["timestamp"] + " "+historyKey)
			del notificationHistory[historyKey]


	print "\n%d notifications generated to MaaS." % (notificationsGenerated)

	# write history to disk
	data_string = json.dumps(notificationHistory)

	try:   
		fout = open(NOTIFICATION_HISTORY_LOCATION, "w+")
		fout.write(data_string)
		fout.close()
		print "\nHistory written to ", NOTIFICATION_HISTORY_LOCATION

		try:
			# try writing health status to health log for nagios
			fout = open(CW_HEALTH_LOG, "w+")
			fout.write("OK")
			fout.close()
		except Exception, e:
			print "\nERROR: Unable to write health status to "+CW_HEALTH_LOG
			sys.exit(1)
	except Exception, e:
		print "\nUnable to write history to ",NOTIFICATION_HISTORY_LOCATION
		fatalError()

def fatalError():
	try:
		# try writing health status to health log for nagios
		fout = open(CW_HEALTH_LOG, "w+")
		fout.write("FAIL")
		fout.close()
	except Exception, e:
		print "\nERROR: Unable to write health status to "+CW_HEALTH_LOG
	sys.exit(1)


def generateMaaSNotification(itemTitle, itemDescription, itemLink, itemTimestamp):
	global APPS
	global notificationsGenerated 

	# YYYY-MM-DDThh:mm:ssZ
	#1996-12-19T16:39:57-08:00
	iso_date = dateutil.parser.parse(itemTimestamp)
	utc_date = iso_date.astimezone(pytz.UTC)
	maasTime = rfc3339.format(utc_date, utc=True) 
	print unicode(dateutil.parser.parse(itemTimestamp)) + " => "+maasTime

	# retrieve list of maas segments for each app
	for app in APPS:

		# find a MaaS segment that matches itemTitle
		destinationSegmentId = None
		for segment in app["segments"]: 
			if segment["name"] == itemTitle:
				log("Found segment id (%s) that matches itemTitle (%s)" % (segment["id"], itemTitle))
				destinationSegmentId = segment["id"]
				break

		if destinationSegmentId:
			data = {"pushType" : "segment", "segments" : [destinationSegmentId], "message" : itemDescription, "createdBy" : "cwfeedprocessor@phunware.com", "attributes" : {"title" : itemTitle, "description" : itemDescription, "url" : itemLink, "timestamp" : "now"}, "start" : maasTime}
			xAuthString = buildXAuth("POST", app["access_key"], app["signature_key"], data)
			request = urllib2.Request(MAAS_PUSH_ENDPOINT, json.dumps(data))
			request.add_header("X-Auth", xAuthString)
			try:
				log("attempting request... "+json.dumps(data))
				
				if not TEST_MODE:
					tempFile = urllib2.urlopen(request)
				else:
					print "[API call to MaaS suppressed for test mode.]"
				notificationsGenerated = notificationsGenerated + 1
			except urllib2.HTTPError, e: 
				#stuff
				print e
		else:
			log("Could not find matching segment id for (%s)" % (itemTitle))
 

def buildXAuth(method, accessKey, signatureKey, data):
	timestamp = str(int(t.time()))
	signatureString =  method + "&" + accessKey + "&" + timestamp + "&" +  json.dumps(data)
	hash =  hmac.new(signatureKey, signatureString, hashlib.sha256).hexdigest()
	return accessKey + ':' + timestamp  + ':' + hash

if __name__ == "__main__":
   main(sys.argv[1:])