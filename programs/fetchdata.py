# Download JSON data for the Indego PhillyBikeShare program on fixed intervals and archive the data files into date-specific folders
#-----------------------------------------------------------------------------------------------------------------------------------

import sys, os, datetime, time, requests, tarfile
DATAFOLDER = '/Users/elundquist/Repositories/bikeshare/data' # ROOT FOLDER WHERE ALL DATA FILES WILL BE STORED
START_TIME = '2015-06-17 00:00:00'                           # FIRST TIME DATA WILL BE PULLED
CLOSE_TIME = '2015-06-18 00:00:00'                           # FINAL TIME DATA WILL BE PULLED
INTERVAL   = 15 * 60                                         # INTERVAL (IN SECONDS) BETWEEN DATA PULLS
DATAURL    = 'https://api.phila.gov/bike-share-stations/v1'  # URL OF BIKESHARE DATA API & FAKE USER AGENT TO PRESENT TO WEBSITE
USERAGENT  = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2227.1 Safari/537.36'
HEADERS    = {'User-Agent': USERAGENT}                       # PASS THIS DICTIONARY TO REQUESTS TO FORMAT CONTENTS AS HTTP HEADERS

def main():
	'''main function to control program flow. the program will wait until the user-specified start time, and then begin
	   to download the bikeshare JSON data from its API handler. the program will keep downloading the data at regular
	   intervals until the end time is reached'''

	beg_dt = datetime.datetime.strptime(START_TIME, '%Y-%m-%d %H:%M:%S')
	end_dt = datetime.datetime.strptime(CLOSE_TIME, '%Y-%m-%d %H:%M:%S')
	print('Waiting to begin iterative data downloads until the specified start time: [{0}]'.format(START_TIME))

	while True:

		curtime = datetime.datetime.now()
		fromstr = beg_dt - curtime

		if (fromstr.days == 0) and (fromstr.seconds == 0) and (0 < fromstr.microseconds < 9999):
			print('Beginning iterative data downloads now...\n')
			break

	while True:

		curtime = datetime.datetime.now()
		fromend = end_dt - curtime

		if (fromend.days < 0):
			print('\nEnding iterative data downloads now...')
			break
		else:
			download()
			itime = datetime.datetime.now() - curtime
			time.sleep(INTERVAL - (itime.seconds + itime.microseconds/1e6))

	if os.listdir(DATAFOLDER):
		print('Archiving data downloads into date-specific folders')
		archive()
	else:
		print('There are no data downloads to archive - ending program execution')


def download():
	'''function to actually perform the API query and save the results to disk in a new file. the 'requests' module is used
	   to send an HTTP request to the API with headers that make it look like the request is coming from a real browser, which
	   is important because the web server denies requests coming straight from Python. after the request is made, a new file
	   is opened to hold the response data, and one line of the response is written to it at a time'''

	try:
		response = requests.get(DATAURL, headers=HEADERS)
		fname    = 'bikeshare_data_' + time.strftime('%Y-%m-%d-%H-%M-%S') + '.json'
		fh       = open(os.path.join(DATAFOLDER, fname), mode='w')

		for line in response:
			fh.write(line.decode('utf8'))
		print('datafile [{0}] downloaded successfully!'.format(fname))

	except requests.exception.RequestException:
		print('API query not successful')
	except IOError:
		print('could not open/write to file')
	finally:
		if fh: fh.close()


def archive():
	'''function to archive all of the downloaded data files into date-specific folders. this will help keep the data files
	   organized and also save disk space if downloading over a long time span and/or at frequent intervals'''

	arcdates = set()
	contents = os.listdir(DATAFOLDER)

	for item in contents:
		fullpath = os.path.join(DATAFOLDER, item)
		if os.path.isfile(fullpath) and item.endswith('.json'):
			filedate = item[15:25]
			arcdates.add(filedate)

	for arcdate in arcdates:
		try:
			tarname = 'bikeshare_archive_' + arcdate + '.tar.gz'
			tar = tarfile.open(os.path.join(DATAFOLDER, tarname), 'w:gz')
			itemcount = 0

			for item in contents:
				fullpath = os.path.join(DATAFOLDER, item)
				if os.path.isfile(fullpath) and item.endswith('.json'):
					filedate = item[15:25]
					if filedate == arcdate:
						tar.add(fullpath, arcname=item, recursive=False)
						os.remove(fullpath)
						itemcount += 1

			print('{0} datafiles collected on [{1}] successfully archived'.format(itemcount, arcdate))
		except tarfile.TarError:
			print('could not open/write to archive file')
		finally:
			if tar: tar.close()
		
main()

