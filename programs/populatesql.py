# Process downloaded JSON data for the Indego PhillyBikeShare program and insert it into a MySQL database
#--------------------------------------------------------------------------------------------------------

import os, shutil, time, tarfile, json, mysql.connector
DATAFOLDER = '/Users/elundquist/Repositories/bikeshare/data' 
DBSETTINGS = dict(user='python', password='r3dsox', host='127.0.0.1', database='bikeshare')

def main():
    '''main function to loop through archived data files, extract all of the JSON data files from them 
       one at a time, parse the JSON data into a dictionary, and insert into a MySQL database'''

    for item in os.listdir(DATAFOLDER):
        fullpath = os.path.join(DATAFOLDER, item)
        if (os.path.isfile(fullpath)) and (fullpath.endswith('.tar.gz')):
            print('\n\nattempting to process archive file: [{0}]'.format(item))
            try:

                tempdir = os.path.join(DATAFOLDER,'tempdata')
                if os.path.isdir(tempdir): shutil.rmtree(tempdir)

                os.mkdir(tempdir)
                tar = tarfile.open(fullpath, "r")
                tar.extractall(tempdir)

                for dfile in os.listdir(tempdir):
                    print('\n\nattempting to process datafile: {0}'.format(dfile))
                    dpath = os.path.join(tempdir, dfile)
                    parsejson(dpath)
                    os.remove(dpath)

            except OSError:
                print('there was a problem creating the temporary data directory')
            except tarfile.TarError:
                print('there was a problem extracting the data files from the archive')
            finally:
                if os.path.isdir(tempdir): shutil.rmtree(tempdir)


def parsejson(dfile):
    '''open a datafile, parse the JSON, and save all relevant data fields into a dictionary (sinfo) including
       the timestamp of the datafile (to the minute) for each station in the datafile. once each station's 
       dictionary has been built, pass it to insertmysql() to actually insert the data into the database'''

    timestamp = time.strptime(os.path.split(dfile)[1][15:-5], '%Y-%m-%d-%H-%M-%S')
    timestamp = time.strftime('%Y-%m-%d %H:%M:00', timestamp)

    try:

        fh = open(dfile)
        jdata = json.load(fh)

        for station in jdata['features']:

            sinfo = {}
            sinfo['long']   = station['geometry']['coordinates'][0]
            sinfo['lat']    = station['geometry']['coordinates'][1]
            sinfo['street'] = station['properties']['addressStreet']
            sinfo['city']   = station['properties']['addressCity']
            sinfo['state']  = station['properties']['addressState']
            sinfo['zip']    = station['properties']['addressZipCode']
            sinfo['abikes'] = station['properties']['bikesAvailable']
            sinfo['adocks'] = station['properties']['docksAvailable']
            sinfo['id']     = station['properties']['kioskId']
            sinfo['status'] = station['properties']['kioskPublicStatus']
            sinfo['name']   = station['properties']['name']
            sinfo['tdocks'] = station['properties']['totalDocks']
            sinfo['time']   = timestamp

            print('successfully parsed JSON data for station [{id}: {name}]'.format(**sinfo))
            insertmysql(sinfo)
        
    except IOError:
        print('could not open JSON datafile')
    except ValueError:
        print('could not decode JSON')
    finally:
        if fh: fh.close()


def insertmysql(sinfo):
    '''using a dictionary of station-specific data fields insert them into the two MySQL tables created to store 
       data from the bikeshare program. there's a station-level table (station) and a station/time level table 
       (stationstatus). the ON DUPLICATE KEY UPDATE clause allows this program to be run multiple times with the 
       same underlying data to be inserted without causing MySQL errors'''

    try:
        cnx = mysql.connector.connect(**DBSETTINGS)
        cursor = cnx.cursor()

        insertstation = ("INSERT INTO station SET "
                         "stationid   = %(id)s, "
                         "stationname = %(name)s, "
                         "latitude    = %(lat)s, "
                         "longitude   = %(long)s, "
                         "street      = %(street)s, "
                         "city        = %(city)s, "
                         "state       = %(state)s, "
                         "zip         = %(zip)s, "
                         "totaldocks  = %(tdocks)s "
                         "ON DUPLICATE KEY UPDATE stationid = stationid;")

        insertstatus = ("INSERT INTO stationstatus SET "
                        "stationid   = %(id)s, "
                        "status      = %(status)s, "
                        "availbikes  = %(abikes)s, "
                        "availdocks  = %(adocks)s, "
                        "statustime  = %(time)s "
                        "ON DUPLICATE KEY UPDATE stationid = stationid, statustime = statustime;")

        cursor.execute(insertstation, sinfo)
        print('successfully added station-level records for for station [{id}: {name}]'.format(**sinfo))

        cursor.execute(insertstatus, sinfo)
        print('successfully added station/time-level records for for station [{id}: {name}]\n'.format(**sinfo))

        cnx.commit()
        cursor.close()
        cnx.close()

    except mysql.connector.Error as err:
        if   err.errno == mysql.connector.errorcode.ER_HOSTNAME:
            print('could not connect to the hostname in the supplied credentials')
        elif err.errno == mysql.connector.errorcode.ER_ACCESS_DENIED_ERROR:
            print('access was denied to the database using the supplied credentials')
        elif err.errno == mysql.connector.errorcode.ER_BAD_DB_ERROR:
            print('could not connect to the database in the supplied credentials')
        else:
            print(err)

main()

