SELECT 'stationid', 'stationname', 'latitude', 'longitude', 'street',
	   'city', 'state', 'zip', 'totaldocks', 'status', 'availbikes',
       'availdocks', 'statustime'
UNION ALL
SELECT * FROM merged
INTO OUTFILE '/Users/elundquist/Repositories/bikeshare/data/merged.csv'
FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\n';