SELECT a.stationid, a.stationname, b.statustime, b.availbikes, b.availdocks
FROM station AS a INNER JOIN stationstatus AS b
ON a.stationid = b.stationid
WHERE b.availbikes = 0 AND b.status = "Active"
ORDER BY stationid, statustime;

SELECT statustime, COUNT(DISTINCT stationid) AS zerobikes
FROM stationstatus
WHERE availbikes = 0
GROUP BY statustime
ORDER BY statustime;

SELECT statustime, COUNT(DISTINCT stationid) AS zerodocks
FROM stationstatus
WHERE availdocks = 0 and status = 'Active'
GROUP BY statustime
ORDER BY statustime;



