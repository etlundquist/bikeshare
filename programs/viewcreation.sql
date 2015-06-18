CREATE VIEW merged AS
SELECT a.stationid, a.stationname, a.latitude, a.longitude, a.street,
	   a.city, a.state, a.zip, a.totaldocks, b.status, b.availbikes, 
	   b.availdocks, b.statustime
FROM station AS a INNER JOIN stationstatus AS b
ON a.stationid = b.stationid;

CREATE VIEW summary AS
SELECT statustime,
	   SUM(status="Active") AS active_stations,
	   SUM(availbikes)      AS tot_availbikes,
	   SUM(availdocks)      AS tot_availdocks,
	   AVG(availbikes)      AS avg_availbikes,
       AVG(availdocks)      AS avg_availdocks
FROM stationstatus
GROUP BY statustime;
