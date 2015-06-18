CREATE TABLE station (
	stationid   SMALLINT UNSIGNED NOT NULL,
	stationname VARCHAR(40),
	latitude    FLOAT,
	longitude   FLOAT,
	street      VARCHAR(40),
	city        VARCHAR(40),
	state       CHAR(2),
	zip         CHAR(5),
	totaldocks  TINYINT UNSIGNED,
	CONSTRAINT pk PRIMARY KEY (stationid)
); 

CREATE TABLE stationstatus (
	stationid  SMALLINT UNSIGNED NOT NULL,
	status     VARCHAR(20),
	availbikes TINYINT UNSIGNED,
	availdocks TINYINT UNSIGNED,
	statustime DATETIME,
	CONSTRAINT pk PRIMARY KEY (stationid, statustime),
	CONSTRAINT fk FOREIGN KEY (stationid) REFERENCES station (stationid) 
	ON UPDATE CASCADE ON DELETE CASCADE
);