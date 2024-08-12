-- Drop tables
DROP TABLE episodes;
DROP TABLE episodeIV;

-- Activity 1:
-- Creating the table
CREATE TABLE episodes (name STRING, line STRING) 
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' TBLPROPERTIES ("skip.header.line.count"="2");

-- Load all 3 text files into the table
LOAD DATA LOCAL INPATH '/root/inputs/episodeIV_dialogues.txt' INTO TABLE episodes;
LOAD DATA LOCAL INPATH '/root/inputs/episodeV_dialogues.txt' INTO TABLE episodes;
LOAD DATA LOCAL INPATH '/root/inputs/episodeVI_dialogues.txt' INTO TABLE episodes;

-- Generate output and export it
INSERT OVERWRITE LOCAL DIRECTORY '/root/HiveOutput1' ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
SELECT name, COUNT(no_of_lines) AS no_of_lines from episodes GROUP BY name ORDER BY no_of_lines DESC;


-- Activity 2:
-- For Episode IV
CREATE TABLE episodeIV (name STRING, line STRING) 
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' TBLPROPERTIES ("skip.header.line.count"="2");
LOAD DATA LOCAL INPATH '/root/inputs/episodeIV_dialogues.txt' INTO TABLE episodeIV;

INSERT OVERWRITE LOCAL DIRECTORY '/root/HiveOutput2'
SELECT COUNT(*) FROM episodeIV WHERE INSTR(line, 'Luke')>0;
-- SELECT COUNT(*) FROM episodeIV WHERE line LIKE '%Luke%';
-- Output: 56