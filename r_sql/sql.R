#install.packages("RSQLite")
library(DBI)
library(dplyr)

conn <- dbConnect(RSQLite::SQLite(), "airline2.db")

airports <- read.csv("~/st2195_assignment_3/r_sql/airports.csv", header = TRUE)
dbWriteTable(conn, "airports", airports)

carriers <- read.csv("~/st2195_assignment_3/r_sql/carriers.csv", header = TRUE)
dbWriteTable(conn, "carriers", carriers)

planes <- read.csv("~/st2195_assignment_3/r_sql/plane-data.csv", header = TRUE)
dbWriteTable(conn, "planes", planes)


ontime_2000 <- read.csv("~/st2195_assignment_3/r_sql/2000.csv", header = TRUE)
ontime_2001 <- read.csv("~/st2195_assignment_3/r_sql/2001.csv", header = TRUE)
ontime_2002 <- read.csv("~/st2195_assignment_3/r_sql/2002.csv", header = TRUE)
ontime_2003 <- read.csv("~/st2195_assignment_3/r_sql/2003.csv", header = TRUE)
ontime_2004 <- read.csv("~/st2195_assignment_3/r_sql/2004.csv", header = TRUE)
ontime_2005 <- read.csv("~/st2195_assignment_3/r_sql/2005.csv", header = TRUE)

#combine all the ontime tables
ontime = rbind(ontime_2000, ontime_2001, ontime_2002, ontime_2003, ontime_2004, ontime_2005)

dbWriteTable(conn, "ontime", ontime)
#remove temporary dataframes
rm(ontime_2000, ontime_2001, ontime_2002, ontime_2003, ontime_2004, ontime_2005)

#garbage collection
gc()
dbListTables(conn)
dbListFields(conn, "ontime")
#which plane model has the lowest associated average depature delay
#excluding cancelled and diverted flights
q1 <- dbGetQuery(conn,
                 "SELECT model AS model, AVG(DepDelay) AS Avg_Delay
               FROM Planes
               JOIN Ontime
			         USING (tailnum)
               WHERE Cancelled = 0
               AND Diverted = 0
               AND DepDelay > 0
               GROUP BY model
               ORDER BY Avg_Delay")

print(paste(q1[1, "model"], "has the lowest associated average depature delay,
            excluding cancelled and diverted flights"))
write.csv(as.data.frame(q1), "sql_q1_output.csv", row.names = FALSE)

#which city has the highest number of inbound flights
#excluded cancelled flights
q2 <- dbGetQuery(conn,
                  "SELECT Airports.city AS city, COUNT(*) AS total
               FROM Airports 
			         JOIN Ontime 
			         On Ontime.Dest = Airports.iata
               WHERE Cancelled = 0
               GROUP BY airports.city
               ORDER BY total")

print(paste(q2[1, "city"], "has the highest number of inbounf flights,
            excluding cancelled flights"))
write.csv(as.data.frame(q2), "sql_q2_output.csv", row.names = FALSE)

#which carrier has the highest number of cancelled flights
q3 <- dbGetQuery(conn,
                 "SELECT Carriers.Description AS carrier, COUNT(*) AS total 
               FROM Carriers 
               JOIN Ontime 
               On Ontime.UniqueCarrier = Carriers.Code
               WHERE Ontime.Cancelled = 1
               GROUP BY Carriers.Description
               ORDER BY total")

print(paste(q3[1, "carrier"], "has the highest number of cancelled flights"))
write.csv(as.data.frame(q3), "sql_q3_output.csv", row.names = FALSE)

#which carrier has the highest number of cancelled flights
#relative to their number of total flights
q4 <- dbGetQuery(conn,
                 "SELECT a1.Carrier AS carrier, 
                 (CAST(a1.numerator AS FLOAT)/CAST(a2.denominator AS FLOAT)) AS ratio
                 FROM
                 (SELECT Carriers.Description AS carrier, COUNT(*) AS numerator
              FROM Carriers
              JOIN Ontime
              On Ontime.UniqueCarrier = Carriers.Code
              WHERE Ontime.Cancelled = 1
              GROUP BY Carriers.Description)
              AS a1 JOIN 
              (SELECT Carriers.Description AS carrier, COUNT(*) AS denominator
              FROM Carriers 
              JOIN ontime
              On Ontime.UniqueCarrier = Carriers.Code
              GROUP BY Carriers.Description)
              AS a2 USING (carrier)
              ORDER BY ratio")

print(paste(q4[1, "carrier"], "has the highest number of cancelled flights,
            relative to their number of total flights"))
write.csv(as.data.frame(q4), "sql_q4_output.csv", row.names = FALSE)
