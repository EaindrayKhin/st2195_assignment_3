import sqlite3
import pandas as pd

conn = sqlite3.connect ('airline2.db')
airports = pd.read_csv("airports.csv")
airports.to_sql('Airports', con=conn, index=False)

carriers = pd.read_csv("carriers.csv")
carriers.to_sql('Carriers', con=conn, index=False)

planes = pd.read_csv("plane-data.csv")
planes.to_sql('Planes', con=conn, index=False)

ontime = pd.read_csv("2000.csv")
ontime.to_sql('Ontime', con=conn, index=False)

ontime = pd.read_csv("2001.csv")
ontime.to_sql('Ontime', con=conn, if_exists='append',index=False)

ontime = pd.read_csv("2002.csv")
ontime.to_sql('Ontime', con=conn,if_exists='append', index=False)

ontime = pd.read_csv("2003.csv")
ontime.to_sql('Ontime', con=conn, if_exists='append', index=False)

ontime = pd.read_csv("2004.csv")
ontime.to_sql('Ontime', con=conn,if_exists='append', index=False)

ontime = pd.read_csv("2005.csv")
ontime.to_sql('Ontime', con=conn, if_exists='append',index=False)

c = conn.cursor()

#which plane model has the lowest associated average depature delay
#excluding cancelled and diverted flights

q1 = c.execute('''
                SELECT model AS model, AVG(Ontime.DepDelay) AS Avg_Delay
                FROM Planes
                JOIN Ontime
			   USING (tailnum)
                WHERE Cancelled = 0
                AND Diverted = 0
                AND DepDelay > 0
                GROUP BY model
                ORDER BY Avg_Delay
                ''').fetchall()
 
print(q1[0], "has the lowest associated average depature delay")               

q1 = pd.DataFrame(q1)
q1.columns = ["model", "Average_Delay"]
q1.to_csv('sql_q1_output.csv', index=False)               

#which city has the highest number of inbound flights
#excluded cancelled flights              
q2 = c.execute('''
               SELECT Airports.city AS city, COUNT(*) AS total
               FROM Airports 
			   JOIN Ontime 
			   On Ontime.Dest = Airports.iata
               WHERE Cancelled = 0
               GROUP BY Airports.city
               ORDER BY total DESC
               ''').fetchall()

print(q2[0], " has the highest number of inbound flights")
q2 = pd.DataFrame(q2)
q2.columns = ["city", "Total_Flights"]
q2.to_csv('sql_q2_output.csv', index=False)  

               
#which carrier has the highest number of cancelled flights
q3 = c.execute('''
               SELECT Ontime.UniqueCarrier, Carriers.Description, SUM(Cancelled) AS total_cancelled
               FROM Ontime 
			   JOIN Carriers
			   On Ontime.UniqueCarrier = Carriers.Code
               GROUP BY Carriers.Description
               ORDER BY total_cancelled DESC
               ''').fetchall()

print(q3[0], "has the highest number of cancelled flights")
q3 = pd.DataFrame(q3)
q3.columns = ["Description", "Carriers", "Total_Cancelled"]
q3.to_csv('sql_q3_output.csv', index=False)  

#which carrier has the highest number of cancelled flights
#relative to their number of total flights
q4 = c.execute('''
               SELECT UniqueCarrier, Carriers.Description, COUNT(UniqueCarrier) AS total_flights, SUM(Cancelled) AS total_cancelled_flights,
               (CAST(SUM(Cancelled) AS FLOAT)/CAST(COUNT(UniqueCarrier) AS FLOAT)) AS ratio
               FROM Ontime
               JOIN Carriers
               ON Ontime.UniqueCarrier = Carriers.Code
               GROUP BY UniqueCarrier
               ORDER BY ratio DESC
               ''').fetchall()
               
print(q4[0], "has the highest number of cancelled flights, relative to their number of total flights")
q4 = pd.DataFrame(q4)
q4.columns = ["Carrier", "Description", "Total_Flights", "Total_Cancelled", "Ratio"]
q4.to_csv('sql_q4_output.csv', index=False)
              
               