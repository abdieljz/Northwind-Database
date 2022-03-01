import sqlite3
import pandas
import csv
import re
    
def connect():
    conn=sqlite3.connect("lite.db")
    cur=conn.cursor()
    cur.execute("CREATE TABLE IF NOT EXISTS calculation (ID INTEGER PRIMARY KEY,Station_ID INTEGER,Virtual_Variable TEXT,\
                    Variable_ID INTEGER,Unit_ID INTEGER,Formula TEXT,RoC_Active INTEGER,RoC_Precision INTEGER,\
                    RoC_Period_Value INTEGER,RoC_Period_Type INTEGER,RoC_Unit_Value INTEGER,RoC_Unit_Type INTEGER,\
                    Datum_Variable_ID INTEGER,Datum_Timestamp TEXT,Datum_Information TEXT,Constants TEXT)")
    conn.commit()
    conn.close()

    
def insert(Station_ID,Virtual_Variable,Variable_ID,Unit_ID,Formula):
    conn=sqlite3.connect("lite.db")
    cur=conn.cursor()
    cur.execute("INSERT INTO calculation VALUES(NULL,?,?,?,?,?,0,0,1,3600,1,3600,0,\"0000-00-00 00:00:00\",0,\"\")",(Station_ID,Virtual_Variable,Variable_ID,Unit_ID,Formula))
    conn.commit()
    conn.close()
    
def export():
    conn = sqlite3.connect("lite.db")
    cur=conn.cursor()
    cur.execute("SELECT * FROM calculation")
    rows=cur.fetchall()
    csv_path = "output.csv"
    with open(csv_path, "w", newline="") as csv_file:
        csv_writer = csv.writer(csv_file,delimiter=";")
        # Write headers.
        csv_writer.writerow([i[0] for i in cur.description])
        # Write data.
        csv_writer.writerows(rows)