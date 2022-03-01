import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy import MetaData
from datetime import datetime
import psycopg2
import sqlite3

def log(logfile, message):
    timestamp_format = '%H:%M:%S-%h-%d-%Y'
    #Hour-Minute-Second-MonthName-Day-Year
    now = datetime.now() # get current timestamp
    timestamp = now.strftime(timestamp_format)
    with open(logfile,"a") as f: 
        f.write('[' + timestamp + ']: ' + message + '\n')
        print(message)

def transform():

    log(logfile, "-------------------------------------------------------------")
    log(logfile, "Inicia Fase De Transformacion")
    
    df_customer = pd.read_sql_query("""SELECT Id AS CustomerID, ContactName AS CustomerName, City, Country, Region  FROM Customer;
        """, con=engine.connect())
    df_employee = pd.read_sql_query("""SELECT Id AS EmployeeID, LastName AS Name, City, Country,Region,HireDate as hiredate FROM Employee;
        """, con=engine.connect())
    df_Order = pd.read_sql_query("""SELECT OrderDateFROM 'Order';
        """, con=engine.connect())
    df_product = pd.read_sql_query("""SELECT id AS ProductID, ProductName, CategoryName AS categoryName
               FROM ProductDetails_V;
        """, con=engine.connect())
    df_artists = pd.read_sql_query("""SELECT od.ProductID, o.EmployeeID, o.CustomerID, o.OrderDate AS orderDate,
                                        o.Id AS orderID, od.quantity AS Quantity, od.unitPrice,
                                        od.quantity * od.unitPrice as total        
                                      FROM 'Order' o, OrderDetail od
                                      where o.ID = od.OrderId;
        """, con=engine.connect())
    log(logfile, "Finaliza Fase De Transformacion")
    log(logfile, "-------------------------------------------------------------")
    return df_customer, df_employee, df_Order, df_product, df_artists
   
def load():
    conn_string=sqlite3.connect('Northwind_large.sqlite')
    db = create_engine(conn_string)
    conn = db.connect()
    try:
        log(logfile, "-------------------------------------------------------------")
        log(logfile, "Inicia Fase De Carga")
        df_customer.to_sql('dim_customer', conn, if_exists='append',index=False)
        df_employee.to_sql('dim_employee', conn, if_exists='append',index=False)
        df_Order.to_sql('dim_order', conn, if_exists='append',index=False)
        df_product.to_sql('dim_product', conn, if_exists='append',index=False)
        df_artists.to_sql('fact_sales', conn, if_exists='append',index=False)
        conn = psycopg2.connect(conn_string)
        conn.autocommit = True
        cursor = conn.cursor()
        log(logfile, "Finaliza Fase De Carga")
        log(logfile, "-------------------------------------------------------------")
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally: 
        if conn is not None:
            conn.close()
            print('Database connection closed.')

def extract():
    log(logfile, "--------------------------------------------------------")
    log(logfile, "Inicia Fase De Extraccion")
    metadata = MetaData()
    metadata.create_all(engine)
    log(logfile, "Finaliza Fase De Extraccion")
    log(logfile, "--------------------------------------------------------")


if __name__ == '__main__':
    
    logfile = "ProyectoETL_logfile.txt"
    log(logfile, "ETL Trabajo iniciado.")
    engine = create_engine('sqlite:///DW_Northwind4.db')
    extract()
    (df_customer, df_employee, df_Order, df_product, df_artists) = transform()
    load()
    log(logfile, "ETL Trabajo finalizado.")