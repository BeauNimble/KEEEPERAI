# Importeren van packages
from datetime import datetime
import pandas as pd
import time
import pyodbc
import sqlalchemy
import traceback


# Set up db connection
def db_connection():
    connection = pyodbc.connect("Driver={SQL Server};"
                                "Server=projects.nimble.expert;"
                                "Database=KeeperDev;"
                                "uid=sa;pwd=Admin@12345;IntegratedSecurity = false;")
    return connection

def db_engine():
    database = 'KeeperDev'
    username = 'sa'
    password = 'Admin@12345'
    server = 'projects.nimble.expert'
    # Create SQLAlchemy engine
    engine = sqlalchemy.create_engine(
        "mssql+pyodbc://" + username + ":" + password + "@" + server + "/" + database + "?driver=SQL+Server")
    return engine


# Read files
def init():
    # DB connection
    connection = db_connection()
    cursor = connection.cursor()
    # Get initial batch number
    cursor.execute('SELECT MAX(ID) FROM dbo.Batch')
    (batch,) = cursor.fetchone()
    print("Current amount of batches: ", end='')
    print(batch)
    initial = batch
    # Check every 20 seconds whether the DB has been updated
    start_time = time.time()
    seconds = 5

    # Queue of batches which arent processed yet

    while True:
        current_time = time.time()
        elapsed_time = current_time - start_time
        if elapsed_time > seconds:
            # Get queue size
            cursor.execute('SELECT COUNT(ID) FROM dbo.Batch WHERE Status is NULL')
            (qsize,) = cursor.fetchone()
            try:
                cursor.execute('SELECT MAX(ID) FROM dbo.Batch')
                (last,) = cursor.fetchone()
            except:
                timeStamp(last, qsize)
                print("[01] [Error] [DB] Error trying to get the latest batchID ")
            if last is None:
                timeStamp(last, qsize)
                print("[WARNING] [!] Seems like the db got truncated.. Please check [!]")
                last = 0
                initial = last
            elif last < initial:
                timeStamp(last, qsize)
                print("[!] Seems like the db got truncated.. Please check [!]")
                last = 0
                initial = last
            # If last batch id is higher than initial batch id
            elif last > initial:
                # Read table
                timeStamp(last, qsize)
                print("[!] Found new one! Adding to queue.. [!]")
                # Check for the first if the bot is already busy and dont add to queue
                print(" -- Amount of queued items: ", end='')
                print(qsize)
                # Set initial to the current one
                initial = last
            # If last batch is same as initial just check queue
            elif last == initial:
                timeStamp(last, qsize)
                if qsize != 0:
                    cursor = connection.cursor()
                    cursor.execute('SELECT MIN(ID) FROM dbo.batch WHERE Status is NULL')
                    (next,) = cursor.fetchone()
                    print("[>] Next to be processed: ", end='')
                    print(next)
                    try:
                        check(next, 0)
                    except Exception:
                        error = traceback.format_exc()
                        current_time = datetime.now().strftime("%d-%m-%Y %H:%M:%S")
                        errorLog(current_time, next, error)
            start_time = time.time()


def timeStamp(last, queue):
    print("[*]", end='')
    print(datetime.now().strftime("%d-%m-%Y %H:%M:%S"), end='')
    print(" -- ", end='')
    print("Last batch ID: ", end='')
    print(last, end='')
    print(" -- ", end='')
    print(" Queue size: ", end='')
    print(queue)


# Logs all errors into error_log.txt
def errorLog(timestamp, batchID, error):
    update("Error", batchID)
    print("[!!] ERROR [!!] See log")
    file = open("error_log.txt", "a")
    file.write(str(timestamp) + "-- [" + str(batchID) + "] --" + str(error) + "\n")
    file.close()


# Check the next batch on whether it is empty or not
def check(batchid, n):
    connection = db_connection()
    cursor = connection.cursor()
    cursor.execute('SELECT COUNT(BatchId) FROM dbo.Orders WHERE BatchId = (?)', batchid)
    (amount,) = cursor.fetchone()
    # If the db is empty wait 5x5s (25s total)
    if amount == 0:
        print("[Empty dataframe -- trying again in 10s]")
        time.sleep(10)
        n = n + 1
        # > 25s = empty
        if n >= 8:
            # Update batch status in dbo.Batch
            update("Invalid", batchid)
            init()
        check(batchid, n)
    else:
        # If not empty wait 10s until it is fully uploaded
        time.sleep(10)
        start(batchid)


def start(batchID):
    # Create SQLAlchemy engine
    engine = db_engine()
    # Test SQL connection
    engine.connect()
    df_orders = pd.read_sql_query('SELECT * FROM dbo.Orders WHERE batchID = (?)', engine, params=(batchID,))
    update("Processing", batchID)
    df_orders["Start Point"] = df_orders["Start Point"].astype(str).astype(int)
    # df_orders.to_excel("input.xlsx")
    # df_orders = filter(df_orders)
    cycle_time(df_orders, batchID)
    print(df_orders.head())


def update(status, batchid):
    connection = db_connection()
    cursor = connection.cursor()
    # print(status)
    # print("Updating record..")
    # print(batchid)
    cursor.execute('''UPDATE dbo.Batch 
    SET Status = (?) 
    WHERE ID = (?)''', status, batchid)
    connection.commit()


def cycle_time(df, batchID):
    df_cycle = pd.read_excel('material_info_mould.xlsx', header=0)
    df_cycle = df_cycle[["Material", "Mean zcs", "actual cycle SAP"]]
    df_cycle_complete = pd.merge(left=df, right=df_cycle, how='left', left_on='Material', right_on='Material')
    timm = []
    quantity = df_cycle_complete['Rec./reqd quantity']
    time1 = df_cycle_complete['Mean zcs']
    time2 = df_cycle_complete['actual cycle SAP']
    for i in range(len(quantity)):
        if time1[i] != 'NaN' or time1[i] != 'None':
            timm.append(quantity[i] * time1[i])
        else:
            timm.append(quantity[i] * time2[i])
    df_cycle_complete['Estimated time'] = timm
    df_cycle_complete['Estimated time'] = pd.to_datetime(df_cycle_complete['Estimated time'], unit='s')
    # print(df_cycle_complete.head())
    # df_cycle_complete.to_excel("df_cycle_complete.xlsx")

    # Get start and finish date of the schedule
    connection = db_connection()
    cursor = connection.cursor()
    cursor.execute("SELECT StartDate FROM dbo.Batch WHERE ID = ?", (batchID))
    (startdate1,) = cursor.fetchone()
    cursor.execute("SELECT FinishDate FROM dbo.Batch WHERE ID = ?", (batchID))
    (finishdate1,) = cursor.fetchone()
    cursor.execute("SELECT Weight_bool FROM dbo.Batch WHERE ID = ?", (batchID))
    (default_weights,) = cursor.fetchone()
    start_date = datetime.strptime(startdate1, "%d-%m-%Y")
    finish_date = datetime.strptime(finishdate1, "%d-%m-%Y")

    # default_weights = 0  # The default setting of the prioritisation is used
    if not default_weights == 1:
        from weights import planningWeight
        planningWeight(df_cycle_complete, start_date, finish_date, batchID)
        # print("!!Batch processed with weights set!!")
    elif default_weights == 1:
        from planning import planning
        planning(df_cycle_complete, start_date, finish_date, batchID)
        # print("!!Batch processed with default settings!!")


# Filter the raw dataframe originating from the database
def filter(df):
    df1 = df[['Order', 'Material', 'Rec./reqd quantity', 'Mould',
              'Originating document', 'needed on']]
    df1 = df1.rename(columns={"Material": "Item", 'Rec./reqd quantity': "Quantity", "Originating document": "Planning",
                              "needed on": "Finish date"})
    print(df.columns)
    print(df1.columns)
    return df1


def main():
    init()


if __name__ == "__main__":
    main()
