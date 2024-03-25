import sqlite3

def database_connection(file:str):
    try :
        conn=sqlite3.connect(file)
        #Returnin connection object 
        return conn
    
    except sqlite3.Error as msg:
        print("Connection Error occured",msg)
        return None
    
def execute_query(coonecton_object,query):
    try :
        cursor=coonecton_object.cursor()
        cursor.execute(query)
        #Returning cursor object after query execution
        return cursor
    
    except sqlite3.Error as msg:
        print("Querry errors occured ", msg)
        return None