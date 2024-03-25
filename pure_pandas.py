from database import database_connection
import pandas as pd

def create_dataframe(conn:database_connection,table_name:list):
    list_of_dataframes=[]
    for name in table_name:
        temp=pd.read_sql_query(f"select * from {name}",conn)
        list_of_dataframes.append(temp)
    return list_of_dataframes

def left_join(list_of_dataframes:list):
    temp=pd.merge(list_of_dataframes[0],list_of_dataframes[1] ,on="customer_id",how="left")
    temp=pd.merge(temp,list_of_dataframes[2],on="sales_id",how="left")
    temp=pd.merge(temp,list_of_dataframes[3],on="item_id",how="left")
    temp["quantity"].fillna(0,inplace=True)
    return temp

def convert_df(df:pd):
    # Convertint to int
    df["quantity"]=df["quantity"].astype(int)
    #Droping other column
    df.drop("item_id",axis=1,inplace=True)
    df.drop("sales_id",axis=1,inplace=True)
    df.drop("order_id",axis=1,inplace=True)
    #Filtering df on age
    df.query('age>=18 and age<=35',inplace=True)
    #Applying group by and find sum  
    res=df.groupby(["customer_id","age","item_name"])["quantity"].sum()
    return res


database_file="Data Engineer_ETL Assignment.db"

try:
    conn=database_connection(database_file)
except:
    print("Error occured while making connection object")
if conn:
    list_of_dataframes=create_dataframe(conn,["customers","sales","orders","items"])
    if list_of_dataframes:
        df=left_join(list_of_dataframes)
        if df.any().any():
            new_df=convert_df(df)
            if new_df.any().any():
                new_df.to_csv("otput_pure_pandas.csv",sep=";")
                print("Success!!")
            else:
                print("File writing Error")
        else:
            print("df coversion Errors")
    else:
        print("List creation Errors")

else:
    print("Connection Errors")
conn.close()