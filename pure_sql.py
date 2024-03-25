from database import database_connection,execute_query
import csv

database_file="Data Engineer_ETL Assignment.db"

querry="""select c.customer_id as Customer_id ,c.age as Customer_age ,i.item_name as Item_name,
           sum(coalesce(o.quantity,0)) as Quantity
           FROM customers c
           join sales s on c.customer_id=s.customer_id
           join orders o on s.sales_id=o.sales_id
           join items i on i.item_id=o.item_id
           where Customer_age between 18 and 35
           group by 1,2,3
           order by 1 
           """
#Running file system to fetch the output
conn=database_connection(database_file)
if conn:
    cursor=execute_query(conn,querry)
    if cursor:
        list_of_output=cursor.fetchall()
        with open("output_pure_sql.csv","w",) as file:
            csv_writer=csv.writer(file,delimiter=";")
            csv_writer.writerows(list_of_output)
            print("Success !!")
    else:
        print("Cursor creation Error")
else:
    print("Connection Error")

cursor.close()
conn.close()

