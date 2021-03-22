import psycopg2

def main():
    #establishing the connection
    conn = psycopg2.connect(
        database="postgres", user='postgres', password='myPassword', host='0.0.0.0', port= '5432')
    #Creating a cursor object using the cursor() method
    cur = conn.cursor()

    # conn = psycopg2.connect(
    # database="postgres", user='postgres', password='myPassword', host='127.0.0.1', port= '5432')
        
    #Preparing query to create a database
    sql = '''CREATE database database_cws'''

    #Creating a database
    cur.execute(sql)
    print("Database created successfully........")

    conn.commit()
    #Closing the connection
    conn.close()

main()