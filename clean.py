import csv
import psycopg2

def createTable(tableName, primaryKey, columns):
    cur.execute("""
    CREATE TABLE %s(
        %s PRIMARY KEY,
        %s   
    )
    """ % (tableName, primaryKey, columns))

port = 5433 

conn = psycopg2.connect("host=localhost port=%s dbname=database_cw user=postgres password=myPassword" % (port))
cur = conn.cursor()
createTable('ratings', 'ratingId integer', 'userId integer, movieId integer, rating float, timestamp integer')
createTable('bigRatings', 'bigRatingId integer', 'userId text, movieId integer, rating float, timestamp text')
with open('ratings.csv', 'r', encoding="utf8") as ratings, open('ratings-big.csv', 'r', encoding="utf8") as big_ratings:
    ratingReader = csv.reader(ratings)
    bigRatingReader = csv.reader(big_ratings)
    next(ratingReader) # Skip the header row.
    next(bigRatingReader) # Skip the header row.
    # for row1 in ratingReader:
        
    #     for row2 in bigRatingReader:
    #         if row1[1] == row2[1] and row1[2] == row2[2]:

    for i, row in enumerate(ratingReader, 1):
            cur.execute(
            "INSERT INTO ratings VALUES (%s, %s, %s, %s, %s)",
            (i,row[0], row[1], float(row[2]), row[3])
        )

    for i, row in enumerate(bigRatingReader, 1):
            cur.execute(
            "INSERT INTO bigRatings VALUES (%s, %s, %s, %s, %s)",
            (i,row[0], row[1], float(row[2]), row[3])
        )

conn.commit()