import psycopg2
import csv

port = 5433 | 5432

def createTable(tableName, primaryKey, columns):
    cur.execute("""
    CREATE TABLE %s(
        %s PRIMARY KEY,
        %s   
    )
    """ % (tableName, primaryKey, columns))

conn = psycopg2.connect("host=localhost port=%s dbname=database_cw user=postgres password=myPassword" % (port))
cur = conn.cursor()
# cur.execute("""CREATE TABLE movies(
# movieId integer PRIMARY KEY,
# title text,
# year text
# )
# """)

def seperateNameAndYear(title):
    return (title[:-7], title[-5:-1])

def add_movies_table():
    with open('movies.csv', 'r', encoding="utf8") as movies, open('links.csv', 'r') as links:
        movieReader = csv.reader(movies)
        linkReader = csv.reader(links)
        next(movieReader) # Skip the header row.
        next(linkReader) # Skip the header row.
        for row in zip(movieReader, linkReader):
            title, year = seperateNameAndYear(row[0][1])
            cur.execute(
            "INSERT INTO movies VALUES (%s, %s, %s, %s, %s, %s)",
            (row[0][0], title, year, '{' + row[0][2].replace('|', ',') + '}',row[1][1], row[1][2])
        )


def add_ratings_table():
    with open('ratings.csv', 'r', encoding="utf8") as f:
        reader = csv.reader(f)
        next(reader) # Skip the header row.
        for i, row in enumerate(reader, 1):
            cur.execute(
            "INSERT INTO ratings VALUES (%s, %s, %s, %s, %s)",
            (i, row[0], row[1], row[2], row[3])
        )

def add_tags_table():
    with open('tags.csv', 'r', encoding="utf8") as f:
        reader = csv.reader(f)
        next(reader) # Skip the header row.
        for i, row in enumerate(reader, 1):
            cur.execute(
            "INSERT INTO tags VALUES (%s, %s, %s, %s, %s)",
            (i, row[0], row[1], row[2], row[3])
        )


def main():
    # add movies table
    createTable('movies', 'movieId integer', 'title text, year text, genre text [], imdbId text, tmdbId text')
    add_movies_table()

    createTable('ratings', 'ratingId integer', 'userId integer, movieId integer, rating float, timestamp integer')
    add_ratings_table()

    createTable('tags', 'tagId integer', 'userId integer, movieId integer, tag text, timestamp integer')
    add_tags_table()

    """personality table userid, Openness,Agreeableness,Emotional_stability (bruhh), Conscientiousness,extraversion,
     assigned metric, assigned condition, MovieId [1..12],Predicted_rating [1..12], is_personalized, enjoy_watching 
    """

    conn.commit() #commit everything


    
    
main()
