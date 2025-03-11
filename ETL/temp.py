import psycopg2

conn = psycopg2.connect(database="postgres", user="user", password="password", host="localhost", port="5432")