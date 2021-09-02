import sqlite3

conn = sqlite3.connect('crawlerdb.sqlite')
cur = conn.cursor()

cur.executescript('''
    drop table if exists Pages;
    drop table if exists Links;
    drop table if exists Webs;
''')

conn.commit()

print('Database successfuly deleted')
