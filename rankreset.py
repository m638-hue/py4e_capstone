import sqlite3

conn = sqlite3.connect("crawlerdb.sqlite")
cur = conn.cursor()

cur.execute('update  Pages set new_rank = 1.0, old_rank = 0.0')

conn.commit()
