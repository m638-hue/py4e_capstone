import sqlite3
import urllib.error
from urllib.parse import urljoin
from urllib.parse import urlparse
from urllib.request import urlopen
from bs4 import BeautifulSoup

conn = sqlite3.connect('crawlerdb.sqlite')
cur = conn.cursor()

cur.executescript('''
    Create table if not exists Pages (
    id          integer primary key,
    url         text unique,
    html        text,
    error       integer,
    old_rank    real,
    new_rank    real
    );

    Create table if not exists Links(
    from_id     integer,
    to_id       integer
    );

    Create table if not exists Webs(
    url         text unique
    )
''')

cur.execute('Select id, url from Pages where html is Null and error is Null order by random() limit 1')
row = cur.fetchone()

if row is not None :
    print("The internet has been entirely copied")

else :
    user_page = input('Please enter another website below :)\n')
    if len(user_page) < 1 : user_page = 'http://python-data.dr-chuck.net/'
    if user_page.endswith('/') : user_page = user_page[:-1]
    if user_page.endswith('.html') or user_page.endswith('.htm') :
        pos = user_page.rfind('/')
        user_page = user_page[:pos]

    if len(user_page) > 1 :
        cur.execute('insert or ignore into Webs (url) values (?)', (user_page, ))
        cur.execute('insert or ignore into Pages (url, html, new_rank) values (?, NULL, 1.0)', (user_page, ))
        conn.commit()

cur.execute('select url from Webs')
webs = list()

for row in cur.fetchall() :
    webs.append(row[0])

limit = 0

while True :
    if limit < 1 :
        try :
            limit = int(input("How many pages should be retrieved?\nMust be a number\n"))
        except :
            print('Quitting program (° ͜ʖ͡°)╭∩╮')
            quit()

    limit -= 1

    cur.execute('Select id, url from Pages where html is null and error is null order by random() limit 1')

    try :
        row = cur.fetchone()
        fromId = row[0]
        url = row[1]

    except :
        print('Entire internet has been copied')
        limit = 0
        break

    cur.execute('Delete from Links where from_id = ?', (fromId, ))

    try :
        document = urlopen(url)
        html = document.read()

        code = document.getcode()
        if code != 200 :
            print('Error on page:', code)
            cur.execute('update Pages set error = ? where url = ?', (code, url))

        if document.info().get_content_type() != 'text/html' :
            print('Page is not html')
            cur.execute('update Pages set error = ? where url = ?', (-1, url))
            conn.commit()
            continue

        soup = BeautifulSoup(html, 'html.parser')

    except KeyboardInterrupt :
        print("Program interruptd by user")
        break

    except :
        print('Failed to retrieve page')
        cur.execute('UPDATE Pages SET error=-1 WHERE url=?', (url, ) )
        conn.commit()
        continue

    cur.execute('INSERT OR IGNORE INTO Pages (url, html, new_rank) VALUES ( ?, NULL, 1.0 )', ( url, ) )
    cur.execute('UPDATE Pages SET html=? WHERE url=?', (memoryview(html), url ))
    conn.commit()

    tags = soup('a')
    count = 0

    for tag in tags :
        href = tag.get('href', None)
        if href is None : continue

        up = urlparse(href)
        if len(up.scheme) < 1 : href = urljoin(url, href)

        ipos = href.find('#')
        if ipos > 1 : href = href[:ipos]
        if ( href.endswith('.png') or href.endswith('.jpg') or href.endswith('.gif') ) : continue
        if ( href.endswith('/') ) : href = href[:-1]
        if ( len(href) < 1 ) : continue

        found = False
        for web in webs :
            if href.startswith(web) :
                found = True
                break

        if not found : continue

        cur.execute('insert or ignore into Pages (url, html, new_rank) values (?, NULL, 1.0)', (href,))
        count += 1
        conn.commit()

        cur.execute('select id from Pages where url = ? limit 1', (href,))
        try :
            row = cur.fetchone()
            toId = row[0]

        except :
            print('Could not retrieve id')
            continue

        if fromId != toId :
            cur.execute('insert into Links (from_id, to_id) values (?, ?)', (fromId, toId))

    print(limit, '-', count)

cur.close()
