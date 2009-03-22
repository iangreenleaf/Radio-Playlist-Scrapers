#!/home/youngian/local/bin/python
import urllib
from pysqlite2 import dbapi2
import re
import datetime
import sys
import simplejson

debug = False

def create_tables(db):
	connection = dbapi2.connect(db)
	c = connection.cursor()

	# Comment all these statements if not starting with a blank slate
	c.execute('drop table if exists songs')
	c.execute('''create table songs
			(id integer primary key,
			time_played time,
			date_played date,
			artist text,
			title text)''')
	# The most important thing this does is store the "next" value that we use in requests
	c.execute('drop table if exists last_parsed')
	c.execute('''create table last_parsed
			(id integer primary key,
			next_id int,
			date date,
			count int)''')
	# Put a dummy value in here
	# This will start us off with a query that doesn't get any entries but gets the
	# most recent "next" value. This entry may be deleted once a running database exists
	c.execute('insert into last_parsed (next_id, date, count) values (1, "1980-01-01", 0)')
	connection.commit()
	c.close()

def append_songs(db, url):
	connection = dbapi2.connect(db)
	c = connection.cursor()

	thisdate = datetime.date.today()

	c.execute('select next_id from last_parsed order by id desc limit 1');
	nextAt = c.fetchone()[0]
	url = url + '/ev/' + str(nextAt)

	page = urllib.urlopen(url)
	result = simplejson.load(page)

	count = 0
	for song in recent_songs(result):
			dt = song[0]
			date = dt.date()
			time = dt.time()
			artist = song[1]
			title = song[2]
			c.execute('insert into songs\
					(date_played, time_played, artist, title)\
					values (?, ?, ?, ?)', (date.isoformat(), time.isoformat(), artist, title))
			count = count + 1

	next_id = result['next']
	c.execute('insert into last_parsed (next_id, date, count)\
			values (?, ?, ?)', (next_id, thisdate.isoformat(), count))

	connection.commit()
	c.close()

	return


def recent_songs(result):

	songs = result['entries']
	for row in songs:
		song_title = row['title']
		artist = row['by']
		if artist == None or song_title == None:
			continue
		artist = normalize(artist)
		song_title = normalize(song_title)
		dt_parsed = datetime.datetime.fromtimestamp(row['at'] / 1000)
		dt_parsed = dt_parsed.replace(microsecond = 0, second = 0)
		yield (dt_parsed, artist, song_title)


def normalize(string):
	string = unicode(string)
	# Filter anything out of the ordinary, including punctuation
	rexp = re.compile('[^\w\s\d]')
	string = rexp.sub('', string)
	# Compress whitespace
	rexp = re.compile('\s+')
	string = rexp.sub(' ', string)
	# Lowercase everything
	string = string.lower()
	# trim whitespace
	string = string.strip()
	return string

stations = {'something': 'http://r2b.yes.com/relay/fead789258e8b63acb4c17cfb5ded00d1fa2a3e9'}
sum = 0
for key in stations:
	db = '/home/youngian/scraper/' + key + '.sqlite'
	db = 'newtmpdb'
	url = stations[key]
	append_songs(db, url)