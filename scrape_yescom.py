#!/home/youngian/local/bin/python
import urllib
from pysqlite2 import dbapi2
import re
import datetime
import sys
import simplejson

debug = False

def append_songs(db, url):
	connection = dbapi2.connect(db)
	c = connection.cursor()

	thisdate = datetime.date.today()

	#c.execute('select next_id from last_parsed order by date desc limit 1');
	#nextAt = c.fetchone()[0]
	nextAt = 31553
	url = url + '/ev/' + str(nextAt)

	count = 0
	for song in last_10_songs(url):
			dt = song[0]
			date = dt.date()
			time = dt.time()
			artist = song[1]
			title = song[2]
			c.execute('insert into songs\
					(date_played, time_played, artist, title)\
					values (?, ?, ?, ?)', (date.isoformat(), time.isoformat(), artist, title))
			count = count + 1

	c.execute('insert into parsed_info\
			values (?, ?)', (thisdate.isoformat(), count))

	connection.commit()
	c.close()

	return


def last_10_songs(url):
	page = urllib.urlopen(url)

	result = simplejson.load(page)
	next_id = result['next']
	songs = result['entries']
	for row in songs:
		song_title = row['title']
		artist = row['by']
		if artist == None or song_title == None:
			continue
		artist = normalize(artist)
		song_title = normalize(song_title)
		#central = timezone('US/Central')
		#dt_parsed = datetime.fromtimestamp(row['at'] / 1000, central)
		dt_parsed = datetime.datetime.fromtimestamp(row['at'] / 1000)
		#dt_parsed = dt_parsed.astimezone(central)
		dt_parsed = dt_parsed.replace(microsecond = 0, second = 0)
		yield (dt_parsed, artist, song_title, next_id)
		#yield (datetime.datetime.now().time().strftime('%H:%M:%S'), artist, song_title)


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
