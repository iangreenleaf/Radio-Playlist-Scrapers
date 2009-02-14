#!/usr/bin/python
import urllib
from BeautifulSoup import BeautifulSoup
import sqlite3
import re
import datetime
import sys


def append_songs(db, url, cushion_minutes=60):
	connection = sqlite3.connect(db)
	c = connection.cursor()

	thisdate = datetime.date.today()
	cushiondelta = datetime.timedelta(minutes=cushion_minutes)
	cushion = datetime.datetime.now() - cushiondelta

	c.execute('''select title from songs\
			where date_played >= ?\
			and time_played >= ?''',
			(cushion.date().isoformat(), cushion.time().isoformat()))
			#(thisdate.isoformat(), datetime.now() - cushiondelta)

	recentsongsresult = c.fetchall()
	recentsongs = []
	for row in recentsongsresult:
		recentsongs.append(row[0])

	count = 0
	for song in last_10_songs(url):
		if song[2] in recentsongs:
			continue
		else:
			c.execute('insert into songs\
					(date_played, time_played, artist, title)\
					values (?, ?, ?, ?)', (thisdate.isoformat(),) + song)
			count = count + 1

	c.execute('insert into parsed_info\
			values (?, ?)', (thisdate.isoformat(), count))

	connection.commit()
	c.close()


def last_10_songs(url):
	page = urllib.urlopen(url)

	soup = BeautifulSoup(page.read(), convertEntities=BeautifulSoup.HTML_ENTITIES)
	rows = soup.findAll(['td'], { "class" : ["playlist_artist"] })
	for row in rows:
		song_title = None
		artist = None
		bits = row.findAll('a')
		for bit in bits:

			if bit.b:
				artist = bit.b.string
			else:
				song_title = bit.string

		if artist == None or song_title == None:
			continue
		artist = normalize(artist)
		song_title = normalize(song_title)
		time_parsed = datetime.datetime.now().time()
		time_parsed = time_parsed.replace(microsecond = 0, second = 0)
		yield (time_parsed.isoformat(), artist, song_title)
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

# Just spit out some summary info to make sure things went okay
def dump_db():
	connection = sqlite3.connect('tmpdb2')
	c = connection.cursor()
	c.execute('select * from parsed_info')
	for line in c:
		print line
	c.close()


stations = {'cities97': 'http://www.cities97.com/iplaylist/playlist.html?last10=1',
		'kdwb': 'http://www.kdwb.com/iplaylist/playlist.html?last10=1',
		'kool108': 'http://www.kool108.com/iplaylist/playlist.html?last10=1'}
for key in stations:
	db = './' + key + '.sqlite'
	url = stations[key]
	append_songs(db, url)
dump_db()
