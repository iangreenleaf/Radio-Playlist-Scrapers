#!/home/youngian/local/bin/python
import urllib
from BeautifulSoup import BeautifulSoup
from pysqlite2 import dbapi2
import re
import datetime
from pytz import timezone
import pytz
import sys

debug = False

def append_songs(db, url, cushion_minutes=60):
	connection = dbapi2.connect(db)
	c = connection.cursor()

	thisdate = datetime.date.today()

	c.execute('''select title,artist from songs\
			order by date_played desc,time_played desc
			limit 10''')

	recentsongsresult = c.fetchall()
	recentsongs = []
	for row in recentsongsresult:
		recentsongs.append(row[1] + row[0])

	# Let's set up to do a bit of sanity checking
	dupe_found = False
	dupe_warning = False
	dupes = []
	inserted = []

	count = 0
	for song in last_10_songs(url):
		if song[1] + song[2] in recentsongs:
			dupe_found = True
			dupes.append(song[1] + song[2])
			continue
		else:
			if dupe_found:
				dupe_warning = True
			# This should keep our songs in the order they were parsed
			dt = song[0] - datetime.timedelta(minutes = count)
			date = dt.date()
			time = dt.time()
			artist = song[1]
			title = song[2]
			inserted.append(song)
			c.execute('insert into songs\
					(date_played, time_played, artist, title)\
					values (?, ?, ?, ?)', (date.isoformat(), time.isoformat(), artist, title))
			count = count + 1

	c.execute('insert into parsed_info\
			values (?, ?)', (thisdate.isoformat(), count))

	if debug:
			if (len(inserted) > 5):
				print db + ': Got ' + str(len(inserted)) + ' insertions, which seems odd'
			if dupe_warning:
				print db + ': Duplicates detected out of order'
			if len(inserted) > 5 or dupe_warning:
				print 'Inserted: ' + str(inserted)
				print 'Dupes: ' + str(dupes)
				print 'Recent songs: ' + str(recentsongsresult)

	connection.commit()
	c.close()

	return len(inserted)


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
		dt_parsed = datetime.datetime.now(tz=pytz.utc)
		central = timezone('US/Central')
		dt_parsed = dt_parsed.astimezone(central)
		dt_parsed = dt_parsed.replace(microsecond = 0, second = 0)
		yield (dt_parsed, artist, song_title)
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

stations = {'cities97': 'http://www.cities97.com/iplaylist/playlist.html?last10=1',
		'kdwb': 'http://www.kdwb.com/iplaylist/playlist.html?last10=1',
		'kool108': 'http://www.kool108.com/iplaylist/playlist.html?last10=1'}
sum = 0
for key in stations:
	db = '/home/youngian/scraper/' + key + '.sqlite'
	url = stations[key]
	sum += append_songs(db, url)

if debug and sum == 0:
	print 'No insertions on any station. I\'m vaguely concerned.'
