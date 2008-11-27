#!/usr/bin/python
import urllib
from BeautifulSoup import BeautifulSoup
import sqlite3
import re
import datetime
import sys

def populate_songs(db, startdate, enddate):
	connection = sqlite3.connect(db)
	c = connection.cursor()

	c.execute('drop table if exists songs')
	c.execute('''create table songs
			(id integer primary key,
			time_played time,
			date_played date,
			artist text,
			title text)''')
	c.execute('drop table if exists parsed_info')
	c.execute('''create table parsed_info
			(day date,
			number_songs int)''')

	thisdate = startdate
	while thisdate <= enddate:
		if thisdate.day == 1:
			print("Working on "+str(thisdate.month)+"-"+str(thisdate.year))
		count = 0
		for song in songs_for_day(thisdate):
			c.execute('insert into songs\
					(date_played, time_played, artist, title)\
					values (?, ?, ?, ?)', (thisdate.isoformat(),) + song)
			count = count + 1
		c.execute('insert into parsed_info\
				values (?, ?)', (thisdate.isoformat(), count))
		thisdate = thisdate + datetime.timedelta(1)

	# Clean up duplicate entries that exist in the Current's playlists
	c.execute('''delete from songs where id in \
			(select b.id from songs a, songs b \
			where a.date_played=b.date_played \
			and a.time_played=b.time_played \
			and a.artist=b.artist \
			and a.title=b.title \
			and a.id<b.id)''')

	connection.commit()
	c.close()

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

def songs_for_day(date):
	page = urllib.urlopen("http://minnesota.publicradio.org/radio/services/the_current/songs_played/?month="+str(date.month)+"&day="+str(date.day)+"&year="+str(date.year))

	soup = BeautifulSoup(page.read(), convertEntities=BeautifulSoup.HTML_ENTITIES)
	divs = soup.findAll(['div', 'h3'], { "class" : ["playlist_time", "playlist_title", "header"] })
	for div in divs:
		if div['class'] == "header" and div.name == "h3":
			pm_re = re.compile('(11.*12.*AM)|PM')
			fake_pm_re = re.compile('(11.*12.*PM)')
			if div.string and pm_re.search(div.string) and not fake_pm_re.search(div.string):
				post_meridiem = True
			else:
				post_meridiem = False
		elif div['class'] == "playlist_time":
			#play_time = datetime.time(div.string)
			timebits = map(int, div.string.split(':'))
			if post_meridiem and timebits[0] < 12:
				timebits[0] += 12
			if not post_meridiem and timebits[0] == 12:
				timebits[0] = 0
			play_time = datetime.time(timebits[0], timebits[1])
		elif div['class'] == "playlist_title":
			artist = div.a.string
			song_title = ''.join(div.findAll(text=True, recursive=False))
			song_title = re.sub(r'\s*-\s+', r'', song_title)
			#print play_time + " " + artist + " - " + song_title
			if artist == None or song_title == None:
				continue
			artist = normalize(artist)
			song_title = normalize(song_title)
			yield (play_time.isoformat(), artist, song_title)

def dump_db():
	connection = sqlite3.connect('tmpdb')
	c = connection.cursor()
	c.execute('select * from parsed_info')
	for line in c:
		print line
	c.close()

if len(sys.argv) > 1:
	db = sys.argv[1]
else:
	db = './tmpdb'

start = datetime.date(2005, 12, 22)
end = datetime.date(2008, 10, 22)
#end = datetime.date(2006, 1, 2)
populate_songs(db, start, end)
dump_db()
