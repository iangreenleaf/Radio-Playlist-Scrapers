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
		count = 0
		for song in songs_for_day(thisdate):
			c.execute('insert into songs\
					(date_played, time_played, artist, title)\
					values (?, ?, ?, ?)', (thisdate.isoformat(),) + song)
			count = count + 1
		c.execute('insert into parsed_info\
				values (?, ?)', (thisdate.isoformat(), count))
		thisdate = thisdate + datetime.timedelta(1)

	connection.commit()
	c.close()

def songs_for_day(date):
	page = urllib.urlopen("http://minnesota.publicradio.org/radio/services/the_current/songs_played/?month="+str(date.month)+"&day="+str(date.day)+"&year="+str(date.year))

	soup = BeautifulSoup(page.read())
	divs = soup.findAll('div', { "class" : ["playlist_time", "playlist_title"] })
	for div in divs:
		if div['class'] == "playlist_time":
			#play_time = datetime.time(div.string)
			play_time = div.string
		else:
			artist = div.a.string
			song_title = ''.join(div.findAll(text=True, recursive=False))
			song_title = re.sub(r'\s*-\s+', r'', song_title)
			#print play_time + " " + artist + " - " + song_title
			yield (play_time, artist, song_title)

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
populate_songs(db, start, end)
dump_db()

