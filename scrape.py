#!/usr/bin/python

# Ian Young
# December 2008

# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
# 
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
# 
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

import urllib
from BeautifulSoup import BeautifulSoup
import sqlite3
import re
import datetime
import sys

# Initialize a DB, and scrape all songs from startdate through enddate into it
def populate_songs(db, startdate, enddate):
	connection = sqlite3.connect(db)
	c = connection.cursor()

	# Comment all these statements if not starting with a blank slate
	c.execute('drop table if exists songs')
	c.execute('''create table songs
			(id integer primary key,
			time_played time,
			date_played date,
			artist text,
			title text)''')
	# parsed_info just tracks some summary information, it's not very crucial
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

	# Clean up duplicate entries that exist in the Current's playlists (SLOW)
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

# Get all the songs for the given day from The Current's site
# Generator that yields (time, artist, title) tuples
def songs_for_day(date):
	# nab the page
	page = urllib.urlopen("http://minnesota.publicradio.org/radio/services/the_current/playlist/playlist.php?month="+str(date.month)+"&day="+str(date.day)+"&year="+str(date.year))

	# Use BeautifulSoup to parse the page structure
	soup = BeautifulSoup(page.read(), convertEntities=BeautifulSoup.HTML_ENTITIES)
	divs = soup.findAll(['div', 'span'], { "class" : ["playTime", "songInfo", "hourH3"] })
	for div in divs:
		# This is an hour header, do all sorts of black magic to figure out
		# if we're in AM or PM time
		if div['class'] == "hourH3":
			pm_re = re.compile('(11.*12.*AM)|PM')
			fake_pm_re = re.compile('(11.*12.*PM)')
			if div.string and pm_re.search(div.string) and not fake_pm_re.search(div.string):
				post_meridiem = True
			else:
				post_meridiem = False
		# If it's a time, parse it into a time object
		elif div['class'] == "playTime":
			timebits = map(int, div.string.split(':'))
			# Make the hour military time
			if post_meridiem and timebits[0] < 12:
				timebits[0] += 12
			# Midnight becomes 0:00
			if not post_meridiem and timebits[0] == 12:
				timebits[0] = 0
			play_time = datetime.time(timebits[0], timebits[1])
		# Otherwise it's a artist and title string, parse it
		elif div['class'] == "songInfo":
			if (div.h4.a != None):
				song_title = div.h4.a.string
				artist = ''.join(div.h4.findAll(text=True, recursive=False))
				# sanity check
				if artist == None or song_title == None:
					continue
			else:
				myStrings = div.h4.findAll(text=True, recursive=False)
				# sanity check
				if len(myStrings) < 2:
					continue
				artist = myStrings[0]
				song_title = myStrings[1]
			artist = normalize(artist)
			song_title = normalize(song_title)
			yield (play_time.isoformat(), artist, song_title)

# Just spit out some summary info to make sure things went okay
def dump_db():
	connection = sqlite3.connect(db)
	c = connection.cursor()
	c.execute('select * from parsed_info')
	for line in c:
		print line
	c.close()

# Get the DB from cli argument, or just use a default
if len(sys.argv) > 1:
	db = sys.argv[1]
else:
	db = './tmpdb'

start = datetime.date(2005, 12, 22)
end = datetime.date(2009, 5, 22)
populate_songs(db, start, end)
dump_db()
