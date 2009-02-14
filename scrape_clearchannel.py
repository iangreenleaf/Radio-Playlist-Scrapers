#!/usr/bin/python
import urllib
from BeautifulSoup import BeautifulSoup
import sqlite3
import re
import datetime
import sys


def last_10_songs(url):
	page = urllib.urlopen("http://www.cities97.com/iplaylist/playlist.html?net="+num)
	page = urllib.urlopen(url)

	soup = BeautifulSoup(page.read(), convertEntities=BeautifulSoup.HTML_ENTITIES)
	divs = soup.findAll(['td'], { "class" : ["playlist_artist"] })
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

