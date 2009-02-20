#!/usr/bin/python

# Ian Young
# December 2008

# This program is free software; you can redistribute it and/or modify it 
# under the terms of the GNU General Public License as published by the Free 
# Software Foundation; either version 2 of the License, or (at your option) 
# any later version.
# 
# This program is distributed in the hope that it will be useful, but WITHOUT 
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or 
# FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for 
# more details.
# 
# You should have received a copy of the GNU General Public License along 
# with this program; if not, write to the Free Software Foundation, Inc., 59 
# Temple Place, Suite 330, Boston, MA 02111-1307 USA 

import sqlite3
import time
import datetime
import sys
from dateutil.relativedelta import relativedelta

# Get the DB from cli argument, or just use a default
if len(sys.argv) > 1:
	db = sys.argv[1]
else:
	db = './tmpdb'

# Initialize the DB. Yeah, this is global.
connection = sqlite3.connect(db)
c = connection.cursor()

# If you're reading this, sorry about the lack of comments. Check out the
# functions further down for descriptively-named stuff.
def aggregate_by_period(start, end, timedelta, columns, groupby="", orderby="date_played", constraints=""):
	if groupby:
		groupby = " group by "+groupby
	if orderby:
		orderby = " order by "+orderby
	if constraints:
		constraints = " and ("+constraints+")"
	curr = start
	results = []
	while curr + timedelta <= end:
		c.execute("select "+columns+" from songs where date_played >= ? and date_played < ? \
				"+constraints+groupby+orderby, (curr, curr + timedelta))
		results += c.fetchall()
		curr = curr + timedelta
	return results

def aggregate_by_period_subselect(start, end, timedelta, columns1, columns2, groupby):
	if groupby:
		groupby = " group by "+groupby
	curr = start
	results = []
	while curr + timedelta <= end:
		c.execute("select ?, "+columns1+" from \
				(select "+columns2+" from songs where date_played >= ? and date_played < ? "\
				+groupby+" order by date_played)", (curr, curr, curr + timedelta))
		results += c.fetchall()
		curr = curr + timedelta
	return results
	
def unique_song_ratio_by_week(start, end):
	weekdelta = datetime.timedelta(7)
	return aggregate_by_period(start, end, weekdelta, "min(date_played), (1.0 * count(distinct title)) / count(title)")

def unique_song_ratio_by_month(start, end):
	#monthdelta = datetime.timedelta(28)
	monthdelta = relativedelta(months=1)
	return aggregate_by_period(start, end, monthdelta, "min(date_played), (1.0 * count(distinct title)) / count(title)")

def avg_song_plays_by_week(start, end):
	weekdelta = datetime.timedelta(7)
	return aggregate_by_period_subselect(start, end, weekdelta, \
			"avg(c)", "count(*) as c", "title, artist")

def max_song_plays_by_week(start, end):
	weekdelta = datetime.timedelta(7)
	return aggregate_by_period_subselect(start, end, weekdelta, \
			"max(c)", "count(*) as c", "title, artist")

def avg_song_plays_by_month(start, end):
	#monthdelta = datetime.timedelta(28)
	monthdelta = relativedelta(months=1)
	return aggregate_by_period_subselect(start, end, monthdelta, \
			"avg(c)", "count(*) as c", "title, artist")

def max_song_plays_by_month(start, end):
	monthdelta = relativedelta(months=1)
	return aggregate_by_period_subselect(start, end, monthdelta, \
#			"min(firstdate), max(c)", "min(date_played) as firstdate, count(*) as c", "title, artist")
			"max(c)", "count(*) as c", "title, artist")

def max_song_plays_by_day(start, end):
	weekdelta = datetime.timedelta(1)
	return aggregate_by_period_subselect(start, end, weekdelta, \
			"max(c)", "count(*) as c", "title, artist")

def most_played_songs_by_week(start, end):
	weekdelta = datetime.timedelta(7)
	return aggregate_by_period(start, end, weekdelta,\
			"count(*) as c, artist, title", "title, artist", "c desc limit 5")

def most_played_songs_by_day(start, end):
	daydelta = datetime.timedelta(1)
	return aggregate_by_period(start, end, daydelta,\
			"count(*) as c, artist, title", "title, artist", "c desc limit 5")

def most_played_songs_by_month(start, end):
	monthdelta = relativedelta(months=1)
	return aggregate_by_period(start, end, monthdelta,\
			"count(*) as c, artist, title", "title, artist", "c desc limit 5")

def threshhold_helper(begin, length, threshhold):
	subquery = "select count(*) as c, artist, title from songs where date_played >= ? and date_played < ? group by artist, title"
	query = "select artist, title from (" + subquery + ") where c > ?"
	c.execute(query, (begin, begin + length, threshhold))
	return c.fetchall()

def played_threshhold_by_week(start, end, threshhold):
	weekdelta = datetime.timedelta(7)
	#results = aggregate_by_period(start, end, weekdelta,\
	#		"count(*) as c, artist, title", "title, artist", "c desc limit " + str(threshhold))
	starting_set = threshhold_helper(start, weekdelta, threshhold)
	week_arr = {}
	i = 0
	while len(starting_set) > 0:
		i += 1
		week_arr[i] = []
		new_week = threshhold_helper(start + weekdelta * i, weekdelta, threshhold)
		for song in starting_set:
			found = False
			for new_song in new_week:
				if song[0] == new_song[0] and song[1] == new_song[1]:
					found = True
					break

			if not found:
				week_arr[i].append(song)
				starting_set.remove(song)

	return week_arr

def unique_song_ratio_by_week_drivetime(start, end):
	weekdelta = datetime.timedelta(7)
	return aggregate_by_period(start, end, weekdelta, "min(date_played), (1.0 * count(distinct title)) / count(title)", constraints="(time_played > time('06:00') and time_played < time('10:00')) or (time_played > time('15:00') and time_played < ('19:00'))")

def unique_song_ratio_by_month_and_time(start, end, starttime, endtime):
	monthdelta = relativedelta(months=1)
	return aggregate_by_period(start, end, monthdelta, "min(date_played), (1.0 * count(distinct title)) / count(title)", constraints="time_played > '"+starttime.isoformat()+"' and time_played < '"+endtime.isoformat()+"'")

def array_to_csv(array):
	for row in array:
		print(",".join(map(str, row)))

start = datetime.date(2005, 12, 22)
end = datetime.date(2008, 11, 22)
#end = datetime.date(2006, 1, 22)
#array_to_csv(unique_song_ratio_by_week(start, end))
#array_to_csv(unique_song_ratio_by_month(start, end))
#array_to_csv(avg_song_plays_by_week(start, end))
#array_to_csv(avg_song_plays_by_month(start, end))
#array_to_csv(max_song_plays_by_week(start, end))
#array_to_csv(max_song_plays_by_month(start, end))
#array_to_csv(max_song_plays_by_day(start, end))
#array_to_csv(most_played_songs_by_month(start, end))
#array_to_csv(most_played_songs_by_week(start, end))
#array_to_csv(most_played_songs_by_day(start, end))
#array_to_csv(unique_song_ratio_by_week_drivetime(start, end))
#array_to_csv(unique_song_ratio_by_week_and_time(start, end, datetime.time(2,0), datetime.time(4,0)))
#array_to_csv(unique_song_ratio_by_month_and_time(start, end, datetime.time(12,0), datetime.time(13,59)))

#for h in range(0,24,2):
#	starttime = datetime.time(h,0)
#	endtime = datetime.time(h+1,59)
#	print(','+starttime.strftime("%H:%M")+'-'+endtime.strftime("%H:%M"))
#	array_to_csv(unique_song_ratio_by_month_and_time(start, end, starttime, endtime))

for n,l in played_threshhold_by_week(start, end, 5).iteritems():
	print str(n) + ',' + str(len(l))
