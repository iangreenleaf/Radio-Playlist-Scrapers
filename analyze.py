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

def threshold_helper(begin, length, threshold):
	subquery = "select count(*) as c, artist, title from songs where date_played >= ? and date_played < ? group by artist, title"
	query = "select artist, title from (" + subquery + ") where c > ?"
	c.execute(query, (begin, begin + length, threshold))
	return c.fetchall()

def longevity_threshold_by_week(start, threshold):
	weekdelta = datetime.timedelta(7)
	#results = aggregate_by_period(start, end, weekdelta,\
	#		"count(*) as c, artist, title", "title, artist", "c desc limit " + str(threshold))
	starting_set = threshold_helper(start, weekdelta, threshold)
	week_arr = {}
	i = 0
	while len(starting_set) > 0:
		i += 1
		week_arr[i] = []
		new_week = threshold_helper(start + weekdelta * i, weekdelta, threshold)
		# Not the fastest search, but that's okay
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

def longevity_avg_by_week(start, end, threshold):
	weekdelta = datetime.timedelta(7)
	timedelta = weekdelta
	curr = start
	results = []
	prev_week = []
	while curr + timedelta <= end:
		maxweeks = 0
		sum = 0
		count = 0
		accepted = []
		for n,l in longevity_threshold_by_week(curr, threshold).iteritems():
			# exclude songs that were on the previous week's list
			for song in l:
				if song in prev_week:
					continue
				else:
					accepted.append(song)

			count += len(accepted)
			sum += n * len(accepted)
			maxweeks = max(maxweeks, n)

		if curr + datetime.timedelta(weeks=maxweeks) > end:
			return results
		if count > 0:
			results.append((curr.isoformat(), float(sum) / float(count)))
		curr = curr + timedelta
		prev_week = accepted
	# Don't imagine we'll ever make it here
	print 'How odd!'

def longevity_max_by_week(start, end, threshold):
	weekdelta = datetime.timedelta(7)
	timedelta = weekdelta
	curr = start
	results = []
	while curr + timedelta <= end:
		maxweeks = 0
		for n,l in longevity_threshold_by_week(curr, threshold).iteritems():

			maxweeks = max(maxweeks, n)

			if maxweeks == 42:
				print l

		if curr + datetime.timedelta(weeks=maxweeks) > end:
			return results
		results.append((curr.isoformat(), maxweeks))
		curr = curr + timedelta
	# Don't imagine we'll ever make it here
	print 'How odd!'


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

#for n,l in longevity_threshold_by_week(start, 5).iteritems():
#	print str(n) + ',' + str(len(l))

#array_to_csv(longevity_avg_by_week(start, end, 5))
array_to_csv(longevity_max_by_week(start, end, 2))
