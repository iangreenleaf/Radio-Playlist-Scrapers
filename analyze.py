#!/usr/bin/python
import sqlite3
import datetime
import sys
from dateutil.relativedelta import relativedelta

if len(sys.argv) > 1:
	db = sys.argv[1]
else:
	db = './tmpdb'

connection = sqlite3.connect(db)
c = connection.cursor()

def aggregate_by_period(start, end, timedelta, columns, groupby="", orderby="date_played"):
	if groupby:
		groupby = " group by "+groupby
	if orderby:
		orderby = " order by "+orderby
	curr = start
	results = []
	while curr + timedelta <= end:
		c.execute("select "+columns+" from songs where date_played >= ? and date_played < ? \
				"+groupby+orderby, (curr, curr + timedelta))
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

def most_played_songs_by_month(start, end):
	monthdelta = relativedelta(months=1)
	return aggregate_by_period(start, end, monthdelta,\
			"count(*) as c, artist, title", "title, artist", "c desc limit 5")

def array_to_csv(array):
	for row in array:
		print(",".join(map(str, row)))

start = datetime.date(2005, 12, 22)
end = datetime.date(2008, 10, 22)
#end = datetime.date(2006, 1, 22)
#array_to_csv(unique_song_ratio_by_week(start, end))
#array_to_csv(unique_song_ratio_by_month(start, end))
#array_to_csv(avg_song_plays_by_week(start, end))
#array_to_csv(avg_song_plays_by_month(start, end))
#array_to_csv(max_song_plays_by_week(start, end))
#array_to_csv(max_song_plays_by_month(start, end))
#array_to_csv(max_song_plays_by_day(start, end))
#array_to_csv(most_played_songs_by_month(start, end))

