#!/usr/bin/python
import sqlite3
import time
import datetime
import sys
from dateutil.relativedelta import relativedelta

if len(sys.argv) > 1:
	db = sys.argv[1]
else:
	db = './tmpdb'

connection = sqlite3.connect(db)
c = connection.cursor()

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

