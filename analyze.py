#!/usr/bin/python
import sqlite3
import datetime
import sys

if len(sys.argv) > 1:
	db = sys.argv[1]
else:
	db = './tmpdb'

connection = sqlite3.connect(db)
c = connection.cursor()

def aggregate_by_period(start, end, timedelta, columns):
	curr = start
	results = []
	while curr + timedelta <= end:
		c.execute("select "+columns+" from songs where date_played >= ? and date_played < ? order by date_played", (curr, curr + timedelta))
		results += c.fetchall()
		curr = curr + timedelta
	return results
	
def unique_song_ratio_by_week(start, end):
	weekdelta = datetime.timedelta(7)
	return aggregate_by_period(start, end, weekdelta, "min(date_played), (1.0 * count(distinct title)) / count(title)")

def unique_song_ratio_by_month(start, end):
	monthdelta = datetime.timedelta(28)
	return aggregate_by_period(start, end, monthdelta, "min(date_played), (1.0 * count(distinct title)) / count(title)")

def avg_song_plays_by_week(start, end):
	weekdelta = datetime.timedelta(7)
	return aggregate_by_period(start, end, weekdelta, "min(date_played), (1.0 * count(distinct title)) / count(title)")

def unique_song_ratio_by_month(start, end):
def array_to_csv(array):
	for row in array:
		print(",".join(map(str, row)))

start = datetime.date(2005, 12, 22)
end = datetime.date(2008, 10, 22)
#end = datetime.date(2006, 1, 22)
#array_to_csv(unique_song_ratio_by_week(start, end))
array_to_csv(unique_song_ratio_by_week(start, end))

