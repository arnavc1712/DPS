#!/usr/bin/python2.7
#
# Assignment2 Interface
#

import psycopg2
import os
import sys
# Donot close the connection inside this file i.e. do not perform openconnection.close()
def RangeQuery(ratingsTableName, ratingMinValue, ratingMaxValue, openconnection):

	RANGE_TABLE_PREFIX = 'RangeRatingsPart'
	RROBIN_TABLE_PREFIX = 'RoundRobinRatingsPart'
	cur = openconnection.cursor()

	cur.execute(""" SELECT * FROM RoundRobinRatingsMetadata""")
	num_partitions = cur.fetchone()[0]
	step = round(float(5)/float(num_partitions),2)
	min_rem = ratingMinValue%step
	min_div = ratingMinValue/step

	max_rem = ratingMaxValue%step
	max_div = ratingMaxValue/step
	
	if ratingMinValue==0:
		min_part_idx = 0
	elif min_rem==0:
		min_part_idx = int(min_div-1)
	else:
		min_part_idx = int(min_div)

	if ratingMaxValue==0:
		max_part_idx = 0
	elif max_rem==0:
		max_part_idx = int(max_div-1)
	else:
		max_part_idx = int(max_div)

	if min_part_idx>num_partitions-1:
		min_part_idx = num_partitions-1
	if max_part_idx>num_partitions-1:
		max_part_idx = num_partitions-1
	final_rows = []
	for i in range(min_part_idx,max_part_idx+1):
		table_name = RANGE_TABLE_PREFIX+str(i)
		QUERY = """ SELECT UserID,MovieID,Rating FROM {0} WHERE RATING>={1} AND RATING<={2}""".format(table_name,ratingMinValue,ratingMaxValue)
		cur.execute(QUERY)
		rows = cur.fetchall()
		
		final_rows.extend([(table_name,) + row for row in rows])


	for i in range(num_partitions):
		table_name = RROBIN_TABLE_PREFIX+str(i)
		QUERY = """ SELECT UserID,MovieID,Rating FROM {0} WHERE RATING>={1} AND RATING<={2}""".format(table_name,ratingMinValue,ratingMaxValue)
		cur.execute(QUERY)
		rows = cur.fetchall()
		if len(rows):
			final_rows.extend([(table_name,) + row for row in rows])

	writeToFile("RangeQueryOut.txt",final_rows)



    



def PointQuery(ratingsTableName, ratingValue, openconnection):
	RANGE_TABLE_PREFIX = 'RangeRatingsPart'
	RROBIN_TABLE_PREFIX = 'RoundRobinRatingsPart'
	cur = openconnection.cursor()

	cur.execute(""" SELECT * FROM RoundRobinRatingsMetadata""")
	num_partitions = cur.fetchone()[0]

	step = round(5/float(num_partitions),2)
	rem = ratingValue%step
	div = ratingValue/step

	final_rows = []
	if ratingValue==0:
		part_idx=0
	elif rem==0:
		part_idx = int(div-1)
	else:
		part_idx = int(div)

	if part_idx>num_partitions-1:
		part_idx = num_partitions-1

	table_name = RANGE_TABLE_PREFIX
	if ratingValue==0:
		table_name+="0"
		QUERY = """ SELECT UserID,MovieID,Rating FROM {0} WHERE RATING={1}""".format(table_name,ratingValue) 
	else:
		table_name+=str(part_idx)
		QUERY = """ SELECT UserID,MovieID,Rating FROM {0} WHERE RATING={1}""".format(table_name,ratingValue) 

	cur.execute(QUERY)
	rows = cur.fetchall()
	final_rows.extend([(table_name,) + row for row in rows])

	for i in range(num_partitions):
		table_name = RROBIN_TABLE_PREFIX+str(i)
	
		QUERY = """ SELECT UserID,MovieID,Rating FROM {0} WHERE RATING={1}""".format(table_name,ratingValue)
		cur.execute(QUERY)
		rows = cur.fetchall()
		if len(rows):
			final_rows.extend([(table_name,) + row for row in rows])


	writeToFile("PointQueryOut.txt",final_rows)




def writeToFile(filename, rows):
    f = open(filename, 'w')
    for line in rows:
        f.write(','.join(str(s) for s in line))
        f.write('\n')
    f.close()
