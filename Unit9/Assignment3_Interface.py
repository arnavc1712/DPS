#!/usr/bin/python2.7
#
# Assignment3 Interface
#

import psycopg2
import os
import sys
import threading

# Donot close the connection inside this file i.e. do not perform openconnection.close()
def ParallelSort (InputTable, SortingColumnName, OutputTable, openconnection):
    #Implement ParallelSort Here.
    number_of_threads = 5
    range_partition_name = "rangepart"
    cur = openconnection.cursor()
    query = "SELECT min({}) from {}".format(SortingColumnName,InputTable)
    cur.execute(query)
    min_val = cur.fetchone()[0]

    query = "SELECT max({}) from {}".format(SortingColumnName,InputTable)
    cur.execute(query)
    max_val = cur.fetchone()[0]

    part_counts = abs(max_val-min_val)/float(number_of_threads)
    # print(min_val,max_val,part_counts)
    query = "create table {0} as select * from {1} where 1=2".format(OutputTable,InputTable)
    cur.execute(query)

    for i in range(number_of_threads):
        output_table = range_partition_name+str(i)
        query = "CREATE table {} as select * from {} where 1=2".format(output_table,InputTable)
        cur.execute(query)

    threads = list(range(number_of_threads))

    count = min_val
    for i in range(number_of_threads):
        lower_val = count
        upper_val = lower_val+part_counts
        count+=part_counts 
        output_table = range_partition_name+str(i)
        x= threading.Thread(target=range_partition,args=(InputTable,SortingColumnName,output_table,lower_val,upper_val,i,openconnection))
        threads[i] = x
        threads[i].start()

    for i in range(number_of_threads):
        threads[i].join()

    for i in range(number_of_threads):
        output_table = range_partition_name+str(i)
        query = "insert into {0} select * from {1}".format(OutputTable,output_table)
        cur.execute(query)
        query = "drop table if exists {0}".format(output_table)
        cur.execute(query)

    


    openconnection.commit()
    # pass #Remove this once you are done with implementation

def ParallelJoin (InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, OutputTable, openconnection):
    #Implement ParallelJoin Here.
     # Remove this once you are done with implementation

    cur = openconnection.cursor()
    number_of_threads = 5
    table1_range_partition = "table1_range_partition"
    table2_range_partition = "table2_range_partition"

    query = "select min({0}) from {1}".format(Table1JoinColumn,InputTable1)
    cur.execute(query)

    min_val_1 = cur.fetchone()[0]

    query = "select min({0}) from {1}".format(Table2JoinColumn,InputTable2)
    cur.execute(query)

    min_val_2 = cur.fetchone()[0]

    query = "select max({0}) from {1}".format(Table1JoinColumn,InputTable1)
    cur.execute(query)

    max_val_1 = cur.fetchone()[0]

    query = "select max({0}) from {1}".format(Table2JoinColumn,InputTable2)
    cur.execute(query)

    max_val_2 = cur.fetchone()[0]

    all_min = min(min_val_1,min_val_2)
    all_max = max(max_val_1,max_val_2)

    # Range partioning
    part_counts = abs(all_max-all_min)/float(number_of_threads)

    count = all_min

    for i in range(number_of_threads):
        lower_val = count
        upper_val = lower_val+part_counts
        count+=part_counts 
        table1_output_table = table1_range_partition+str(i)
        table2_output_table = table2_range_partition+str(i)
        create_range_partitions(InputTable1,Table1JoinColumn,table1_output_table,lower_val,upper_val,i,openconnection)
        create_range_partitions(InputTable2,Table2JoinColumn,table2_output_table,lower_val,upper_val,i,openconnection)


    ## Parallel Joining

    threads = list(range(number_of_threads))

    for i in range(number_of_threads):
        output_table = "join_partition"+str(i)
        table1_output_table = table1_range_partition+str(i)
        table2_output_table = table2_range_partition+str(i)
        x= threading.Thread(target=parallel_join,args=(table1_output_table,Table1JoinColumn,table2_output_table,Table2JoinColumn,output_table,i,openconnection))
        threads[i] = x
        threads[i].start()

    for i in range(number_of_threads):
        threads[i].join()

    query = "create table {0} as select * from {1},{2} where 1=2".format(OutputTable,InputTable1,InputTable2)
    cur.execute(query)

    for i in range(number_of_threads):
        output_table = "join_partition"+str(i)
        table1_output_table = table1_range_partition+str(i)
        table2_output_table = table2_range_partition+str(i)
        query = "insert into {0} select * from {1}".format(OutputTable,output_table)
        cur.execute(query)

        cur.execute("drop table if exists {}".format(table1_output_table))
        cur.execute("drop table if exists {}".format(table2_output_table)) 
        cur.execute("drop table if exists {}".format(output_table))


    openconnection.commit()








def range_partition(InputTable,SortingColumnName,output_table,lower_val,upper_val,part_idx,openconnection):
    cur = openconnection.cursor()
    if part_idx==0:
        query = "insert into {0} select * from {1} where {2}>={3} and {2}<={4} order by {2}".format(output_table,InputTable,SortingColumnName,lower_val,upper_val) 
    else:
        query = "insert into {0} select * from {1} where {2}>{3} and {2}<={4} order by {2}".format(output_table,InputTable,SortingColumnName,lower_val,upper_val) 

    cur.execute(query)


def create_range_partitions(InputTable,JoinColumnName,output_table,lower_val,upper_val,part_idx,openconnection):
    cur = openconnection.cursor()
    if part_idx==0:
        query = "create table {0} as select * from {1} where {2}>={3} and {2}<={4} order by {2}".format(output_table,InputTable,JoinColumnName,lower_val,upper_val) 
    else:
        query = "create table {0} as select * from {1} where {2}>{3} and {2}<={4} order by {2}".format(output_table,InputTable,JoinColumnName,lower_val,upper_val) 

    cur.execute(query)


def parallel_join(table1_output_table,Table1JoinColumn,table2_output_table,Table2JoinColumn,output_table,i,openconnection):
    cur = openconnection.cursor()
    # print(output_table)
    query = "create table {0} as select * from {1} INNER JOIN {2} ON {1}.{3}={2}.{4}".format(output_table,table1_output_table,table2_output_table,Table1JoinColumn,Table2JoinColumn)
    cur.execute(query)

################### DO NOT CHANGE ANYTHING BELOW THIS #############################


# Donot change this function
def getOpenConnection(user='postgres', password='1234', dbname='postgres'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")

# Donot change this function
def createDB(dbname='dds_assignment'):
    """
    We create a DB by connecting to the default user and database of Postgres
    The function first checks if an existing database exists for a given name, else creates it.
    :return:None
    """
    # Connect to the default database
    con = getOpenConnection(dbname='postgres')
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()

    # Check if an existing database with the same name exists
    cur.execute('SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname=\'%s\'' % (dbname,))
    count = cur.fetchone()[0]
    if count == 0:
        cur.execute('CREATE DATABASE %s' % (dbname,))  # Create the database
    else:
        print 'A database named {0} already exists'.format(dbname)

    # Clean up
    cur.close()
    con.commit()
    con.close()

