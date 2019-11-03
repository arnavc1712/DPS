

RATINGS_TABLE = 'ratings'
RANGE_TABLE_PREFIX = 'range_part'
RROBIN_TABLE_PREFIX = 'rrobin_part'
USER_ID_COLNAME = 'userid'
MOVIE_ID_COLNAME = 'movieid'
RATING_COLNAME = 'rating'
INPUT_FILE_PATH = 'test_data.txt'
ACTUAL_ROWS_IN_INPUT_FILE = 20

 #!/usr/bin/python2.7
#
# Interface for the assignement
#

import psycopg2
def getOpenConnection(user='postgres', password='1234', dbname='postgres'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")


def loadRatings(ratingstablename, ratingsfilepath, openconnection):
    cur = openconnection.cursor()
    cur.execute(""" DROP TABLE IF EXISTS {}""".format(ratingstablename))
    cur.execute(""" CREATE TABLE {}(id SERIAL PRIMARY KEY,UserID int, var1 text,MovieID int,var2 text,Rating float,var3 text,timestamp float)""".format(ratingstablename))

    with open(ratingsfilepath) as f:
        cur.copy_from(f,ratingstablename,sep=":",columns = ("UserID","var1","MovieID","var2","Rating","var3","timestamp"))
    cur.execute(""" ALTER TABLE {0} DROP COLUMN var1,DROP COLUMN var2,DROP COLUMN var3,DROP COLUMN timestamp""".format(ratingstablename))

    
    openconnection.commit()
    cur.close()
    


def rangePartition(ratingstablename, numberofpartitions, openconnection):
    cur = openconnection.cursor()
    step = 5/float(numberofpartitions)
    # hash_map = {}
    for i in range(numberofpartitions):
        # hash_map[i]=[i*step,i*step+step]
        cur.execute(""" DROP TABLE IF EXISTS {}""".format("range_part"+str(i)))
        if i==0:

            cur.execute(""" CREATE TABLE {0} AS SELECT * FROM {1} WHERE RATING>={2} AND RATING<={3}""".format("range_part"+str(i),
                                                                                                         ratingstablename,
                                                                                                         str(i*step),
                                                                                                         str(i*step+step)))
        
        else:                                                                                            
            cur.execute(""" CREATE TABLE {0} AS SELECT * FROM {1} WHERE RATING>{2} AND RATING<={3}""".format("range_part"+str(i),
                                                                                                         ratingstablename,
                                                                                                         str(i*step),
                                                                                                         str(i*step+step)))
    ## TODO: Drop table if exists
    cur.execute(""" DROP TABLE IF EXISTS {}""".format("range_count"))
    cur.execute(""" CREATE TABLE RANGE_COUNT (NUM_PARTITIONS int) """)
    cur.execute(""" INSERT INTO RANGE_COUNT (NUM_PARTITIONS) VALUES({0})""".format(str(numberofpartitions)))
    openconnection.commit()
    cur.close()




def roundRobinPartition(ratingstablename, numberofpartitions, openconnection):
    cur = openconnection.cursor()
    
    for i in range(numberofpartitions):
    	cur.execute(""" DROP TABLE IF EXISTS {}""".format("rrobin_part"+str(i)))
        if i==(numberofpartitions-1):
            cur.execute(""" CREATE TABLE {0} AS SELECT * FROM {1} WHERE MOD(ID,{2})= {3}""".format("rrobin_part"+str(i),
                                                                                                ratingstablename,
                                                                                                str(numberofpartitions),
                                                                                                0
                                                                                                ))

        else:
            cur.execute(""" CREATE TABLE {0} AS SELECT * FROM {1} WHERE MOD(ID,{2})= {3}""".format("rrobin_part"+str(i),
                                                                                                    ratingstablename,
                                                                                                    str(numberofpartitions),
                                                                                                    i+1
                                                                                                    ))
        cur.execute(""" ALTER TABLE {} DROP COLUMN ID""".format("rrobin_part"+str(i)))

    cur.execute(""" SELECT COUNT(*) FROM {} """.format(ratingstablename))
    last_count = cur.fetchone()[0]

    cur.execute(""" DROP TABLE IF EXISTS {}""".format("rrobin_count"))
    cur.execute(""" CREATE TABLE RROBIN_COUNT(INDEX int, NUM_PARTITION int)""")
    cur.execute(""" INSERT INTO RROBIN_COUNT (INDEX,NUM_PARTITION) VALUES({0},{1})""".format(str(last_count),str(numberofpartitions)))
    openconnection.commit()
    cur.close()



def roundrobininsert(ratingstablename, userid, itemid, rating, openconnection):
    cur = openconnection.cursor()

    cur.execute(""" SELECT * FROM RROBIN_COUNT """)
    curr_idx,num_partitions = cur.fetchone()
    if (curr_idx+1)%num_partitions==0:
        cur.execute(""" INSERT INTO {0} (UserID,MovieID,Rating) VALUES({1},{2},{3})""".format("rrobin_part"+str(num_partitions-1),
                                                                                              str(userid),
                                                                                              str(itemid),
                                                                                              str(rating)))
    else:
        cur.execute(""" INSERT INTO {0} (UserID,MovieID,Rating) VALUES({1},{2},{3})""".format("rrobin_part"+str((curr_idx+1)%num_partitions -1),
                                                                                              str(userid),
                                                                                              str(itemid),
                                                                                              str(rating)))
    cur.execute(""" UPDATE RROBIN_COUNT SET INDEX={0}""".format(str(curr_idx+1)))
    cur.execute(""" INSERT INTO {0} (UserID,MovieID,Rating) VALUES({1},{2},{3})""".format(ratingstablename,
                                                                                          str(userid),
                                                                                          str(itemid),
                                                                                          str(rating)))

    openconnection.commit()
    cur.close()
    # if(curr)



def rangeinsert(ratingstablename, userid, itemid, rating, openconnection):
    cur = openconnection.cursor()

    cur.execute(""" SELECT * FROM RANGE_COUNT""")
    num_partitions = cur.fetchone()[0]
    step = 5/float(num_partitions)
    rem = rating%step
    div = rating/step

    if rem==0:
    	part_idx = int(div-1)
    else:
    	part_idx = int(div)
    if rating==0:
        cur.execute(""" INSERT INTO {0} (UserID,MovieID,Rating) VALUES({1},{2},{3})""".format("range_part0",
                                                                                          str(userid),
                                                                                          str(itemid),
                                                                                          str(rating)))
    else:
        cur.execute(""" INSERT INTO {0} (UserID,MovieID,Rating) VALUES({1},{2},{3})""".format("range_part" + str(part_idx) ,
                                                                                          str(userid),
                                                                                          str(itemid),
                                                                                          str(rating)))

    cur.execute(""" INSERT INTO {0} (UserID,MovieID,Rating) VALUES({1},{2},{3})""".format(ratingstablename,
                                                                                          str(userid),
                                                                                          str(itemid),
                                                                                          str(rating)))

    openconnection.commit()
    cur.close()


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
    con.close()

def deletepartitionsandexit(openconnection):
    cur = openconnection.cursor()
    cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
    l = []
    for row in cur:
        l.append(row[0])
    for tablename in l:
        cur.execute("drop table if exists {0} CASCADE".format(tablename))

    cur.close()

def deleteTables(ratingstablename, openconnection):
    try:
        cursor = openconnection.cursor()
        if ratingstablename.upper() == 'ALL':
            cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
            tables = cursor.fetchall()
            for table_name in tables:
                cursor.execute('DROP TABLE %s CASCADE' % (table_name[0]))
        else:
            cursor.execute('DROP TABLE %s CASCADE' % (ratingstablename))
        openconnection.commit()
    except psycopg2.DatabaseError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
    except IOError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
    finally:
        if cursor:
            cursor.close()


conn = getOpenConnection(dbname="dds_assignment")

loadRatings(RATINGS_TABLE,INPUT_FILE_PATH,conn)
rangePartition(RATINGS_TABLE,9,conn)
roundRobinPartition(RATINGS_TABLE,9,conn)
roundrobininsert(RATINGS_TABLE, 2, 109, 4.8, conn)
roundrobininsert(RATINGS_TABLE, 2, 109, 4.8, conn)
roundrobininsert(RATINGS_TABLE, 2, 109, 4.8, conn)
roundrobininsert(RATINGS_TABLE, 2, 109, 4.8, conn)
roundrobininsert(RATINGS_TABLE, 2, 109, 4.8, conn)

rangeinsert(RATINGS_TABLE, 10, 109, 4.5, conn)
rangeinsert(RATINGS_TABLE, 10, 109, 4.0, conn)
rangeinsert(RATINGS_TABLE, 10, 109, 5.0, conn)
rangeinsert(RATINGS_TABLE, 10, 109, 1.0, conn)
rangeinsert(RATINGS_TABLE, 10, 109, 0.0, conn)




