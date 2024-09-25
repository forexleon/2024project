#!/usr/bin/python2.7
#
# Interface for the assignement
#

import psycopg2

def getOpenConnection(user='postgres', password='1234', dbname='postgres'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")


def Load_Ratings(ratingstablename, ratingsfilepath, openconnection):
    cur = openconnection.cursor()
    cur.execute("DROP TABLE IF EXISTS " + ratingstablename)
    cur.execute("CREATE TABLE " + ratingstablename + " (row_id serial primary key, UserID INT, temp1 VARCHAR(10), MovieID INT, temp3 VARCHAR(10), Rating REAL, temp5 VARCHAR(10), Timestamp INT)")
    
    loadout = open(ratingsfilepath, 'r')
    cur.copy_from(loadout, ratingstablename, sep=':', columns=('UserID', 'temp1', 'MovieID', 'temp3', 'Rating', 'temp5', 'Timestamp'))
    cur.execute("ALTER TABLE " + ratingstablename + " DROP COLUMN temp1, DROP COLUMN temp3, DROP COLUMN temp5, DROP COLUMN Timestamp")
    
    cur.close()


def Range_Partition(ratingstablename, numberofpartitions, openconnection):
    cur = openconnection.cursor()
    global RangePart
    RangePart = numberofpartitions

    Range = 5.0 / numberofpartitions
    i = 0
    Demo = 0

    while Demo < 5.0:
        if Demo == 0:
            cur.execute("DROP TABLE IF EXISTS range_part" + str(i))
            cur.execute("CREATE TABLE range_part" + str(i) + " AS SELECT * FROM " + ratingstablename + " WHERE Rating>=" + str(Demo) + " AND Rating<=" + str(Demo + Range) + ";")
            i += 1
            Demo = Demo + Range
        else:
            cur.execute("DROP TABLE IF EXISTS range_part" + str(i))
            cur.execute("CREATE TABLE range_part" + str(i) + " AS SELECT * FROM " + ratingstablename + " WHERE Rating>" + str(Demo) + " AND Rating<=" + str(Demo + Range) + ";")
            i += 1
            Demo = Demo + Range

    cur.close()


def RoundRobin_Partition(ratingstablename, numberofpartitions, openconnection):
    con = openconnection
    cur = con.cursor()
    create_robinsert_table = "CREATE TABLE robbinsert(partition_number integer, totalpartition_number integer);"
    
    cur.execute(create_robinsert_table)
    cur.execute("INSERT INTO robbinsert VALUES({0}, {1})".format(0, numberofpartitions))
    
    for i in range(0, numberofpartitions):
        cur.execute("CREATE TABLE robin_part{2} AS SELECT* FROM (SELECT*,ROW_NUMBER() OVER() FROM {0}) AS temp WHERE (ROW_NUMBER-1)%{1}={2}".format(ratingstablename, numberofpartitions, i))
    
    con.commit()
    cur.close()


def RoundRobin_Insert(ratingstablename, userid, itemid, rating, openconnection):
    con = openconnection
    cur = con.cursor()
    cur.execute("SELECT * FROM robbinsert")
    i = cur.fetchone()
    j = i[0]
    cur.execute("INSERT INTO robin_part{0} VALUES({1},{2},{3})".format(j, userid, itemid, rating))
    j = (j + 1) % i[1]
    cur.execute("UPDATE robbinsert SET partition_number={0}".format(j))
    con.commit()


def Range_Insert(ratingstablename, userid, itemid, rating, openconnection):
    cur = openconnection.cursor()
    global RangePart
    
    range2 = 5.0 / RangePart
    Lower = 0
    partitionnumber = 0
    Upper = range2
    
    while Lower < 5.0:
        if Lower == 0:
            if rating >= Lower and rating <= Upper:
                break
            partitionnumber = partitionnumber + 1
            Lower = Lower + range2
            Upper = Upper + range2
        else:
            if rating > Lower and rating <= Upper:
                break
            partitionnumber = partitionnumber + 1
            Lower = Lower + range2
            Upper = Upper + range2

    cur.execute("INSERT INTO range_part" + str(partitionnumber) + "(UserID, MovieID, Rating) VALUES (%s, %s, %s)", (userid, itemid, rating))

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
