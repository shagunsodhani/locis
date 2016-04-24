import os
import utils
import MySQLdb

def connect(config_path):

    '''Open database connection and return conn object to perform database queries'''
    config = utils.configParser(config_path)
    host = config['mysql.host']
    user = config['mysql.username']
    passwd = config['mysql.password']
    db = config['mysql.database']

    try:
        conn=MySQLdb.connect(host, user, passwd, db)
        return conn
    except MySQLdb.Error, e:
        print "ERROR %d IN CONNECTION: %s" % (e.args[0], e.args[1])


def write(sql,cursor,conn):
    '''Perform insert and update operations on the databse.
       Need to pass the cursor object as a parameter'''
    try:
        cursor.execute(sql)
        conn.commit()
    except MySQLdb.ProgrammingError, e:
        print "ERROR %d IN WRITE OPERATION: %s" % (e.args[0], e.args[1])
        print "LAST QUERY WAS: %s" %sql


def read(sql,cursor):
    '''Perform read operations on the databse.
       Need to pass the cursor object as a parameter'''
    try:
        cursor.execute(sql)
        result = cursor.fetchall()
        return result
    except MySQLdb.ProgrammingError, e:
        print "ERROR %d IN READ OPERATION: %s" % (e.args[0], e.args[1])
        print "LAST QUERY WAS: %s" %sql


