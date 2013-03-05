
'''
The dones module can be used to mark whether a key is "done" and check whether
a key has been marked "done".  Keys can also be unmarked, so that they are no
longer "done".  Also, all keys can be unmarked by clearing the Dones.
Keys are kept in their own namespace to avoid conflicts with other
sets of other keys and to make it easy to implement clearing.
'''


import contextlib
import functools
import json
import os
import time
import urlparse

import MySQLdb


# A mysql db url, like 'mysql://username:password@host/database'
DONES_DB_URL = os.environ.get('DONES_DB_URL')


DONES_CACHE = {}
def get(ns, dburl=None):
    '''
    Get a default dones object for ns.  If no dones object exists for ns yet,
    a DbDones object will be created, cached, and returned.
    '''
    if dburl is None:
        dburl = DONES_DB_URL

    cache_key = (ns, dburl)
    if ns not in DONES_CACHE:
        dones_ns = 'dones_{}'.format(ns)
        DONES_CACHE[cache_key] = DbDones(ns=dones_ns, dburl=dburl)

    return DONES_CACHE[cache_key]


class DbDones(object):
    '''
    DbDones are implemented using a database-backed key store.  This means
    it should be relatively high performance, scale to millions of keys,
    and handle concurrent reading and writing well.
    '''
    def __init__(self, ns, dburl=None):
        '''
        ns: a namespace (string) used to keep these dones separate from other
        ones.  ns should be suitable for a table name, e.g. 'my_app_dones'.
        '''
        dburl = dburl if dburl is not None else DONES_DB_URL
        # a function that returns an open db connection
        open_conn = functools.partial(open_url, dburl,
                                      retries=1, sleep=1.0)
        # a function that returns a context manager for opening and closing 
        # a connection
        connect = make_closing_connect(open_conn)
        k = KStore(connect, ns=ns)
        self.k = k
        self.ready = False

    def _get_k(self):
        '''
        Accessing self.k indirectly allows for creating the kvstore table
        if necessary.
        '''
        if not self.ready:
            self.k.create() # create table if it does not exist.
            self.ready = True

        return self.k

    def clear(self):
        '''
        Remove all existing done markers.  Useful for resetting the dones or
        cleaning up when all done.
        '''
        self._get_k().drop()
        self.ready = False # _get_k() will create table next time.
        
    def done(self, key):
        '''
        return True iff key is marked done.
        '''
        return self._get_k().exists(key)

    def mark(self, key):
        '''
        Mark a key as done.
        '''
        return self._get_k().add(key)

    def unmark(self, key):
        return self._get_k().remove(key)

    def all_done(self, keys):
        '''
        Return: True iff all the keys are done.
        '''
        # implementation note: use generator b/c any/all are short-circuit functions
        return all(self.done(key) for key in keys)

    def any_done(self, keys):
        '''
        Return: True iff any of the keys are done.
        '''
        # implementation note: use generator b/c any/all are short-circuit functions
        return any(self.done(key) for key in keys)


class FileJSONAppendDones(object):
    '''
    FileJSONAppendDones are implemented by appending json-serialized keys to a
    flat file.  This comes with some caveats:

    - It should be relatively slow, due to filesystem access.
    - Reading (checking if a key is done) should be linear in the number of
      marks (and unmarks) made.  This means checking N keys in a file with
      M marks should take O(N*M) time.  In practice this starts to get slow
      after a few hundred marks.
    - Writing (marking or unmarking a key) should be much faster (constant
      time?).
    - It should handle concurrent writing somewhat well, since the writes are
      sinlge line writes that are flushed right away.  Data corruption might be
      possible on a filesystem, like NFS, that does not handle concurrent
      writes well.  However, in my experience flushed appends work well.
    - Performance-wise, I would recommend it for thousands, not millions, of
      keys.  It depends a lot on your usage patterns.  In particular, lots
      of single reads will be expensive.
    - When concurrent reading (checking if a key is marked done) and writing
      (marking a key done) is occurring, there are no consistency guarantees,
      since reads and writes are not tranactional.

    It is mostly useful for simple situations where you do not want to or can
    not set up a database or if you want to keep the metadata (dones) next to
    the data files they are associated with.
    '''
    def __init__(self, filename):
        '''
        :param filename: where to store the dones.  This file represents a 
        namespace to keep these dones separate from other dones.  This makes it
        easy to clear out the dones without affecting other dones and to avoid
        conflicting keys.
        '''
        self.path = filename

    def _serialize(self, key):
        return json.dumps(key)

    def _done_line(self, key):
        return 'done ' + self._serialize(key) + '\n'

    def _undone_line(self, key):
        return 'undo ' + self._serialize(key) + '\n'

    def _persist(self, msg):
        with open(self.path, 'a') as fh:
            fh.write(msg)
            fh.flush()

    def compact(self):
        '''
        Not implemented.  This would rewrite the dones file removing any
        keys that have been marked undone (and not remarked as done.)
        '''
        pass

    def clear(self):
        '''
        Remove all existing done markers and the file used to store the dones.
        '''
        if os.path.exists(self.path):
            os.remove(self.path)

    def mark(self, key):
        '''
        Mark a key as done.

        :param key: a json-serializable object.
        '''
        self._persist(self._done_line(key))

    def unmark(self, key):
        '''
        Mark a key as not done.  This is useful after marking a key as done
        to indicate that it is no longer done, since by default a key is
        not done unless explicitly marked as done.

        :param key: a json-serializable object.
        '''
        self._persist(self._undone_line(key))

    def done(self, key):
        '''
        return True iff key is marked done.

        :param key: a json-serializable object.
        '''
        # key is not done b/c the file does not even exist yet
        if not os.path.exists(self.path):
            return False

        is_done = False
        done_line = self._done_line(key)
        undone_line = self._undone_line(key)
        with open(self.path) as fh:
            for line in fh:
                if line == done_line:
                    is_done = True
                elif line == undone_line:
                    is_done = False

        return is_done

    def are_done(self, keys):
        '''
        Return a list of boolean values corresponding to whether or not each
        key in keys is marked done.  This method can be faster than
        individually checking each key, depending on how many keys you
        want to check.

        :param keys: a list of json-serializable keys
        '''
        # No keys are done b/c the file does not even exist yet.
        if not os.path.exists(self.path):
            return [False] * len(keys)

        done_lines = set([self._done_line(key) for key in keys])
        undone_lines = set([self._undone_line(key) for key in keys])
        status = {}
        with open(self.path) as fh:
            for line in fh:
                if line in done_lines:
                    # extract serialized key
                    status[line[5:-1]] = True
                elif line in undone_lines:
                    status[line[5:-1]] = False
        serialized_keys = [self._serialize(key) for key in keys]
        return [status.get(sk, False) for sk in serialized_keys]

    def all_done(self, keys):
        '''
        Return: True iff all the keys are done.

        :param keys: a list of json-serializable keys
        '''
        return all(self.done(key) for key in keys)

    def any_done(self, keys):
        '''
        Return: True iff any of the keys are done.

        :param keys: a list of json-serializable keys
        '''
        return any(self.are_done(keys))



#################
# MySQL Functions


def open_conn(host, db, user, password, retries=0, sleep=0.5):
    '''
    Return an open mysql db connection using the given credentials.  Use
    `retries` and `sleep` to be robust to the occassional transient connection
    failure.

    retries: if an exception when getting the connection, try again at most this many times.
    sleep: pause between retries for this many seconds.  a float >= 0.
    '''
    assert retries >= 0

    try:
        return MySQLdb.connect(host=host, user=user, passwd=password, db=db)
    except Exception:
        if retries > 0:
            time.sleep(sleep)
            return open_conn(host, db, user, password, retries - 1, sleep)
        else:
            raise


def open_url(url, retries=0, sleep=0.5):
    '''
    Open a mysql connection to a url.  Note that if your password has
    punctuation characters, it might break the parsing of url.

    url: A string in the form "mysql://username:password@host.domain/database"
    '''
    return open_conn(retries=retries, sleep=sleep, **parse_url(url))


def parse_url(url):
    result = urlparse.urlsplit(url)
    # path is '', '/', or '/<database>'. Remove any leading slash to get the
    # database.
    db = result.path[1:]
    return {'host': result.hostname, 'db': db, 'user': result.username,
            'password': result.password}


#####################
# Key Store Functions


def make_closing_connect(open_conn):
    '''
    Return a function which opens a connection and then returns a context
    manager that returns that connection when entering a context and then
    closes it when the context is exited.

    open_conn: a function which returns an open DBAPI 2.0 connection.
    '''
    def connect():
        return contextlib.closing(open_conn())

    return connect


@contextlib.contextmanager
def doTransaction(conn, start=True, startSQL='START TRANSACTION'):
    '''
    wrap a connection in a transaction.  starts a transaction, yields the conn, and then if an exception occurs, calls rollback().  otherwise calls commit().
    start: if True, executes 'START TRANSACTION' sql before yielding conn.  Useful for connections that are autocommit by default.
    startSQL: override if 'START TRANSACTION' does not work for your db server.
    '''
    try:
        if start:
            executeSQL(conn, startSQL)
        yield conn
    except:
        if conn is not None:
            conn.rollback()
        raise
    else:
        conn.commit()


@contextlib.contextmanager
def doCursor(conn):
    '''
    create and yield a cursor, closing it when done.
    '''
    cursor = conn.cursor()
    try:
        yield cursor
    finally:
        cursor.close()


def selectSQL(conn, sql, args=None):
    '''
    sql: a select statement
    args: if sql has parameters defined with either %s or %(key)s then args should be a either list or dict of parameter
    values respectively.
    returns a tuple of rows, each of which is a tuple.
    '''
    with doCursor(conn) as cursor:
        cursor.execute(sql, args)
        results = cursor.fetchall()
        return results


def insertSQL(conn, sql, args=None):
    '''
    args: if sql has parameters defined with either %s or %(key)s then args should be a either list or dict of parameter
    values respectively.
    returns the insert id
    '''
    with doCursor(conn) as cursor:
        cursor.execute(sql, args)
        id = conn.insert_id()
        return id


def executeSQL(conn, sql, args=None):
    '''
    args: if sql has parameters defined with either %s or %(key)s then args should be a either list or dict of parameter
    values respectively.
    executes sql statement.  useful for executing statements like CREATE TABLE or RENAME TABLE,
    which do not have an result like insert id or a rowset.
    returns: the number of rows affected by the sql statement if any.
    '''
    with doCursor(conn) as cursor:
        numRowsAffected = cursor.execute(sql, args)
        return numRowsAffected


class KStore(object):
    '''
    A key store backed by a mysql database.  Upon first using a namespace, call
    create() to initialize the table.  When done using a namespace, call drop()
    to drop the table.
    '''
    def __init__(self, connect, ns='key_store'):
        '''
        connect: A function which returns a context manager for getting a
        DBAPI 2.0 connection.  
        
        An example which returns a context manager that opens and closes a
        connection:

            @contextlib.contextmanager
            def myconnect():
                try:
                    conn = MySQLdb.connect(host=h, user=u, passwd=p, db=d)
                    yield conn
                finally:
                    conn.close()

        Which would be used like:

            with myconnect() as conn:
                do_something(conn)

        Typically the manager would either open and
        close a connection (to avoid maintaining a persistent connection to the
        database) or return the same open connection every time (to avoid
        opening and closing connections too rapidly) or return a connection
        from a connection pool (to avoid having too many open connections).
        ns: The "namespace" of the keys.  Should be a valid mysql table name.
        Defaults to 'key_store'.
        '''
        self.connect = connect
        self.table = ns

    def create(self):
        sql = '''CREATE TABLE IF NOT EXISTS {table} (
                 id INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
                 name VARCHAR(255) NOT NULL UNIQUE KEY,
                 create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                 INDEX key_index (name) 
                 )'''.format(table=self.table)
        with self.connect() as conn:
            with doTransaction(conn):
                executeSQL(conn, sql)
        return self

    def drop(self):
        with self.connect() as conn:
            with doTransaction(conn):
                executeSQL(conn, 'DROP TABLE IF EXISTS ' + self.table)
        return self

    def reset(self):
        return self.drop().create()

    def exists(self, key):
        encodedKey = json.dumps(key)
        with self.connect() as conn:
            sql = 'SELECT id FROM ' + self.table + ' WHERE name = %s'
            results = selectSQL(conn, sql, args=[encodedKey])
            return bool(results) # True if there are any results, False otherwise.

    def add(self, key):
        '''
        add key to the namespace.  it is fine to add a key multiple times.
        '''
        encodedKey = json.dumps(key)
        with self.connect() as conn:
            with doTransaction(conn):
                sql = 'INSERT IGNORE INTO ' + self.table + ' (name) VALUES (%s)'
                return insertSQL(conn, sql, args=[encodedKey])

    def remove(self, key):
        '''
        remove key from the namespace.  it is fine to remove a key multiple times.
        '''
        encodedKey = json.dumps(key)
        sql = 'DELETE FROM ' + self.table + ' WHERE name = %s'
        with self.connect() as conn:
            with doTransaction(conn):
                return executeSQL(conn, sql, args=[encodedKey])


