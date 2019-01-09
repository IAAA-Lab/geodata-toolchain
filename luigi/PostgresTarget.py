import logging
import datetime
import luigi
from DB import Db

logger = logging.getLogger('luigi-interface')

try:
    import psycopg2
    import psycopg2.errorcodes
    import psycopg2.extensions
except ImportError:
    logger.warning("Loading postgres module without psycopg2 installed. Will crash at runtime if postgres functionality is used.")


class PostgresTarget(luigi.Target):
    """
    Target for a resource in Postgres.

    This will rarely have to be directly instantiated by the user.
    """
    marker_table = luigi.configuration.get_config().get('postgres', 'marker-table', 'table_updates')

    # If not supplied, fall back to default Postgres port
    DEFAULT_DB_PORT = 5432

    # Use DB side timestamps or client side timestamps in the marker_table
    use_db_timestamps = True

    def __init__(self, db: Db):
        """
        Args:
            host (str): Postgres server address. Possibly a host:port string.
            database (str): Database name
            user (str): Database user
            password (str): Password for specified user
            update_id (str): An identifier for this data set
            port (int): Postgres server port.

        """
        if ':' in db.host:
            self.host, self.port = db.host.split(':')
        else:
            self.host = db.host
            self.port = db.port or self.DEFAULT_DB_PORT
        self.database = db.database
        self.user = db.user
        self.password = db.password
        self.table = db.table
        self.update_id = db.update_id

    def create_marker_table(self):
        """
        Create marker table if it doesn't exist.

        """
        if self.connection == None:
            self.connect()

        self.connection.autocommit = True

        with self.connection.cursor() as cur:
            sql = """ CREATE TABLE {marker_table} (
                    update_id TEXT PRIMARY KEY,
                    target_table TEXT,
                    inserted TIMESTAMP);
                """.format(marker_table=self.marker_table)

            try:
                cur.execute(sql)
            except psycopg2.ProgrammingError as e:
                if e.pgcode == psycopg2.errorcodes.DUPLICATE_TABLE:
                    pass
    
    def touch(self):
        """
        Mark this update as complete.

        """
        self.create_marker_table()

        with self.connection.cursor() as cur:            
            cur.execute("""UPDATE {marker_table} SET update_id=%s, target_table=%s, inserted=%s WHERE update_id = (SELECT update_id FROM table_updates ORDER BY inserted desc limit 1);
            """.format(marker_table=self.marker_table),
                (self.update_id, self.table,
                datetime.datetime.now()))
        

    def connect(self):
        """
        Get a psycopg2 connection object to the database where the table is.
        """
        
        with psycopg2.connect(
           port = self.port,
           database = self.database,
           user = self.user,
           password = self.password) as connection:
               connection.set_client_encoding('utf-8')
               self.connection = connection

        return self    

    def open(self, mode):
        raise NotImplementedError("Cannot open() PostgresTarget")
    
    def exists(self, connection=None):
        """
        Check if the output is created and so the task is completed
        """
        
        if connection is None:
            self.connect().connection.autocommit = True
            
        with self.connection.cursor() as cursor:
            try:
                cursor.execute("""SELECT * FROM {} WHERE update_id={} LIMIT 1""".format(self.marker_table, self.update_id))
                row = cursor.fetchone()
                return row is not None
            except:
                self.create_marker_table()
                return False

    def executeQuery(self, sql: str):
        """
        Execute query with the connection oppened previously and update marker_table
        """
        with self.connection.cursor() as cur:
            cur.execute(sql)
            self.connection.commit()
            self.result = cur.fetchall()

            self.touch()

            return self
