import configparser
import mysql.connector
import logging

logging.basicConfig(level=logging.INFO)

class DatabaseManager:
    def __init__(self,
                 config_path: str = '../config.config.ini',
                 secrets_path: str = '../config/secrets.ini') -> None:
        secrets = configparser.ConfigParser()
        secrets.read(secrets_path)
        try:
            self._conn = mysql.connector.connect(
                host=secrets.get('mysql', 'host'),
                user=secrets.get('mysql', 'user'),
                password=secrets.get('mysql', 'password'),
                database=secrets.get('mysql', 'database')
            )
            self.cursor = self._conn.cursor()
            logging.info("MySQL connection established")
        except mysql.connector.Error as err:
            logging.error(f"Error connecting to MySQL: {err}")
            raise

    def _close_connection(self) -> None:
        if self._conn is not None:
            self.cursor.close()
            self._conn.close()
            logging.info("MySQL connection closed")
        else:
            logging.info("No connection exists")
            raise

    def _table_exists(self,
                      table_name: str) -> bool:
        self.cursor.execute(f"SHOW TABLES LIKE '{table_name}'")
        return self.cursor.fetchone() is not None

    def _vector_exists(self,
                       table_name: str,
                       vector_id: int) -> bool:
        self.cursor.execute(f'SELECT vector_id FROM `{table_name}` WHERE '
                            f'vector_id = %s', (vector_id,))
        return self.cursor.fetchone() is not None

    def _column_exists(self,
                            table_name: str,
                            date: str) -> bool:
        self.cursor.execute(f'''
            SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS WHERE 
            TABLE_NAME = `{table_name}` AND COLUMN_NAME = `{date}`
            ''')
        return self.cursor.fetchone()[0] > 0

    def _values_match(self, table_name: str,
                      vector_id: int,
                      date: str,
                      value: float) -> bool:
        date_column = f'`{date}`'
        self.cursor.execute(f'''
            SELECT {date_column} FROM `{table_name}` WHERE vector_id = %s
        ''', (vector_id,))
        current_value = self.cursor.fetchone()[0]
        return current_value == value