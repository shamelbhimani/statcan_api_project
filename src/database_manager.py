import configparser
import mysql.connector
import logging
from typing import Any

logging.basicConfig(level=logging.INFO)

class DatabaseManager:
    def __init__(self,
                 database_config_path: str = '../config/secrets.ini') -> None:
        config = configparser.ConfigParser()
        config.read(database_config_path)
        self.stats = {
            "tables_created": 0,
            "vectors_added": 0,
            "columns_added": 0,
            "values_updated": 0,
        }
        try:
            self._conn = mysql.connector.connect(
                host=config.get('mysql', 'host'),
                user=config.get('mysql', 'user'),
                password=config.get('mysql', 'password'),
                database=config.get('mysql', 'database')
            )
            self.cursor = self._conn.cursor()
            logging.info("MySQL connection established")
        except mysql.connector.Error as err:
            logging.error(f"Error connecting to MySQL: {err}")
            raise

    def close_connection(self) -> None:
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

    def _create_table(self, table_name: str,
                      definition: str = None) -> None:
        if not self._table_exists(table_name):
            logging.info(f"Creating table {table_name} for {definition}")
            self.cursor.execute(f"""
                CREATE TABLE `{table_name}` (
                    vector_id BIGINT NOT NULL PRIMARY KEY,
                    definition TEXT
                ) COMMENT = `{definition}`
                                """)
            self.stats["tables_created"] += 1
        else:
            logging.info(f"Table {table_name} already exists")

    def _add_vector(self, table_name: str,
                    vector_id: int,
                    definition: str = None) -> None:
        logging.info(f"Adding vector {vector_id} to table {table_name}")
        self.cursor.execute(f"""
            INSERT INTO `{table_name}` (vector_id, definition)` VALUES (
            %s, %s)""", (vector_id, definition))
        self.stats["vectors_added"] += 1

    def _add_column(self, table_name:
    str, date: str):
        logging.info(f"Adding column {date} to table {table_name}")
        self.cursor.execute(f"""
            ALTER TABLE `{table_name}` ADD COLUMN `{date}` FLOAT
                            """)
        self.stats["columns_added"] += 1

    def _update_value(self, table_name: str,
                   vector_id: int,
                   date: str,
                   value: float) -> None:
        logging.info(f"Adding value {value} to table {table_name}")
        self.cursor.execute(f"""
            UPDATE `{table_name}` SET `{date}` = %s WHERE vector_id = %s
                            """, (value, vector_id))
        self.stats["values_updated"] += 1

    def _process_series(self,
                        table_name: str,
                        vector_id: int,
                        series: dict[str, float]) -> None:
        for date, value in series.items():
            if not self._column_exists(table_name, date):
                self._add_column(table_name, date)

            if not self._values_match(table_name, vector_id, date, value):
                self._update_value(table_name, vector_id, date, value)

    def _process_vector(self,
                        table_name: str,
                        vector_id: int,
                        series: dict[str, float],
                        definitions: dict[int, str]) -> None:
        if not self._vector_exists(table_name, vector_id):
            vector_definition = definitions.get(vector_id, 'N/A')
            self._add_vector(table_name, vector_id, vector_definition)

    def _log_stats(self) -> None:
        logging.info("=-=-=-=-= Summary of Process =-=-=-=-=")
        logging.info(f"Tables Created: {self.stats['tables_created']}")
        logging.info(f"Vectors Added: {self.stats['vectors_added']}")
        logging.info(f"Columns Added: {self.stats['columns_added']}")
        logging.info(f"Values Updated: {self.stats['values_updated']}")
        logging.info("=-=-=-=-= End =-=-=-=-=")

    def update_database(self,
                        data: dict[int, Any],
                        definitions: dict[int, str]) -> None:
        for product_id, vectors in data.items():
            table_name = f'{product_id}'
            product_definition = definitions.get(product_id, 'N/A')
            self._create_table(table_name, product_definition)

            for vector_id, series in vectors.items():
                self._process_series(table_name, vector_id, series, definitions)

        self._conn.commit()
        logging.info("Database updates completed.")
        self._log_stats()

def run_process(data: dict[int, Any],
                definitions: dict[int, str]) -> None:
    try:
        db = DatabaseManager()
        db._update_database(data, definitions)
        db._close_connection()
    except mysql.connector.Error as err:
        logging.error(f"A critical error occured in the database manager:"
                      f" {err}")