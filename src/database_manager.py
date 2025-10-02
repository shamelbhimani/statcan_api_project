import configparser
import mysql.connector
import logging
from typing import Any
from pathlib import Path


class DatabaseManager:
    """
    A class object to manage the MySQL database, including establishing and
    closing connections, creating tables, and inserting and updating tables.
    """

    def __init__(self, database_config_path: Path) -> None:
        """
        Initializes the DatabaseManager object.

        :param database_config_path: Path to the database configuration file.

        :except FileNotFoundError: Raises an exception if the database
        configuration file does not exist at the specified path.
        :except (configparser.NoSectionError, configparser.NoOptionError):
        Raises an exception if there is no section or option in the database
        configuration file.
        :except mysql.connector.Error: Raises an exception if an error occurs
        while connecting to the database.
        """
        config = configparser.ConfigParser()
        try:
            config.read(database_config_path)
            logging.info("Database configuration completed.")
        except FileNotFoundError:
            logging.error(
                f"Database configuration file not found at path:"
                f" {database_config_path}."
            )
        except (configparser.NoSectionError, configparser.NoOptionError) as e:
            logging.error(
                f"Database configuration error at path: {database_config_path}: {e}"
            )

        self.stats = {
            "tables_created": 0,
            "vectors_added": 0,
            "columns_added": 0,
            "values_added": 0,
            "values_updated": 0,
        }

        try:
            self._conn = mysql.connector.connect(
                host=config.get("mysql", "host"),
                user=config.get("mysql", "user"),
                password=config.get("mysql", "password"),
                database=config.get("mysql", "database"),
            )
            self.cursor = self._conn.cursor()
            logging.info(
                f"MySQL connection established to {config.get('mysql', 'database')}"
            )
            logging.info(f"Logged in as {config.get('mysql', 'user')}")
        except mysql.connector.Error as err:
            logging.error(f"Error connecting to MySQL: {err}")
            raise
        except (configparser.NoSectionError, configparser.NoOptionError) as e:
            logging.error(f"Configuration error at path {database_config_path}: {e}")

    def close_connection(self) -> None:
        """
        Closes the connection to the database.

        :raise Raises a warning if connection could not be closed due to a
        non-existent connection or if the connection was already closed.
        """
        if self._conn is not None:
            self.cursor.close()
            self._conn.close()
            logging.debug("MySQL connection closed")
        else:
            logging.warning(
                "Attempted to close a non-existent or already closed connection."
            )
            raise

    def _table_exists(self, table_name: str) -> bool:
        """
        Checks the database to see if the specified table exists.

        :param table_name: Name of the table to check.
        :return: Bool indicating if the specified table exists.
        """
        query = f"""
                SHOW TABLES LIKE '{table_name}'
                """
        self.cursor.execute(query)
        return self.cursor.fetchone() is not None

    def _vector_exists(self, table_name: str, vector_id: int) -> bool:
        """
        Checks the database to see if the specified vector exists in the
        specified table.

        :param table_name: Name of the table to check.
        :param vector_id: ID of the vector to check.
        :return: Bool indicating if the specified vector exists.
        """
        query = f"""
                SELECT vector_id
                FROM `{table_name}`
                    WHERE vector_id = %s
                """
        self.cursor.execute(query, (vector_id,))
        return self.cursor.fetchone() is not None

    def _column_exists(self, table_name: str, date: str) -> bool:
        """
        Checks the database to see if the specified column exists in the
        specified table.

        :param table_name: Name of the table to check.
        :param date: Name of the column to check.
        :return: Bool indicating if the specified column exists.
        """
        query = """
            SELECT COUNT(*) 
            FROM INFORMATION_SCHEMA.COLUMNS 
            WHERE TABLE_NAME = %s AND 
            COLUMN_NAME = %s 
            """

        self.cursor.execute(query, (table_name, date))
        return self.cursor.fetchone()[0] > 0

    def _values_match(
        self, table_name: str, vector_id: int, date: str, value: float
    ) -> bool:
        """
        Checks the database to see if the specified value exists in the
        specified table at the specified vector ID row at the specified date
        column.

        :param table_name: Name of the table to check.
        :param vector_id: ID of the vector to check.
        :param date: Name of the column to check.
        :param value: Value to check.
        :return: Bool indicating if the specified value exists.
        """
        date_column = f"`{date}`"
        query = f"""
                SELECT {date_column} 
                FROM `{table_name}`
                    WHERE vector_id = %s
                """
        self.cursor.execute(query, (vector_id,))
        current_value = self.cursor.fetchone()[0]
        return current_value == value

    def _create_table(self, table_name: str, definition: str = None) -> None:
        """
        Create a table with the specified definition.

        :param table_name: Name of the table to create.
        :param definition: Table definition.
        """
        if not self._table_exists(table_name):
            logging.info(f"Creating table {table_name} with definition: {definition}")
            definition = definition.replace("'", "''")
            query = f"""
                    CREATE TABLE `{table_name}` (
                    vector_id BIGINT NOT NULL PRIMARY KEY,
                    definition TEXT )
                        COMMENT = %s
                    """
            self.cursor.execute(query, (definition,))
            self.stats["tables_created"] += 1
            logging.info(f"Table {table_name} created.")
        else:
            logging.info(f"Table {table_name} already exists. Skipping creation...")

    def _add_vector(
        self, table_name: str, vector_id: int, definition: str = None
    ) -> None:
        """
        Add a vector and its definition to the specified table.

        :param table_name: Name of the table to open.
        :param vector_id: ID of the vector to add.
        :param definition: Vector definition.
        """
        logging.info(f"Adding vector {vector_id} to table {table_name}...")
        params = (vector_id, definition)
        query = f"""
                INSERT INTO `{table_name}`
                    (vector_id, definition) VALUES (%s, %s)
                """
        self.cursor.execute(query, params)
        logging.info("Vector added.")
        self.stats["vectors_added"] += 1

    def _add_column(self, table_name: str, date: str) -> None:
        """
        Add a date column to the specified table

        :param table_name: Name of the table to open.
        :param date: Column to add.
        :return:
        """
        query = f"""
                ALTER TABLE `{table_name}`
                    ADD COLUMN `{date}` FLOAT
                """
        self.cursor.execute(query)
        logging.info("Column added.")
        self.stats["columns_added"] += 1

    def _update_value(
        self, table_name: str, vector_id: int, date: str, value: float
    ) -> None:
        """
        Value to update or add into the specified table, at the specified
        vector row, at the specified date column.

        :param table_name: Name of the table to update.
        :param vector_id: ID of the vector to update.
        :param date: Name of the column to update.
        :param value: Value to update.
        """
        query = f"""
                UPDATE `{table_name}`
                SET `{date}` = %s
                    WHERE vector_id = %s
                """
        self.cursor.execute(query, (value, vector_id))

    def _process_series(
        self, table_name: str, vector_id: int, series: dict[str, float]
    ) -> None:
        """
        Process series data in the specified table at the specified vector ID.

        :param table_name: Name of the table to open.
        :param vector_id: ID of the vector to open.
        :param series: Series to process.
        """
        for date, value in series.items():
            if not self._column_exists(table_name, date):
                logging.debug(f"Column {date} does not exist.")
                self._add_column(table_name, date)
                logging.debug(f"Adding column {date} to table {table_name}.")
                self._update_value(table_name, vector_id, date, value)
                self.stats["values_added"] += 1
                logging.debug(
                    f"Value '{value}' added to column {date} in "
                    f"table {table_name} at vector {vector_id}."
                )
            else:
                logging.debug(f"Column {date} already exists in table {table_name}.")
                if not self._values_match(table_name, vector_id, date, value):
                    self._update_value(table_name, vector_id, date, value)
                    self.stats["values_updated"] += 1
                    logging.debug(
                        f"Value '{value}' updated on column {date} "
                        f"in table {table_name} at vector"
                        f" {vector_id}."
                    )
                else:
                    logging.debug(
                        f"Value for vector {vector_id} on {date} "
                        f"already matches. No update required. "
                        f"Skipping..."
                    )

    def _process_vector(
        self,
        table_name: str,
        vector_id: int,
        series: dict[str, float],
        definitions: dict[int, str],
    ) -> None:
        """
        Process the specified vector data in the specified table.

        :param table_name: Name of the table to open.
        :param vector_id: ID of the vector to process.
        :param series: Series to process.
        :param definitions: Definition of vector.
        """
        if not self._vector_exists(table_name, vector_id):
            vector_definition = definitions.get(vector_id, "No Definition")
            if vector_definition == "No Definition":
                logging.info(f"No definition for {vector_id}")
            else:
                logging.info(
                    f"Vector definition found for {vector_id}: {vector_definition}"
                )
            self._add_vector(
                table_name=table_name, vector_id=vector_id, definition=vector_definition
            )
            logging.debug(f"Adding vector {vector_id} to table {table_name}.")
        self._process_series(table_name=table_name, vector_id=vector_id, series=series)
        logging.debug(f"Series added to table {table_name} at vector {vector_id}.")

    def _log_stats(self) -> None:
        """
        Log statistics about the processed data.
        :return:
        """
        logging.info("=-=-=-=-= Summary of Process =-=-=-=-=")
        logging.info(f"Tables Created:      {self.stats['tables_created']}")
        logging.info(f"Vectors Added:       {self.stats['vectors_added']}")
        logging.info(f"Columns Added:       {self.stats['columns_added']}")
        logging.info(f"Values Added:        {self.stats['values_added']}")
        logging.info(f"Values Updated:      {self.stats['values_updated']}")
        logging.info("=-=-=-=-=-=-=-=-= End =-=-=-=-=-=-=-=-=")

    def update_database(
        self, data: dict[int, Any], definitions: dict[int, str]
    ) -> None:
        """
        Function to update the database with the specified data and
        definitions.

        :param data: A dictionary containing the data to be updated.
        :param definitions: A dictionary containing the definitions of tables
        and vectors.
        """
        logging.info("Starting database update...")
        for product_id, vectors in data.items():
            table_name = f"{product_id}"
            product_definition = definitions.get(product_id, "No Definition")
            if product_definition == "No Definition":
                logging.info(f"No definition found for product {product_id}")
            else:
                logging.info(
                    f"Product {product_id} definition found: {product_definition}"
                )
            self._create_table(table_name, product_definition)

            for vector_id, series in vectors.items():
                self._process_vector(table_name, vector_id, series, definitions)

        self._conn.commit()
        logging.info("Database updates completed.")
        self._log_stats()


def run_process(
    data: dict[int, Any], definitions: dict[int, str], database_config_path: Path
) -> None:
    """
    Secondary to main.py. Function to process the data.

    :param data: Data to be processed.
    :param definitions: A dictionary containing the definitions of tables
    and vectors.
    :param database_config_path: Path to the database configuration file.

    :except mysql.connector.Error: Raises a critical error if an error occurs in
    the process. Database connection will be rolled back.
    """
    db = None
    try:
        db = DatabaseManager(database_config_path)
        db.update_database(data, definitions)
        db.close_connection()
    except mysql.connector.Error as err:
        logging.critical(f"A critical error occurred in the database manager: {err}")
        if db and db._conn.is_connected():
            db._conn.rollback()
            logging.info("Database transactions was rolled back.")
    finally:
        if db:
            db.close_connection()
        logging.info("Database update process completed.")
