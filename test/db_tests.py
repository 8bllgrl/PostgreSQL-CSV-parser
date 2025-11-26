import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch
import psycopg2
from psycopg2.sql import SQL

from database_provider import connect_to_db
from jptranslations_provider import is_japanese
from sql_provider import create_table_from_df, insert_data_from_df, insert_data_from_df_with_japanese

import os
import pandas as pd
import numpy as np


def assert_table_data_has_japanese_text(table_name: str, connection):
    assert connection is not None
    assert isinstance(connection, psycopg2.extensions.connection)

    cursor = connection.cursor()

    try:
        squery = f"SELECT * FROM {table_name} WHERE _key = '%s'"
        cursor.execute(squery, (26,))
        record = cursor.fetchone()

        assert record is not None, f"Record with _key = 26 not found in table {table_name}."

        column_3_value = record[2]
        assert is_japanese(column_3_value), f"Column 3 value '{column_3_value}' does not contain Japanese text."

        print(f"Record found and column 3 contains Japanese text in table '{table_name}'.")

    except Exception as e:
        print(f"Error occurred: {e}")
    finally:
        try:
            connection.commit()
        except psycopg2.Error as commit_error:
            print(f"Error committing transaction: {commit_error}")

        try:
            cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
            print(f"Table {table_name} dropped.")
        except psycopg2.Error as drop_error:
            print(f"Error dropping table {table_name}: {drop_error}")

        if cursor:
            cursor.close()
        if connection:
            connection.close()
        print("Connection closed.")


def verify_and_compare_records(csv_file: str, expected_num_records: int, connection):
    if not os.path.exists(csv_file):
        raise FileNotFoundError(f"The CSV file {csv_file} does not exist!")

    table_name = os.path.splitext(os.path.basename(str(csv_file)))[0]

    try:
        csvdf = pd.read_csv(csv_file)

        create_table_from_df(csvdf, table_name, connection)
        insert_data_from_df(csvdf, table_name, connection)

        cursor = connection.cursor()

        squery = f"SELECT * FROM {table_name}"
        cursor.execute(squery)
        table_exists = cursor.fetchone()[0]
        assert table_exists, f"Table {table_name} was not created successfully."

        records = cursor.fetchall()
        assert len(records) > 0, f"No records found in table {table_name}."

        num_records = len(records)
        num_columns = len(cursor.description) if cursor.description else 0
        print(f"Table {table_name} has {num_records} records and {num_columns} columns.")

        assert num_records > 0, f"No records found in table {table_name}."
        assert num_records == expected_num_records
        assert num_columns == 3

        squery = f"SELECT * FROM {table_name} WHERE _key = '%s'"
        cursor.execute(squery, (26,))
        record = cursor.fetchone()
        assert record is not None, "Record with _key = 26 not found."

        matching_rows = csvdf[csvdf['key'] == "26"]
        assert len(matching_rows) > 0, "No record with _key = 26 found in the CSV."

        csv_record = csvdf[csvdf['key'] == "26"].iloc[0]

        for column, value in zip(csvdf.columns, record):
            csv_value = csv_record[column]
            assert value == csv_value, f"Value mismatch in column '{column}': Database has '{value}', CSV has '{csv_value}'"

        print(f"Database record for _key=26: {record}")
        print(f"CSV record for key=26: {csv_record.to_dict()}")

        for idx, csv_record in csvdf.iterrows():
            cursor.execute(f"SELECT * FROM {table_name} WHERE _key = %s", (str(csv_record['key']),))
            db_record = cursor.fetchone()

            assert db_record is not None, f"Record with _key = {csv_record['key']} not found in the database."

            for column in csvdf.columns:
                csv_value = str(csv_record[column])
                db_value = str(db_record[csvdf.columns.get_loc(column)])

                if csv_value.lower() == 'nan':
                    csv_value = np.nan
                if db_value.lower() == 'nan':
                    db_value = np.nan

                if pd.isna(csv_value) and pd.isna(db_value):
                    continue
                else:
                    assert csv_value == db_value, f"Value mismatch in column '{column}' for _key = {csv_record['key']}: Database has '{db_value}', CSV has '{csv_value}'"

        print(f"All records match between CSV and database for table '{table_name}'.")
        if cursor:
            cursor.close()
    except Exception as e:
        print(f"Error occurred: {e}")
        if connection:
            connection.close()
    finally:
        cursor = connection.cursor()
        cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
        print(f"Table {table_name} dropped.")
        if cursor:
            cursor.close()
        if connection:
            connection.close()


class TestDatabase(unittest.TestCase):

    def test_real_connection_to_db_integration(self):
        connection = connect_to_db()
        try:
            self.assertIsNotNone(connection)
            self.assertIsInstance(connection, psycopg2.extensions.connection)
        finally:
            if connection:
                connection.close()

    def test_insert_eng_data_from_csv(self):
        project_root = Path(__file__).resolve().parent.parent
        base_dir = project_root / 'rsrc' / 'csv'
        csv_eng_file = str(base_dir / 'eng' / 'quest' / '000' / 'ClsArc000_00021.csv')
        if not os.path.exists(csv_eng_file):
            raise FileNotFoundError(f"The CSV file {csv_eng_file} does not exist!")

        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value = mock_cursor

        csvdf = pd.read_csv(csv_eng_file)

        print(f"Number of columns in CSV: {len(csvdf.columns)}")

        table_name = os.path.splitext(os.path.basename(str(csv_eng_file)))[0]

        create_table_from_df(csvdf, table_name, mock_connection)
        mock_cursor.execute.assert_called()

        for call in mock_cursor.execute.call_args_list:
            print(f"execute called with: {call}")

        drop_table_query = SQL('DROP TABLE IF EXISTS ClsArc000_00021 CASCADE;')
        mock_cursor.execute.assert_any_call(drop_table_query)
        drop_table_query = SQL(
            '\n        CREATE TABLE ClsArc000_00021 (\n            _key TEXT, _0 TEXT, _1 TEXT\n        )\n    ')
        mock_cursor.execute.assert_any_call(drop_table_query)

        # create data from csv
        insert_data_from_df(csvdf, table_name, mock_connection)

        # reset mock
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value = mock_cursor

        select_query = f"SELECT * FROM {table_name};"
        mock_cursor.execute(select_query)
        for call in mock_cursor.execute.call_args_list:
            print(f"execute called with: {call}")

        records = mock_cursor.fetchall()

        mock_cursor.execute.assert_called()
        assert records, f"No records found in the table {table_name}!"

        print(f"Records fetched from {table_name}:")
        for record in records:
            print(record)

    def test_insert_eng_quest_data_to_real_db(self):
        connection = connect_to_db()
        assert connection is not None
        assert isinstance(connection, psycopg2.extensions.connection)

        project_root = Path(__file__).resolve().parent.parent
        base_dir = project_root / 'rsrc' / 'csv'
        csv_eng_file = str(base_dir / 'eng' / 'quest' / '000' / 'ClsArc000_00021.csv')

        verify_and_compare_records(csv_eng_file, expected_num_records=83, connection=connection)

    def test_insert_eng_cutscene_data_to_real_db(self):
        connection = connect_to_db()
        assert connection is not None
        assert isinstance(connection, psycopg2.extensions.connection)

        project_root = Path(__file__).resolve().parent.parent
        base_dir = project_root / 'rsrc' / 'csv'
        csv_eng_file = str(base_dir / 'eng' / 'cut_scene' / '022' / 'VoiceMan_02200.csv')

        verify_and_compare_records(csv_eng_file, expected_num_records=262, connection=connection)

    def test_insert_jp_quest_data_to_real_db(self):
        connection = connect_to_db()
        assert connection is not None
        assert isinstance(connection, psycopg2.extensions.connection)

        project_root = Path(__file__).resolve().parent.parent
        base_dir = project_root / 'rsrc' / 'csv'
        csv_eng_file = str(base_dir / 'jp' / 'quest' / '000' / 'ClsArc000_00021.csv')

        verify_and_compare_records(csv_eng_file, expected_num_records=83, connection=connection)

        # assert that has jap text.
        connection = connect_to_db()
        assert_table_data_has_japanese_text("ClsArc000_00021", connection)

    def test_insert_jp_cutscene_data_to_real_db(self):
        connection = connect_to_db()
        assert connection is not None
        assert isinstance(connection, psycopg2.extensions.connection)

        project_root = Path(__file__).resolve().parent.parent
        base_dir = project_root / 'rsrc' / 'csv'
        csv_eng_file = str(base_dir / 'jp' / 'cut_scene' / '022' / 'VoiceMan_02200.csv')

        verify_and_compare_records(csv_eng_file, expected_num_records=262, connection=connection)
        # assert that has jap text.
        connection = connect_to_db()
        assert_table_data_has_japanese_text("VoiceMan_02200", connection)

    def test_insert_GaiUsd501_00043(self):
        connection = connect_to_db()
        assert connection is not None
        assert isinstance(connection, psycopg2.extensions.connection)

        project_root = Path(__file__).resolve().parent.parent
        base_dir = project_root / 'rsrc' / 'csv'
        csv_eng_file = str(base_dir / 'eng' / 'quest' / '000' / 'GaiUsd501_00043.csv')
        verify_and_compare_records(csv_eng_file, expected_num_records=179, connection=connection)

        #do jp process, where err occur with VarChar(225)
        connection = connect_to_db()
        assert connection is not None
        assert isinstance(connection, psycopg2.extensions.connection)
        csv_jp_file = str(base_dir / 'jp' / 'quest' / '000' / 'GaiUsd501_00043.csv')

        table_name = os.path.splitext(os.path.basename(str(csv_jp_file)))[0]
        csvdf = pd.read_csv(csv_jp_file)
        insert_data_from_df_with_japanese(csvdf, table_name, connection)
        cursor = connection.cursor()

    # def test_quest_table_exists(self):
        
    @staticmethod
    def run_all_tests():
        test_loader = unittest.TestLoader()
        test_suite = test_loader.loadTestsFromTestCase(TestDatabase)

        test_runner = unittest.TextTestRunner()
        result = test_runner.run(test_suite)
        return result
