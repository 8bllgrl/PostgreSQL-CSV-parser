# Example usage
import os

import pandas as pd

from csv_structure_provider import list_quest_files_for_language, Config, list_quest_files_for_eng, \
    list_cutscene_files_for_eng, list_quest_files_for_jp, list_cutscene_files_for_jp
from database_provider import connect_to_db
from sql_provider import create_table_from_df, insert_data_from_df, insert_data_from_df_with_japanese


def process_csv_files():
    conn = connect_to_db()

    #TODO: This is repetitive. Please clean up, perhaps by making the list_(language) files to return a list of files, not a directory.

    files = list_quest_files_for_eng(Config.BASE_CSV_DIR)

    for file_path in files:
        if os.path.isfile(file_path) and file_path.endswith(".csv"):
            print(f"Processing file: {file_path}")
            table_name = os.path.splitext(os.path.basename(file_path))[0]
            if table_name == "Quest":
                print(f"Skipping file: {file_path}")
                continue
            df = pd.read_csv(file_path)
            create_table_from_df(df, table_name, conn)
            insert_data_from_df(df, table_name, conn)
            print(f"Processed {file_path} into table {table_name}.")

    files = list_cutscene_files_for_eng(Config.BASE_CSV_DIR)

    for file_path in files:
        if os.path.isfile(file_path) and file_path.endswith(".csv"):
            print(f"Processing file: {file_path}")
            table_name = os.path.splitext(os.path.basename(file_path))[0]
            if table_name == "Quest":
                print(f"Skipping file: {file_path}")
                continue
            df = pd.read_csv(file_path)
            create_table_from_df(df, table_name, conn)
            insert_data_from_df(df, table_name, conn)
            print(f"Processed {file_path} into table {table_name}.")

    #TODO: This is repetitive. Please clean up, perhaps by making the list_(language) files to return a list of files, not a directory.

    # Jp should not make a new table.
    print("~~~Starting JP files~~~")

    files = list_quest_files_for_jp(Config.BASE_CSV_DIR)

    for file_path in files:
        if os.path.isfile(file_path) and file_path.endswith(".csv"):
            print(f"Processing file: {file_path}")
            table_name = os.path.splitext(os.path.basename(file_path))[0]
            if table_name == "Quest":
                print(f"Skipping file: {file_path}")
                continue
            df = pd.read_csv(file_path)
            insert_data_from_df_with_japanese(df, table_name, conn)
            print(f"Processed {file_path} into table {table_name}.")

    files = list_cutscene_files_for_jp(Config.BASE_CSV_DIR)

    for file_path in files:
        if os.path.isfile(file_path) and file_path.endswith(".csv"):
            print(f"Processing file: {file_path}")
            table_name = os.path.splitext(os.path.basename(file_path))[0]
            if table_name == "Quest":
                print(f"Skipping file: {file_path}")
                continue
            df = pd.read_csv(file_path)
            insert_data_from_df_with_japanese(df, table_name, conn)
            print(f"Processed {file_path} into table {table_name}.")

    # do the quest table now:


    conn.close()

import time

if __name__ == "__main__":
    base_dir = r'C:\Users\sbelknap\PycharmProjects\dialogdbconn\rsrc\csv'
    Config.initialize_language_base_directories(base_dir)

    start_time = time.time()

    # Main method.
    process_csv_files()
    # # #

    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f"Elapsed time: {elapsed_time:.2f} seconds")
