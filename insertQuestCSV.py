import os
import time

import pandas as pd

from csv_structure_provider import Config
from database_provider import connect_to_db


def create_quest_table(conn):
    try:
        cur = conn.cursor()

        cur.execute("DROP TABLE IF EXISTS Quest;")

        create_table_query = """
        CREATE TABLE Quest (
            id SERIAL PRIMARY KEY,
            quest_name_eng VARCHAR(255),
            quest_name_jp VARCHAR(255),
            expansion_number INTEGER,
            table_name VARCHAR(255)
        );
        """

        cur.execute(create_table_query)

        conn.commit()

        print("Table 'Quest' created successfully.")

    except Exception as e:
        print(f"An error occurred: {e}")


def process_quest_files():
    conn = connect_to_db()

    # Use relative paths based on the current script directory
    script_dir = os.path.dirname(__file__)  # Get the directory of the current script
    eng_quest_file = os.path.join(script_dir, 'rsrc', 'csv', 'eng', 'Quest.csv')
    jp_quest_file = os.path.join(script_dir, 'rsrc', 'csv', 'jp', 'Quest.csv')

    # Create the table if it doesn't exist
    create_quest_table(conn)

    # Load the English quest CSV into a pandas DataFrame, skipping the first two rows
    df_eng = pd.read_csv(eng_quest_file, skiprows=2, low_memory=False)

    with conn.cursor() as cur:
        # Insert English quest rows into the database
        for index, row in df_eng.iterrows():
            eng_name = row.iloc[1]  # Use iloc to access columns by index
            table_name = row.iloc[2]  # Use iloc to access columns by index
            expansion_num = row.iloc[3]  # Use iloc to access columns by index

            # Print values for debugging
            print(eng_name, table_name, expansion_num)

            # Insert English quest data into the Quest table
            insert_query = """
            INSERT INTO Quest (quest_name_eng, quest_name_jp, expansion_number, table_name)
            VALUES (%s, %s, %s, %s);
            """

            # Insert a row into the table (None for quest_name_jp as it's not in the CSV)
            cur.execute(insert_query, (eng_name, None, expansion_num, table_name))

        # Commit changes for the English quests
        conn.commit()

    # Load the Japanese quest CSV into a pandas DataFrame, skipping the first two rows
    df_jp = pd.read_csv(jp_quest_file, skiprows=2, low_memory=False)

    with conn.cursor() as cur:
        # Update Japanese quest names based on table_name matching
        for index, row in df_jp.iterrows():
            jp_name = row.iloc[1]  # Use iloc to access columns by index
            table_name = row.iloc[2]  # Use iloc to access columns by index

            # Check if jp_name or table_name is NaN and skip this row if so
            if pd.isna(jp_name) or pd.isna(table_name):
                continue  # Skip this row if either jp_name or table_name is NaN

            # Print values for debugging
            print(jp_name, table_name)

            # Update the corresponding record in the Quest table
            update_query = """
            UPDATE Quest
            SET quest_name_jp = %s
            WHERE table_name = %s;
            """

            # Execute the update query for each row in the Japanese CSV
            cur.execute(update_query, (jp_name, table_name))

        # Commit changes for the Japanese quests
        conn.commit()

    # Close the connection after processing
    conn.close()

if __name__ == "__main__":
    # base_dir = r'C:\Users\sbelknap\PycharmProjects\dialogdbconn\rsrc\csv'
    # Config.initialize_language_base_directories(base_dir)

    start_time = time.time()
    # # #
    process_quest_files()
    # # #
    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f"Elapsed time: {elapsed_time:.2f} seconds")