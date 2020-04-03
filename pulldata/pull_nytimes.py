"""Pull data from New York Times github and import into db."""

import os
import shutil
from git import Repo, GitCommandError
import mysql.connector
from mysql.connector import Error

def pull_nytimes():
    """Pull data from New York Times github and import into db."""
    repo_dir = '/tmp/covid-19-data'

    # Clone repo
    try:
        Repo.clone_from("https://github.com/nytimes/covid-19-data.git", repo_dir)
    except GitCommandError as e:
        print("Repo exists already.")

    db, cursor = connect_to_db()

    # Try to update record
    sql_update_query = """UPDATE us_states SET cases = 5 WHERE id = 1"""
    cursor.execute(sql_update_query)
    record = cursor.fetchone()

    if record is None:
        print("No matching.")
    else:
        db.commit()
        print("Updated record.")

    # Delete tmp files
    try:
        shutil.rmtree(repo_dir)
    except OSError as e:
        print("Error: %s : %s" % (repo_dir, e.strerror))


def connect_to_db():
    try:
        db = mysql.connector.connect(host="db",
                                     database="COVID",
                                     user=os.environ.get('MYSQL_USER'),
                                     password=os.environ.get('MYSQL_PASSWORD'))

        if db.is_connected():
            db_info = db.get_server_info()
            print("Connected to MariaDB version", db_info)
            cursor = db.cursor()
            cursor.execute("select database();")
            record = cursor.fetchone()
            print("Connected to database:", record)

            return (db, cursor)

    except Error as e:
        print("Error while connecting to MariaDB", e)


if __name__ == "__main__":
    pull_nytimes()
