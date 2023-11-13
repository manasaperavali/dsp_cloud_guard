import pymysql.cursors
import hashlib
import csv
import random

def hash_string(input_string):
    sha256 = hashlib.sha256()
    sha256.update(input_string.encode('utf-8'))
    return sha256.hexdigest()

def database_exists(connection, database_name):
    query = "SHOW DATABASES LIKE %s"
    with connection.cursor() as cursor:
        cursor.execute(query, (database_name,))
        result = cursor.fetchone()
        return result is not None

def create_database(connection, database_name):
    query = f"CREATE DATABASE {database_name}"
    with connection.cursor() as cursor:
        cursor.execute(query)
    print(f"Database created: {database_name}")

def create_table(connection, table_query):
    with connection.cursor() as cursor:
        cursor.execute(table_query)

def insert_data(connection, insert_query, data):
    try:
        with connection.cursor() as cursor:
            cursor.execute(insert_query, data)
        connection.commit()
    except Exception as e:
        print(f"Error during insertion: {e}")

def main():
    db_connection = pymysql.connect(host='localhost',
                                    user='manager',
                                    password='dummy',
                                    charset='utf8mb4',
                                    cursorclass=pymysql.cursors.DictCursor)

    db_name = 'medrecord'
    try:

        if not database_exists(db_connection, db_name):
            create_database(db_connection, db_name)
            db_connection.commit()

            with pymysql.connect(host='localhost',
                                user='manager',
                                password='dummy',
                                db=db_name,
                                charset='utf8mb4',
                                cursorclass=pymysql.cursors.DictCursor) as table_connection:

                # Create Customer Table
                create_table_query = """
                CREATE TABLE IF NOT EXISTS healthcare (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    first_name VARCHAR(255) NOT NULL,
                    last_name VARCHAR(255) NOT NULL,
                    gender VARCHAR(30) NOT NULL,
                    age INT NOT NULL,
                    weight DOUBLE NOT NULL,
                    height DOUBLE NOT NULL,
                    health_history VARCHAR(255) NOT NULL
                )
                """
                create_table(table_connection, create_table_query)

                create_user_table_query = """
                CREATE TABLE IF NOT EXISTS Userinfo (
                    username VARCHAR(255) NOT NULL PRIMARY KEY,
                    password VARCHAR(255) NOT NULL,
                    groupname VARCHAR(255) NOT NULL
                )
                """
                create_table(table_connection, create_user_table_query)

                create_hash_table_query = """
                CREATE TABLE IF NOT EXISTS healthcare_hash (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    first_name VARCHAR(255) NOT NULL,
                    last_name VARCHAR(255) NOT NULL,
                    gender VARCHAR(255) NOT NULL,
                    age VARCHAR(255) NOT NULL,
                    weight VARCHAR(255) NOT NULL,
                    height VARCHAR(255) NOT NULL,
                    health_history VARCHAR(255) NOT NULL
                )
                """
                create_table(table_connection, create_hash_table_query)

                insert_query = """
                INSERT INTO healthcare (first_name, last_name, gender, age, weight, height, health_history) 
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """
                insert_hash_query = """
                INSERT INTO healthcare_hash (first_name, last_name, gender, age, weight, height, health_history) 
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """

                random.seed(42)  # Setting a seed for reproducibility
                csv_file = "healthcare_data.csv"

                with open(csv_file, 'r') as file:
                    csv_reader = csv.reader(file)
                    headers = next(csv_reader)  # Skip header row

                    for row in csv_reader:
                        #faker se karna he
                        first_name, last_name, gender, age, weight, height, health_history = map(str.strip, row)
                        data = (first_name, last_name, gender, int(age), float(weight), float(height), health_history)

                        hash_first_name = hash_string(first_name)
                        hash_last_name = hash_string(last_name)
                        hash_gender = hash_string(gender)
                        hash_age = hash_string(age)
                        hash_weight = hash_string(weight)
                        hash_height = hash_string(height)
                        hash_health_history = hash_string(health_history)

                        insert_data(table_connection, insert_query, data)
                        insert_data(table_connection, insert_hash_query,
                                    (hash_first_name, hash_last_name, hash_gender, hash_age, hash_weight,
                                    hash_height, hash_health_history))

    except Exception as e:
        print(e)
    finally:
        # Close the connection
        if db_connection:
            db_connection.close()
            print("Connection closed.")

if __name__ == "__main__":
    main()

