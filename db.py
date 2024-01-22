import os
import time

import psycopg2
import random
import string

from config import TABLE_1_LEN, TABLE_2_LEN


def time_decorator(func):
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        print(f"Функция {func.__name__} выполнена за {end_time - start_time} секунд.")
        return result
    return wrapper


class PostgreSQL:
    def __init__(self):
        self.names_cache = None
        db_host = os.environ['db_host']
        db_port = os.environ['db_port']
        db_name = os.environ['db_name']
        db_user = os.environ['db_user']
        db_pass = os.environ['db_pass']
        self.connection = psycopg2.connect(dbname=db_name,
                                           user=db_user,
                                           password=db_pass,
                                           host=db_host,
                                           port=db_port)
        self.cursor = self.connection.cursor()

    def generate_tables(self):
        self.__create_tables()
        self.generate_values_for_table_1(TABLE_1_LEN)
        self.generate_values_for_table_2(TABLE_2_LEN)

    def generate_values_for_table_1(self, lenght: int):
        self.names_cache = []
        for name in self.__generate_names(lenght):
            status = random.randint(0, 1)
            self.__insert_values(name, status)
            self.names_cache.append(name)

    def generate_values_for_table_2(self, lenght: int):
        extensions = ['.mp4', '.avi', '.mov', '.mkv', ".png", ".jpg", ".gif", ".bmp", ".tif", ".tiff"]
        current_table_lenght = 0
        for name in self.names_cache:
            if current_table_lenght == lenght:
                break
            ext = random.choice(extensions)
            self.__insert_values(name, ext)
            current_table_lenght += 1

    @time_decorator
    def update_statuses(self):
        try:
            update_query = """
            UPDATE full_names
            SET status = sn.status
            FROM short_names sn
            WHERE sn.name = split_part(full_names.name, '.', 1);
            """
            print(f"Executing update query: {update_query}")
            self.cursor.execute(update_query)
            self.connection.commit()
            print("Статусы обновлены.")
        except Exception as e:
            print(f"Произошла ошибка: {e}")

    def __create_tables(self):
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS short_names (
                id SERIAL PRIMARY KEY,
                name TEXT NOT NULL,
                status INTEGER NOT NULL CHECK (status IN (0, 1))
            );
        """)
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS full_names (
                id SERIAL PRIMARY KEY,
                name TEXT NOT NULL,
                status INTEGER
            );
        """)
        self.connection.commit()

    def __insert_values(self, name, param):
        if type(param) == str:
            self.cursor.execute("INSERT INTO full_names (name) VALUES (%s)", (f"{name}{param}",))
            self.connection.commit()
        else:
            self.cursor.execute(
                "INSERT INTO short_names (name, status) VALUES (%s, %s)",
                (name, param)
            )

    @staticmethod
    def __generate_names(lenght: int):
        for _ in range(lenght):
            name = ''.join(random.choices(string.ascii_lowercase + string.digits, k=10))
            yield name

    def backup(self):
        import os
        backup_query = "COPY full_names TO {} DELIMITER ',' CSV HEADER;".format(os.path.join(os.getcwd(), "full_names.csv"))
        try:
            self.cursor.execute(backup_query)
            self.connection.commit()
            print("Бекап выполнен.")
        except Exception as e:
            print(f"Произошла ошибка: {e}")

    def close(self):
        self.cursor.close()
        self.connection.close()
