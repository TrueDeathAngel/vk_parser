import re
from multiprocessing.connection import Client
from multiprocessing import Barrier, Event
from threading import Thread
from json_manager import *
import sqlite3

is_able_to_work = Event()
barrier = Barrier(4, action=is_able_to_work.clear)


def table_handler(file_name, table_name):
    while True:
        is_able_to_work.wait()

        data_dict = load_dict_from_file(file_name)
        key = list(data_dict.keys())[-1]
        data = data_dict[key]
        update_table(table_name, key, data)

        barrier.wait()


def update_table(table_name, news_id, data):
    connection = sqlite3.connect('news.db')

    cursor = connection.cursor()
    number_of_repeats = 0
    try:
        cursor.execute(f"SELECT COUNT(news_id) FROM {table_name} WHERE news_id = '{news_id}'")
        number_of_repeats = list(cursor.fetchone())[0]
    except sqlite3.OperationalError:
        cursor.execute(f'''CREATE TABLE {table_name}
                (
                news_id VARCHAR(50),
                [data] TEXT
                )''')

    try:
        data = re.sub(r"[\[\]']", '', str(data))
        if number_of_repeats:
            cursor.execute(f"UPDATE {table_name} SET [data] = '{data}' WHERE news_id = '{news_id}'")
        else:
            cursor.execute(f"INSERT INTO {table_name} (news_id, [data]) VALUES ('{news_id}', '{data}')")
    except sqlite3.DatabaseError as err:
        print("Error: ", err)
    else:
        connection.commit()

    cursor.close()

    connection.close()


ip = ('localhost', 6000)

if __name__ == '__main__':
    Thread(target=table_handler, args=(text_file_name, 'news_text')).start()
    Thread(target=table_handler, args=(photo_file_name, 'news_photos')).start()
    Thread(target=table_handler, args=(link_file_name, 'news_links')).start()

    while True:
        try:
            with Client(ip) as conn:
                print('from 1: ' + conn.recv())
                is_able_to_work.set()
                barrier.wait()
                print('wrote to database')
                conn.send('wrote to database!')
                print('sent msg to 1')
        except ConnectionRefusedError:
            print("waiting for process 1")
        except ConnectionResetError:
            print('connection reset')
        except KeyboardInterrupt:
            print('interrupted from keyboard!')
            break
