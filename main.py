import threading
import time
from multiprocessing.connection import Listener
from vk_manager import *
from json_manager import *
from threading import Event, Barrier, Thread


text_event = Event()
photo_event = Event()
link_event = Event()
read_event = Event()

iteration_number = 0

# 0: thread_1 -> 1.json     thread_2 -> 2.json      thread_3 -> 3.json
# 1: thread_4 <- 1.json     thread_2 -> 2.json      thread_3 -> 3.json
# 2: thread_1 -> 1.json     thread_4 <- 2.json      thread_3 -> 3.json
# 3: thread_1 -> 1.json     thread_2 -> 2.json      thread_4 <- 3.json


def new_iteration():
    global connected
    print('work done')
    if connected:
        try:
            conn.send('wrote to json files!')
            print('sent msg to 2')
            print('from 2: ' + conn.recv())
        except ConnectionAbortedError:
            print('connection aborted')
            connected = False
            time.sleep(1)
        except ConnectionResetError:
            print('conn reset')
            connected = False
            time.sleep(1)
    else:
        print('disconnected')

    global iteration_number
    iteration_number = (iteration_number + 1) % 4

    if iteration_number == 0:
        link_event.set()
        read_event.clear()
    elif iteration_number == 1:
        read_event.set()
        text_event.clear()
    elif iteration_number == 2:
        text_event.set()
        photo_event.clear()
    elif iteration_number == 3:
        photo_event.set()
        link_event.clear()


barrier = Barrier(3, action=new_iteration)


def text_handler(data_dict):
    global barrier
    barrier = Barrier(3, action=new_iteration)
    for news_row in data_dict:
        text_event.wait()
        print('text wrote')
        text_dict = load_dict_from_file(text_file_name)
        text_dict[get_news_id(news_row)] = get_text(news_row)
        load_dict_to_file(text_file_name, text_dict)

        barrier.wait()

    barrier = Barrier(threading.active_count() - 3, action=new_iteration)


def photo_handler(data_dict):
    global barrier
    for news_row in data_dict:
        photo_event.wait()
        print('photo wrote')
        photo_dict = load_dict_from_file(photo_file_name)
        photo_dict[get_news_id(news_row)] = get_photos_list(news_row)
        load_dict_to_file(photo_file_name, photo_dict)

        barrier.wait()

    barrier = Barrier(threading.active_count() - 3, action=new_iteration)


def link_handler(data_dict):
    global barrier
    for news_row in data_dict:
        link_event.wait()
        print('link wrote')
        link_dict = load_dict_from_file(link_file_name)
        link_dict[get_news_id(news_row)] = get_links_list(news_row)
        load_dict_to_file(link_file_name, link_dict)

        barrier.wait()

    barrier = Barrier(threading.active_count() - 3, action=new_iteration)


def reader():
    while True:
        read_event.wait()

        print(f'iteration: {iteration_number} | reading file: {text_file_name}')

        barrier.wait()

        print(f'iteration: {iteration_number} | reading file: {photo_file_name}')

        barrier.wait()

        print(f'iteration: {iteration_number} | reading file: {link_file_name}')

        barrier.wait()


connected = False


def find_connection():
    global conn, connected
    while True:
        if not connected:
            try:
                conn = Listener(ip).accept()
                print('connected!')
                connected = True
            except OSError as err:
                print('Error:', err)


ip = ('localhost', 6000)


if __name__ == '__main__':
    global conn

    Thread(daemon=True, target=find_connection).start()
    Thread(daemon=True, target=reader).start()

    while True:

        data = get_news_list()
        text_event.set()
        photo_event.set()
        link_event.set()

        thread_1 = Thread(target=text_handler, args=(data,))
        thread_2 = Thread(target=photo_handler, args=(data,))
        thread_3 = Thread(target=link_handler, args=(data,))

        thread_1.start()
        thread_2.start()
        thread_3.start()

        thread_1.join()
        thread_2.join()
        thread_3.join()

        print("---------------------------------\nEnd!\n---------------------------------")
        barrier = Barrier(3, action=new_iteration)
