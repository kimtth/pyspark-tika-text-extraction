import io
import multiprocessing
import os
import time
import pandas as pd
from multiprocessing import Process

from tika import parser, tika

destination_dir = ".\\dummy\\file-data-load"


def binary_gen(file_list_dir):
    list_files = dict()
    file_list = os.listdir(file_list_dir)
    for file_key in file_list:
        file_location = os.path.join(file_list_dir, file_key)
        with open(file_location, mode='rb') as file:
            file_body = io.BytesIO(file.read())

        list_files[file_key] = file_body
    return list_files


def text_extraction(path, binary_mode=True, client_mode=False):
    TIKA_SERVER_JAR = '.\\tika-jar'
    tika.TikaJarPath = TIKA_SERVER_JAR
    if client_mode:
        tika.TikaClientOnly = True
        os.environ['TIKA_SERVER_ENDPOINT'] = 'http://127.0.0.1:9998/'

    if binary_mode:
        parsed_pdf = parser.from_buffer(path)
    else:
        parsed_pdf = parser.from_file(path)

    data = parsed_pdf['content']
    if data:
        data = data.replace('\n', ' ')
        data = data.replace('\t', ' ')
        data = ' '.join(data.split())  # remove 2 more or space
    else:
        data = ''

    return data


def multiprocess_text_extraction_worker(idx, start_time, file_path, binary, return_dict):
    full_text = text_extraction(binary, True, True)
    return_dict[file_path] = full_text
    if idx % 100 == 0:
        print("--- Text Extraction %s files took %s seconds ---" % (str(idx), (time.time() - start_time)))


def multiprocess_text_extraction_runner(file_list_from_archive):
    # https://stackoverflow.com/questions/10415028/how-can-i-recover-the-return-value-of-a-function-passed-to-multiprocessing-proce
    manager = multiprocessing.Manager()
    return_dict = manager.dict()
    running_tasks = []
    start_time = time.time()
    idx = 1
    for file_path, binary in file_list_from_archive.items():
        running_task = Process(target=multiprocess_text_extraction_worker,
                               args=(idx, start_time, file_path, binary, return_dict))
        running_tasks.append(running_task)
        idx += 1
        running_task.start()

    for running_task in running_tasks:
        running_task.join()

    print("last item", idx)
    return return_dict


if __name__ == '__main__':
    start_time = time.time()
    file_list_from_archive = binary_gen(destination_dir)
    print("--- Step 1 %s seconds ---" % (time.time() - start_time))

    # 1. Single Thread
    return_dict = dict()
    idx = 0
    for file_path, binary in file_list_from_archive.items():
        full_text = text_extraction(binary, True, True)
        return_dict[file_path] = full_text
        idx += 1
        if idx == len(file_list_from_archive):
            print("--- Text Extraction %s files took %s seconds ---" % (str(idx), (time.time() - start_time)))
        elif idx % 100 == 0:
            print("--- Text Extraction %s files took %s seconds ---" % (str(idx), (time.time() - start_time)))

    print("--- Step 2 %s seconds ---" % (time.time() - start_time))
    df = pd.DataFrame(data=return_dict, index=['content'])
    df.T.to_excel('.\\output\\single-dict.xlsx')

    # 2. Multi Thread
    start_time = time.time()
    print("--- Step 1 %s seconds ---" % (time.time() - start_time))
    return_dict = multiprocess_text_extraction_runner(file_list_from_archive)
    print("--- Step 2 %s seconds ---" % (time.time() - start_time))
    df = pd.DataFrame(data=return_dict, index=['content'])
    df.T.to_excel('.\\output\\multi-dict.xlsx')

    print("--- Step 3 %s seconds ---" % (time.time() - start_time))
