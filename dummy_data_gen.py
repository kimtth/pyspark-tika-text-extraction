import shutil
import time
import os
import uuid

target_dir = ".\\dummy\\file-data"
destination_dir = ".\\dummy\\file-data-load"


def gen_test_file():
    list_file = os.listdir(target_dir)

    for file in list_file:
        file_path = os.path.join(target_dir, file)
        base, extension = os.path.splitext(file_path)
        file_name = os.path.basename(file_path)
        file_name = file_name.split('.')[0]

        for i in range(0, 50):
            id = uuid.uuid1()
            file_path_new_name = os.path.join(destination_dir, file_name + str(id) + extension)
            shutil.copy(file_path, file_path_new_name)


if __name__ == '__main__':
    start_time = time.time()
    gen_test_file()
    print("--- Done. %s seconds ---" % (time.time() - start_time))