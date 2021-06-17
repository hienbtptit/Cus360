import leveldb
import plyvel
import os.path, time
import datetime as dt
import glob
def insert_link(url=''):
    
    # all_subdirs = [d for d in os.listdir('/tmp/leveldb') if os.path.isdir(d)]

    # create id for new folder db =  numbers of folders + 1
    today = dt.date.today()
    pathDB = '/tmp/leveldb/' + today.strftime("%d-%m-%Y")
    # open file to insert
    db = plyvel.DB(pathDB, create_if_missing=True)
    key = url.encode()
    value = 'exist'
    y = db.put(key, value.encode())
    db.close()

def check_exist(url=''):
    # exist = 0  --> url is not exist
    # exist = 1  --> url is already existed
    exist = 0
    latest_folders = get_latest_folder(path='/tmp/leveldb/*')

    for folder in latest_folders:
        # get path   of folder db
        pathDB = str(folder)
        #if older --> 
        db = plyvel.DB(pathDB, create_if_missing=True)
        key = url.encode()
        # check if key existed or not
        check_result = db.get(key)
        db.close()
        if (check_result == None):
            # if not exist
            # db.close()
            continue
        else:
            # if existed
            exist = 1

    return exist


def get_folder_creation_time(path=None):
    # print("Created: %s" % time.ctime(os.path.getctime(path)))
    file_time = dt.date.fromtimestamp(os.path.getctime(path))
    return file_time

def get_latest_folder(path=None):
    result = []
    #get all folders
    all_subdirs = [d for d in glob.glob(path) if os.path.isdir(d)]
    for x in all_subdirs:
        print("X: ",x)
    # get date of latest folder
    latest_subdir = max(all_subdirs, key=os.path.getctime)
    # latest_subdir = max(all_subdirs, key=os.path.getctime(path))
    latest_date = get_folder_creation_time(latest_subdir)
    # get array of folders that have same date as latest date
    for subdir in all_subdirs:
        subdir_date = get_folder_creation_time(subdir)
        if subdir_date == latest_date:
            result.append(subdir)
    return result
    # return latest_subdir

if __name__ == "__main__":
    url = 'https://bds123.vn/ban-gap-03-can-ho-2pn-va-3pn-tai-sunshine-city-ciputra-dt-tu-86-112m2-full-do-gia-30-trieu-m2-pr333243.html'
    check = check_exist(url)
    if (check == 1):
        print("This Url is already existed")
        print("DO NOT INSERT")
        # insert_link(url)
    else:
        print(" Not exist, Ready to insert")
        insert_link(url)
        print("Insert successfully")

    print(get_folder_creation_time(path='/tmp/leveldb'))
    array_folder = get_latest_folder(path='/tmp/leveldb/*')
    for x in array_folder:
        print(x)
    # lastest_folder('/home/seluser/leveldb/')
    # print("Latest Folder: ",folder_array)