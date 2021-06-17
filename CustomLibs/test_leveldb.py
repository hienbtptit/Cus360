# from typing import BinaryIO
import levecmdldb
import plyvel


def insert_link(url=''):
    db = plyvel.DB('/tmp/testdb/', create_if_missing=True)
    key = url.encode()
    value = 'exist'
    y = db.put(key,value.encode())

def insert_link(url='', thread_id=None):
    pathDB = '/tmp/leveldb/' + str(thread_id)
    db = plyvel.DB(pathDB, create_if_missing=True)
    key = url.encode()
    value = 'exist'
    y = db.put(key, value.encode())
    db.close()

def check_exist(url=''):
    # exist = 0  --> url is not exist
    # exist = 1  --> url is already existed 
    exist = 0
    db = plyvel.DB('/tmp/testdb/', create_if_missing=True)
    key = url.encode()
    # check if key existed or not
    check_result = db.get(key)
    if (check_result == None):
        # if not exist
        return 0
    else:
        # if existed
        return 1

if __name__ == "__main__":
    url = 'facebook.com'
    check = check_exist(url)
    if (check == 1):
        print("This Url is already existed")
        print("DO NOT INSERT")
        # insert_link(url)
    else:
        print(" Not exist, Ready to insert")
        insert_link(url)
        print("Insert successfully")

