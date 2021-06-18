from multiprocessing import Process

import multiprocessing
import requests
import csv
import os
import pandas
import re
import datetime
import sys
sys.path.append('../')
from CustomLibs import single_thread_leveldb
from CustomLibs import test_leveldb

baseUrl = "https://bds123.vn"
url = "https://bds123.vn/nha-dat-ban-ho-chi-minh.html"

from bs4 import BeautifulSoup
def getFinalPage(urlPara):
    r = requests.get(urlPara)
    soup = BeautifulSoup(r.content, 'html5lib')
    final_page = re.split("=", soup.find_all('a', class_='page-link')[-1]['href'])[-1]
    return int(final_page)

def crawlDataFirstTime(start, end):
    if(end >= getFinalPage(url)): end = getFinalPage(url) + 1
    print("run from " + str(start) + " to " + str(end))
    file_path = os.getcwd()+"\\"+"bds123.csv"
    for i in range(start, end):
        l = []
        try:
            r = requests.get(url+'?created=desc&page='+str(i))
            soup = BeautifulSoup(r.content, 'html5lib')
        except:
            print("Page Url Error: ")
            print(url + '?page=' + str(i))
            continue
        list_link = soup.find_all('a', class_='link')
        for  link in  list_link:
                d = {}
                href = link['href']
                test_leveldb.insert_link(baseUrl + href, start)
                try:
                    request = requests.get(baseUrl + href)
                except:
                    print("Post Url Error: ")
                    print(baseUrl + href)
                    continue
                try:
                    soup = BeautifulSoup(request.content, 'html5lib')
                    d['prid'] = soup.find('section', class_='section-post-detail')['data-id']
                    d['title'] = soup.find('h1', class_='post-h1').text
                    d['des'] = soup.find('div', class_='post-description').text
                    d['phone'] = soup.find('button', class_='author-phone').text
                    time = soup.find('div', class_='item published')
                    d['time'] = time.findChild('span')['title']
                except:
                    print("Get atribute value error")
                    continue
                #format time
                dayPost = re.split("\s", d['time'])[-1]
                dayPost = re.split("/", dayPost)
                timePost = re.split("\s", d['time'])[-2]
                timePost = re.split(":", timePost)
                d['time'] = datetime.datetime(int(dayPost[2]), int(dayPost[1]), int(dayPost[0]), int(timePost[0]),int(timePost[1]))
                l.append(d)
        df= pandas.DataFrame(l)
        df.to_csv(file_path, mode="a", header=False, index=False, na_rep="NaN",quoting=csv.QUOTE_ALL)

def crawlBySchedule(): #crawl data after day : day-month-year
    stop = 0 # stop while loop when 4 url is duplicate
    page = 1
    now = re.split("\s",str(datetime.datetime.now()))[0]
    now = re.split("-",now)

    file_path = os.getcwd() + "\\" + "bds123-" + now[0] + now[1] + now[1] + ".csv"
    print(file_path)
    iterator = 0
    while stop == 0:
        l = []
        try:
            r = requests.get(url + '?created=desc&page=' + str(page))
            soup = BeautifulSoup(r.content, 'html5lib')
        except:
            print("Page Url Error: ")
            print(url + '?page=' + str(page))
            continue
        list_link = soup.find_all('a', class_='link')
        for link in list_link:
            href = link['href']
            d = {}
            if single_thread_leveldb.check_exist(baseUrl + href) == 1:
                if iterator == 3 :
                    stop = 1
                    break
                iterator = iterator+1
            else:
                iterator = 0
                single_thread_leveldb.insert_link(baseUrl + href)

            try:
                request = requests.get(baseUrl + href)
            except:
                print("Post Url Error: ")
                print(baseUrl + href)
                continue
            try:
                soup = BeautifulSoup(request.content, 'html5lib')
                d['prid'] = soup.find('section', class_='section-post-detail')['data-id']
                d['title'] = soup.find('h1', class_='post-h1').text
                d['des'] = soup.find('div', class_='post-description').text
                d['phone'] = soup.find('button', class_='author-phone').text
                time = soup.find('div', class_='item published')
                d['time'] = time.findChild('span')['title']
            except:
                print("Get atribute value error")
                continue
            dayPost = re.split("\s",d['time'])[-1]
            dayPost = re.split("/", dayPost)
            timePost = re.split("\s", d['time'])[-2]
            timePost = re.split(":", timePost)
            d['time'] = datetime.datetime(int(dayPost[2]), int(dayPost[1]), int(dayPost[0]), int(timePost[0]), int(timePost[1]))
            #check if timepost < date then break the loop

            l.append(d)
        df = pandas.DataFrame(l)
        df.to_csv(file_path, mode="a", header=False, index=False, na_rep="NaN", quoting=csv.QUOTE_ALL)
        page = page + 1
        if stop == 1:
            break


'''if __name__ == '__main__':
    #crawlBySchedule(2021,6,16)
    final = getFinalPage('https://bds123.vn/nha-dat-ban-ho-chi-minh.html')
    numProcess = multiprocessing.cpu_count()*2 - 4 # run process

    print(numProcess)
    #print("Final page: "+str(final)+" / " + str(final/numProcess))
    ### Multiprocessing with Process
    processes=[Process(target=crawlDataFirstTime,args=(i,i+int(final/numProcess))) for i in range(1, final, int(final/numProcess))] #init numProcess process

    #multithreading
    #threads=[Thread(target=crawlDataFirstTime,args=(i,i+int(final/numProcess))) for i in range(1, final, int(final/numProcess))]

    # Run processes
    for p in processes:p.start()
    # Exit the completed processes
    for p in processes:p.join()'''



keys = sys.argv[1::2]
values = sys.argv[2::2]

print(keys)
print(values)

args = {k: v for k, v in zip(keys, values)}
print(args)

first_time = args.get('--first-time')
if first_time == '1':
    print("crawlDataFirstTime")
    final = getFinalPage('https://bds123.vn/nha-dat-ban-ho-chi-minh.html')
    numProcess = multiprocessing.cpu_count() * 2 - 4  # run process

    print(numProcess)
    # print("Final page: "+str(final)+" / " + str(final/numProcess))
    ### Multiprocessing with Process
    processes = [Process(target=crawlDataFirstTime, args=(i, i + int(final / numProcess))) for i in
                 range(1, final, int(final / numProcess))]  # init numProcess process

    # multithreading
    # threads=[Thread(target=crawlDataFirstTime,args=(i,i+int(final/numProcess))) for i in range(1, final, int(final/numProcess))]

    # Run processes
    for p in processes: p.start()
    # Exit the completed processes
    for p in processes: p.join()
else:
    print("crawlBySchedule")
    crawlBySchedule()


