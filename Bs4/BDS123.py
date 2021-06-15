from multiprocessing import Process
from threading import Thread
import requests
import csv
import os
import multiprocessing
import pandas
import re
import time
import test_leveldb


from datetime import datetime
from bs4 import BeautifulSoup
def getFinalPage(url):
    r = requests.get(url)
    soup = BeautifulSoup(r.content, 'html5lib')
    final_page = re.split("=", soup.find_all('a', class_='page-link')[-1]['href'])[-1]
    return int(final_page)

def crawlDataFirstTime(start, end):
    baseUrl = "https://bds123.vn"
    url = "https://bds123.vn/nha-dat-ban-ho-chi-minh.html"
    if(end >= getFinalPage(url)): end = getFinalPage(url) + 1
    print("run from " + str(start) + " to " + str(end))
    now = datetime.now()
    current_time = now.strftime("%H:%M:%S")
    print("Start"+current_time);
    file_path = os.getcwd()+"\\"+"bds1231.csv"

    for i in range(start, end):
        l = []
        try:
            r = requests.get(url+'?page='+str(i))
            soup = BeautifulSoup(r.content, 'html5lib')
        except:
            print("Page Url Error: ")
            print(url + '?page=' + str(i))
            continue
        list_link = soup.find_all('a', class_='link')
        for  link in  list_link:
                d = {}
                try:
                    href = link['href']
                  #  print(baseUrl + href)
                    request = requests.get(baseUrl + href)
                except:
                    print("Post Url Error: ")
                    print(baseUrl + href)
                    continue
                if test_leveldb.check_exist(baseUrl+href) == 1: continue
                else:
                    test_leveldb.insert_link(baseUrl+href)
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
                l.append(d)
        df= pandas.DataFrame(l)
        df.to_csv(file_path, mode="a", header=False, index=False, na_rep="NaN",quoting=csv.QUOTE_ALL)

if __name__ == '__main__':
    final = getFinalPage('https://bds123.vn/nha-dat-ban-ho-chi-minh.html')
    numProcess = 13 # run 13 process
    #print("Final page: "+str(final)+" / " + str(final/numProcess))
    ### Multiprocessing with Process
    #processes=[Process(target=crawlDataFirstTime,args=(i,i+int(final/numProcess))) for i in range(1, final, int(final/numProcess))] #init numProcess process

    #multithreading
    threads=[Thread(target=crawlDataFirstTime,args=(i,i+int(final/numProcess))) for i in range(1, final, int(final/numProcess))]
    # Run processes
    #for p in processes:p.start()
    for thread in threads: thread.start()
    # Exit the completed processes
    #for p in processes:p.join()
    for thread in threads: thread.join()

