from multiprocessing import Process

import lxml
import requests
import csv
import os
import multiprocessing
import pandas

from datetime import datetime
from bs4 import BeautifulSoup
def crawlDataFirstTime(start, end):
    l=[]
    print("run from "+str(start)+ " to "+ str(end))
    baseUrl = "https://bds123.vn"
    url = "https://bds123.vn/nha-dat-ban-ho-chi-minh.html"
    now = datetime.now()
    current_time = now.strftime("%H:%M:%S")
    print("Start"+current_time);
    file_path = os.getcwd()+"\\"+"bds123.csv"
    for i in range(start, end):
        try:
            r = requests.get(url+'?page='+str(i))
            soup = BeautifulSoup(r.content, 'html5lib')
        except:
            print("Error: ")
            print(url + '?page=' + str(i))
            continue
        list_link = soup.find_all('a', class_='link')
        for  link in  list_link:
                d = {}
                try:
                    href = link['href']
                    request = requests.get(baseUrl+href)
                except:
                    print("Error: ")
                    print(baseUrl + href)
                    continue
                soup = BeautifulSoup(request.content, 'html5lib')
                d['prid'] = soup.find('section', class_='section-post-detail')['data-id']
                d['title'] = soup.find('h1', class_='post-h1').text
                d['des'] = soup.find('div', class_='post-description').text
                d['phone'] = soup.find('button', class_='author-phone').text
                time = soup.find('div', class_='item published')
                d['time'] = time.findChild('span')['title']
                l.append(d)
    df= pandas.DataFrame(l)
    df.to_csv("bds123.csv", mode="a", header=False, index=False)

if __name__ == '__main__':

    ### Multiprocessing with Process
    processes=[Process(target=crawlDataFirstTime,args=(i,i+2)) for i in range(1, 2633, 187)]
    # Run processes
    for p in processes:p.start()

    # Exit the completed processes
    for p in processes:p.join()

