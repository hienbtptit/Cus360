import datetime
from selenium import webdriver
from multiprocessing import Process
import re
import pandas
import os
import csv
import multiprocessing
from bs4 import BeautifulSoup
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from pathlib import Path
import sys
sys.path.append('../')
from CustomLibs import single_thread_leveldb
from CustomLibs import test_leveldb
baseURL='https://nha.chotot.com/'
Url = 'https://nha.chotot.com/tp-ho-chi-minh/mua-ban-bat-dong-san'
DRIVER_PATH ='/usr/bin/chromedriver'

def writeFieldNameToFile(file):
    field_name = []
    field_name.append({'prid': 'prid', 'title': 'title', 'des': 'des', 'phone': 'phone'})
    df = pandas.DataFrame(field_name)
    df.to_csv(file, mode="a", header=False, index=False, na_rep="NaN",quoting=csv.QUOTE_ALL)
def crawlDataFirstTime(start, end):
    Path("/tmp/leveldb/chotot").mkdir(parents=True, exist_ok=True)
    now = datetime.datetime.now()
    current_time = now.strftime("%H:%M:%S")
    file_path = os.getcwd()+"/chotot/"+"chotot.csv"
    options = webdriver.ChromeOptions()
    options.add_argument('--ignore-certificate-errors')
    options.add_argument('--incognito')
    options.add_argument('--headless')
    driver = webdriver.Chrome(executable_path=DRIVER_PATH, options=options)
    for page in range(start, end):
        try:
            driver.get(Url + '?page=' + str(page))
        except:
            print("Page eror ", Url + '?page=' + str(page))
            continue
        driver.maximize_window()
        wait = WebDriverWait(driver, 60)
        try:
            wait.until(EC.visibility_of_all_elements_located((By.XPATH, "//a[@class='adItem___2GCVQ']")))
            list_link = driver.find_elements_by_xpath("//a[@class='adItem___2GCVQ']")
        except:
            print("Element not visible in page: ", Url + '?page=' + str(page))
            continue
        links = []
        list_data = []
        for link in list_link:
            try:
                l = {}
                l['id'] = link.id
                l['href'] = link.get_attribute('href')
                links.append(l)
            except:
                print('link error: ', link)
                continue
        for link in links:
            try:
                driver.get(link['href'])
            except:
                print("Post link error")
            test_leveldb.insert_link(link['href'], start, '/tmp/leveldb/chotot/')
            soup = BeautifulSoup(driver.page_source, 'html5lib')
            try:
                d={}
                d['prid']=link['id']
                d['title'] = soup.find('h1',class_='adTilte___3UqYW').text
                d['des']=soup.find('p',class_='adBody___ev-xe').text
                d['phone']=re.split(":",soup.find('a', id='call_phone_btn')['href'])[-1]
            except:
                print("Get atribute eror")
                print("Post get attribute error:",link['href'])
                continue
            list_data.append(d)

        df = pandas.DataFrame(list_data)
        df.to_csv(file_path, mode="a", header=False, index=False, na_rep="NaN", quoting=csv.QUOTE_ALL)
    driver.quit()
    now = datetime.datetime.now()
    current_time = now.strftime("%H:%M:%S")
    print("end Time =", current_time)

def crawlBySchedule():
    Path("/tmp/leveldb/chotot").mkdir(parents=True, exist_ok=True)
    stop = 0  # stop while loop when 4 url is duplicate
    page = 1
    now = re.split("\s", str(datetime.datetime.now()))[0]
    now = re.split("-", now)
    file_path = os.getcwd() + "/chotot/" + "chotot-" + now[0] + now[1] + now[2] + ".csv"
    writeFieldNameToFile(file_path)
    options = webdriver.ChromeOptions()
    options.add_argument('--ignore-certificate-errors')
    options.add_argument('--incognito')
    options.add_argument('--headless')
    driver = webdriver.Chrome(executable_path=DRIVER_PATH, options=options)
    iterator = 0
    while stop == 0:
        try:
            driver.get(Url + '?page=' + str(page))
        except:
            print("Page eror ", Url + '?page=' + str(page))
            continue
        driver.maximize_window()
        wait = WebDriverWait(driver, 60)
        try:
            wait.until(EC.visibility_of_all_elements_located((By.XPATH, "//a[@class='adItem___2GCVQ']")))
            list_link = driver.find_elements_by_xpath("//a[@class='adItem___2GCVQ']")
        except:
            print("Element not visible with page link: ", Url + '?page=' + str(page))
        links = []
        list_data = []
        for link in list_link:
            try:
                l = {}
                l['id'] = link.id
                l['href'] = link.get_attribute('href')
                links.append(l)
            except:
                print('link error: ', link)
                continue
        for link in links:
            try:
                driver.get(link['href'])
            except:
                print("Post link error")
            if single_thread_leveldb.check_exist(link['href'], '/tmp/leveldb/chotot/') == 1:
                if iterator == 3:
                    stop = 1
                    break
                iterator = iterator + 1
            else:
                iterator = 0
                single_thread_leveldb.insert_link(link['href'], '/tmp/leveldb/chotot/')
            soup = BeautifulSoup(driver.page_source, 'html5lib')
            try:
                d = {}
                d['prid'] = link['id']
                d['title'] = soup.find('h1', class_='adTilte___3UqYW').text
                d['des'] = soup.find('p', class_='adBody___ev-xe').text
                d['phone'] = re.split(":", soup.find('a', id='call_phone_btn')['href'])[-1]
            except:
                print("Get atribute eror")
                print("Post get attribute error:", link['href'])
                continue
            list_data.append(d)
        page = page + 1
        df = pandas.DataFrame(list_data)
        df.to_csv(file_path, mode="a", header=False, index=False, na_rep="NaN", quoting=csv.QUOTE_ALL)

keys = sys.argv[1::2]
values = sys.argv[2::2]
args = {k: v for k, v in zip(keys, values)}

first_time = args.get('--first-time')
# create folder to store CSV file
Path(os.getcwd() + "/chotot").mkdir(parents=True, exist_ok=True)
if first_time == '1':
    writeFieldNameToFile(os.getcwd() + "/chotot/" + "chotot.csv")
    print("crawlDataFirstTime")
    final = 3889
    numProcess = multiprocessing.cpu_count()  # run process
    ### Multiprocessing with Process
    processes = [Process(target=crawlDataFirstTime, args=(i, i + int(final / numProcess))) for i in
                 range(1, final, int(final / numProcess))]  # init numProcess process
    # Run processes
    for p in processes: p.start()
    # Exit the completed processes
    for p in processes: p.join()
else:
    print("crawlBySchedule")
    crawlBySchedule()

