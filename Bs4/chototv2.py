import datetime
from selenium import webdriver
from multiprocessing import Process
import time
import re
import pandas
import os
import csv
from bs4 import BeautifulSoup
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import sys
sys.path.append('../')
from CustomLibs import single_thread_leveldb
from CustomLibs import test_leveldb
baseURL='https://nha.chotot.com/'
Url = 'https://nha.chotot.com/tp-ho-chi-minh/mua-ban-bat-dong-san'
DRIVER_PATH = 'D:\\bin\\chromedriver.exe'
#final=6652
def crawlDataFirstTime(start, end):
    count = 0;
    now = datetime.datetime.now()
    current_time = now.strftime("%H:%M:%S")
    print("start Time =", current_time)

    file_path = os.getcwd()+"\\"+"chototv2.csv"
    options = webdriver.ChromeOptions()
    options.add_argument('--ignore-certificate-errors')
    options.add_argument('--incognito')
    options.add_argument('--headless')
    driver = webdriver.Chrome(executable_path=DRIVER_PATH, chrome_options=options)
    for page in range(start, end):
        try:
            driver.get(Url + '?page=' + str(page))
        except:
            print("Page eror ", Url + '?page=' + str(page))
            continue
        driver.maximize_window()
        wait = WebDriverWait(driver, 20)
        wait.until(EC.visibility_of_all_elements_located((By.XPATH, "//a[@class='adItem___2GCVQ']")))
        list_link = driver.find_elements_by_xpath("//a[@class='adItem___2GCVQ']")
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
    stop = 0  # stop while loop when 4 url is duplicate
    page = 1
    now = re.split("\s", str(datetime.datetime.now()))[0]
    now = re.split("-", now)

    file_path = os.getcwd() + "\\" + "chotot-" + now[0] + now[1] + now[2] + ".csv"
    options = webdriver.ChromeOptions()
    options.add_argument('--ignore-certificate-errors')
    options.add_argument('--incognito')
    options.add_argument('--headless')
    driver = webdriver.Chrome(executable_path=DRIVER_PATH, chrome_options=options)
    print(file_path)
    iterator = 0
    while stop == 0:
        try:
            driver.get(Url + '?page=' + str(page))
        except:
            print("Page eror ", Url + '?page=' + str(page))
            continue
        driver.maximize_window()
        wait = WebDriverWait(driver, 20)
        wait.until(EC.visibility_of_all_elements_located((By.XPATH, "//a[@class='adItem___2GCVQ']")))
        list_link = driver.find_elements_by_xpath("//a[@class='adItem___2GCVQ']")
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
if first_time == '1':
    print("crawlDataFirstTime")
    final = 3889
    numProcess = 5
    print(numProcess)
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
