
import lxml
import requests
import csv
import os
from datetime import datetime
from bs4 import BeautifulSoup

baseUrl = "https://meeyland.com"
url = "https://meeyland.com/mua-ban-nha-dat/ho-chi-minh"
#https://meeyland.com/mua-ban-nha-dat/ho-chi-minh/page-31785
now = datetime.now()
current_time = now.strftime("%H:%M:%S")
print("start Time =", current_time)
file_path = os.getcwd()+"\\"+"meeyland.csv"
for i  in range  (1,10):
    print("link:")
    print(url+'/page-'+str(i))
    r = requests.get(url+'/page-'+str(i))
    soup = BeautifulSoup(r.content, 'html5lib')
    list_link = soup.find_all('div', class_='col-sm-6 col-md-4')
    for link in list_link:
        href = link.findChild('a')['href']
        request = requests.get(baseUrl + href)
        print(baseUrl + href)
        soup = BeautifulSoup(request.content, 'html5lib')
        # print(soup.prettify())
        prid = soup.find('div', id='nav-tabContent')['data-id']
        print(id)
        title = soup.find('h1', id='nav-home').text
        # print(title)
        des = soup.find('div', class_='des-detail').text
        # print(des)
        phone = soup.find('div', class_='phone')['data-phone']
        # print(phone)
        time = soup.find('div', text='Ngày đăng')
        # print(time)
        with open(file_path, mode='a', encoding="utf8") as csv_file:
            csvfile_writer = csv.writer(csv_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            csvfile_writer.writerow([prid, title, des, phone, time])
            csv_file.close()




