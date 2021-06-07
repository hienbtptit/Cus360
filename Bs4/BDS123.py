
import lxml
import requests
import csv
import os
import multiprocessing
from datetime import datetime
from bs4 import BeautifulSoup
def crawlDataFirstTime(start, end):
    print("run from "+str(start)+ " to "+ str(end))
    baseUrl = "https://bds123.vn/"
    url = "https://bds123.vn/nha-dat-ban-ho-chi-minh.html"
    now = datetime.now()
    current_time = now.strftime("%H:%M:%S")
    print("Start"+current_time);
    file_path = os.getcwd()+"\\"+"bds123.csv"
    for i in range(start, end):
            r = requests.get(url+'?page='+str(i))
            soup = BeautifulSoup(r.content, 'html5lib')
            list_link = soup.find_all('a', class_='link')
            for  link in  list_link:
                href = link['href']
                request = requests.get(baseUrl+href)
                soup = BeautifulSoup(request.content, 'html5lib')
                prid = soup.find('section', class_='section-post-detail')['data-id']
                title = soup.find('h1', class_='post-h1').text
                des = soup.find('div', class_='post-description').text
                phone = soup.find('button', class_='author-phone').text
                time = soup.find('div', class_='item published')
                time_child = time.findChild('span')['title']
                time.findChild
                with open(file_path, mode='a', encoding="utf8") as csv_file:
                    csvfile_writer = csv.writer(csv_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
                    csvfile_writer.writerow([prid, title, des, phone, time_child])
                    csv_file.close()
    now = datetime.now()
    current_time = now.strftime("%H:%M:%S")
    print("End" + current_time);
if __name__ == "__main__":
    # creating processes
    p1 = multiprocessing.Process(target=crawlDataFirstTime, args=(1,188))
    p2 = multiprocessing.Process(target=crawlDataFirstTime, args=(188,376))
    p3 = multiprocessing.Process(target=crawlDataFirstTime, args=(376, 564))
    p4 = multiprocessing.Process(target=crawlDataFirstTime, args=(564, 752))
    p5 = multiprocessing.Process(target=crawlDataFirstTime, args=(752, 940))
    p6 = multiprocessing.Process(target=crawlDataFirstTime, args=(940, 1128))
    p7 = multiprocessing.Process(target=crawlDataFirstTime, args=(1128, 1316))
    p8 = multiprocessing.Process(target=crawlDataFirstTime, args=(1316, 1504))
    p9 = multiprocessing.Process(target=crawlDataFirstTime, args=(1504, 1692))
    p10 = multiprocessing.Process(target=crawlDataFirstTime, args=(1692, 1880))
    p11 = multiprocessing.Process(target=crawlDataFirstTime, args=(1880, 2068))
    p12 = multiprocessing.Process(target=crawlDataFirstTime, args=(2068, 2256))
    p13 = multiprocessing.Process(target=crawlDataFirstTime, args=(2256, 2444))
    p14 = multiprocessing.Process(target=crawlDataFirstTime, args=(2444, 2620))

    # starting process
    p1.start()
    p2.start()
    p3.start()
    p4.start()
    p5.start()
    p6.start()
    p7.start()
    p8.start()
    p9.start()
    p10.start()
    p11.start()
    p12.start()
    p13.start()
    p14.start()

    #wait till process finish
    p1.join()
    p2.join()
    p1.join()
    p2.join()
    p3.join()
    p4.join()
    p5.join()
    p6.join()
    p7.join()
    p8.join()
    p9.join()
    p10.join()
    p11.join()
    p12.join()
    p13.join()
    p14.join()

    # both processes finished
    print("Done!")



