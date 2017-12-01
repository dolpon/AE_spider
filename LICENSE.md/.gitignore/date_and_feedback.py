import csv
import re
import requests
import time as Time
from bs4 import BeautifulSoup as bs
from AE.item import headers,proxies,send_data,start_url
from queue import Queue
import threading
from pymongo import MongoClient

def Get(url,headers):
    while True:
        try:
            res = requests.get(url,headers=headers)
        except:
            print("request error")
            continue
        if res.status_code in range(200,300):
            return res
        else:
            continue

def get_time():
    ntime = str(Time.time())
    ntime = ntime.split(".")
    ntime = ntime[0] + ntime[1][:3]
    return ntime

def try_1(url):
    res = Get(url,headers=headers)
    bsobj = bs(res.text)
    cag = bsobj.find("dl",{"class":"son-category"})
    try:
        search_count = bsobj.find("strong",{"class",re.compile("search-count")}).get_text()
        search_count = int(search_count.replace(",", ""))
    except:
        print(url)
        search_count = None
    mother = bsobj.find("div",{"id":"aliGlobalCrumb"},{"class":"ui-breadcrumb"}).find("h1").find_all("a")
    mother = [i.get_text().replace(" ","") for i in mother]
    follows = cag.find_all("a",{"rel":"follow"})
    now = bsobj.find("span",{"class","current-cate"}).get_text()
    if not follows:
        mother.append(now)
        print(search_count,mother,url)
        #cag_data.append([search_count,mother,url])
        return
    for follow in follows:
        furl = follow.attrs["href"]
        full = "https:{}"
        furl = full.format(furl)
        try_1(furl)

def sku_list(url,cag,pgn):
    for i in range(pgn):
        arg = "/"+ str(i+1)
        okurl = url.format(arg)
        print(okurl)
        res = Get(url=okurl,headers=headers)
        bsobj = bs(res.text)
        sku_tags = bsobj.find("ul",{"class":"util-clearfix son-list"}).find_all("li",{"class":re.compile("list-item.+")})
        url_list = set()
        for tag in sku_tags:
            url = tag.find("a",{"class":re.compile("product.+")})
            try:
                url = url.attrs["href"]
                new_url = "https:{}".format(url)
                print(new_url)
            except:
                print("fuck")
            url_list.add(new_url)
        if len(url_list) != 0:
            url_list = [[i,cag] for i in url_list]
            print("get_url_list",url_list)
            th_pool1(url_list)
        else:
            print(okurl)
            continue

def sku_detail(url,cag):
    res = Get(url=url,headers=headers)
    try:
        id = re.findall("productId=\"[0-9]+\"",res.text)[0].replace("productId=","").replace("\"","")
    except:
        id = None
    try:
        ownerMemberId = re.findall("ownerMemberId=[0-9]+", res.text)[0].replace("ownerMemberId=", "")
    except:
        ownerMemberId = None
    res = bs(res.text)
    try:
        price = res.find("span",{"id":"j-sku-price"},{"class","p-price"}).get_text()
    except:
        price = None
    try:
        votes = res.find("span",{"class":"rantings-num"}).get_text().replace("(","").replace("votes)","")
    except:
        votes = None
    try:
        orders = res.find("span",{"id":"j-order-num"},{"class":"order-num"}).get_text().replace(" orders","")
    except:
        orders = None
    try:
        seller = res.find("dd",{"class":"store-name notranslate"}).get_text().replace("\n", "")
    except:
        seller = None
    try:
        location = res.find("dd",{"class":"store-address"}).get_text().replace("\n", "").replace("\t","")
    except:
        location = None
    try:
        open_time = res.find("div",{"class":"store-open-time"}).find("span").get_text().replace(" year(s)","")
    except:
        open_time = None
    try:
        Itemspecifics = res.find("ul", {"class": "product-property-list util-clearfix"}).find_all("li")
        item_sps = [[i.get_text().replace("\n","")]for i in Itemspecifics]
    except:
        item_sps = None
    sku_writer.writerow([id,ownerMemberId,price,votes,orders,seller,location,open_time,item_sps,cag,url])
    print("writer_ok",id,ownerMemberId,price,votes,orders,seller,location,open_time,item_sps,cag,url)
    db.insert({"id":id, "ownerMemberId":ownerMemberId, "price":price, "votes":votes, "orders":orders, "seller":seller, "location":location, "open_time":open_time, "item_sps":item_sps,
      "cag":cag, "url":url})
    db1.insert({"id":id,"owner_id":ownerMemberId,"voi":votes})
    #if id and ownerMemberId and orders != 0:
    #    get_sold_date(id,url,cag)

def get_sold_date(id,url,cag):
    fb_headers = headers
    fb_headers["Referer"] = url
    fb_headers["Host"] = "feedback.aliexpress.com"
    url = "https://feedback.aliexpress.com/display/evaluationProductDetailAjaxService.htm?productId={}&type=default&page={}&_={}"
    nowtime = get_time()
    ok_url = url.format(id,1,nowtime)
    res = Get(url=ok_url,headers=fb_headers).json()
    try:
        pgn = res["page"]["total"]
    except:
        pgn = None
        print("pgn not fund")
        return
    data = [[id,url,i+1,cag] for i in range(1,int(pgn))]
    records = res["records"]
    keys = ['quantity', 'countryCode', 'buyerAccountPointLeval', 'id', 'unit', 'lotNum', 'name', 'date']
    for record in records:
        data = [record[key] for key in keys]
        data.append(id)
        data.append(cag)
        print(data)
        date_writer.writerow([data])
    if pgn >1:
        th_pool2(data)

def get_sold_date2(id,url,pgn,cag):
    fb_headers = headers
    fb_headers["Referer"] = url
    fb_headers["Host"] = "feedback.aliexpress.com"
    nowtime = get_time()
    ok_url = url.format(id,pgn,nowtime)
    res = Get(url=ok_url,headers=fb_headers).json()
    records = res["records"]
    keys = ['quantity', 'countryCode', 'buyerAccountPointLeval', 'id', 'unit', 'lotNum', 'name', 'date']
    for record in records:
        data = [record[key] for key in keys]
        data.append(id)
        data.append(cag)
        print(data)
        date_writer.writerow([data])

def voi_text(own_id,p_id,voi):
    send_data["ownerMemberId"] = own_id
    send_data["productId"] = p_id
    url = "https://feedback.aliexpress.com/display/productEvaluation.htm#feedback-list"
    pgn = int(voi)/10
    if pgn==0:
        pgn = 1
    for p in range(pgn):
        send_data["page"] = p+1
        send_data["currentPage"] = p
        res = requests.post(url=url,headers=headers,data=send_data)
        bsobj = bs(res.text)
        all_feedback = bsobj.find_all("div",{"class":"feedback-item clearfix"})
        for feedback in all_feedback:
            user_url = feedback.find("span",{"class":"user-name"})
            try:
                user_url = user_url.find("a").attrs["href"]
            except:
                user_url = user_url.get_text()
            print(user_url)
            order_info = feedback.find("div",{"class":"user-order-info"}).find_all("span")
            info = [i.get_text().replace("\n","").replace("\t","").replace(" ","").split(":") for i in order_info]
            info = [{i[0],i[1]}  for i in info ]
            print(info)

def th_pool1(data):
    threads = []
    data_queue = Queue()
    for i in data:
        data_queue.put(i)
    while data_queue.qsize() != 0 or threads:
        for thread in threads:
            if not thread.is_alive():
                threads.remove(thread)
        while data_queue.qsize() != 0 and len(threads) <= 5:
            thread = threading.Thread(target=sku_detail, args=(data_queue.get()))
            thread.setDaemon(True)
            thread.start()
            threads.append(thread)

def th_pool2(data):
    threads = []
    data_queue = Queue()
    for i in data:
        data_queue.put(i)
    while data_queue.qsize() != 0 or threads:
        for thread in threads:
            if not thread.is_alive():
                threads.remove(thread)
        while data_queue.qsize() != 0 and len(threads) <= 10:
            thread = threading.Thread(target=get_sold_date2, args=(data_queue.get()))
            thread.setDaemon(True)
            thread.start()
            threads.append(thread)

def start_spider():
    start_urls = start_url
    for url in start_urls:
        cag = [url[i] for i in range(2,6)]
        pgn = int(url["count"]/44)
        if pgn == 0:
            pgn = 1
        if pgn >100:
            pgn = 100
        url["url"] = url["url"].replace(".html","{}.html")
        sku_list(cag=cag,url=url["url"],pgn=pgn)

if __name__ == "__main__":
    client = MongoClient('localhost', 27017)
    db = client["AE_pet"]["pet"]
    db1 = client["AE_pet"]["fenbu"]
    sku_file = open("sku.csv", "w", encoding="utf-8", newline="")
    sku_writer = csv.writer(sku_file)
    sku_writer.writerow(
        ["id", "ownerMemberId", "price", "votes", "orders", "seller", "location", "open_time", "item_sps",
         "shipping_price","cag", "url"])

    start_spider()
#send_data = {"ownerMemberId":"",
                 "memberType":"seller",
                 "productId":"",
                 "evaStarFilterValue":"all+Stars",
                 "evaSortValue":"sortdefault@feedback",
                 "page":1,
                 "currentPage":2,
                 "i18n":"true",
                 "withPictures":"false",
                 "withPersonalInfo":"false",
                 "withAdditionalFeedback":"false",
                 "onlyFromMyCountry":"false",
                 "version":"0.0.0",
                 "translate":"+Y+",
                 "jumpToTop":"true"
                 }
