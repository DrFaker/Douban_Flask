import random
import string
import requests
from bs4 import BeautifulSoup
import re
import pymysql
from urllib.parse import urlencode
from lxml import etree
from opencc import OpenCC
import time

# Regular expressions
findLink = re.compile(r'<a href="(.*?)">')
findImgSrc = re.compile(r'<img.*src="(.*?)"', re.S)
findTitle = re.compile(r'<span class="title">(.*)</span>')
findRating = re.compile(r'<span class="rating_num" property="v:average">(.*)</span>')
findJudge = re.compile(r'<span>(\d*)人评价</span>')
findInq = re.compile(r'<span class="inq">(.*)</span>')
findBd = re.compile(r'<p class="">(.*?)</p>', re.S)

def get_bid():
    """Generate random bid value"""
    return ''.join(random.choices(string.ascii_letters + string.digits, k=11))

def askURL(url, session, headers, max_retries=10, delay=5):
    """
    Send request and get page content with retry logic.
    If blocked (HTTP 403), it retries with a delay up to max_retries times.
    """
    for attempt in range(max_retries):
        try:
            print(f"Attempting to fetch URL: {url} [Attempt {attempt + 1}/{max_retries}]")
            response = session.get(url, headers=headers, timeout=10)
            if response.status_code == 200:
                print(f"Successfully fetched URL: {url}")
                return response.text
            elif response.status_code == 403:
                print(f"[Attempt {attempt + 1}/{max_retries}] 403 Forbidden: Access denied. Retrying after {delay} seconds...")
                session.cookies.clear()
                headers['Cookie'] = f'bid={get_bid()}'
                time.sleep(delay)  # Wait before retrying
            else:
                print(f"Request failed with status code: {response.status_code}")
                return None
        except Exception as e:
            print(f"[Attempt {attempt + 1}/{max_retries}] Request exception: {e}. Retrying after {delay} seconds...")
            session.cookies.clear()
            headers['Cookie'] = f'bid={get_bid()}'
            time.sleep(delay)  # Wait before retrying
    print("Max retries reached. Skipping this URL.")
    return None


def get_comment_page(comment_link, start, session, headers, max_retries=10, delay=5):
    """
    Get comment page with retry logic.
    If blocked (HTTP 403), it retries with a delay up to max_retries times.
    """
    params = {
        'start': start,
        'limit': '20',
        'sort': 'new_score',
        'status': 'P'
    }
    url = comment_link + 'comments?' + urlencode(params)
    for attempt in range(max_retries):
        try:
            print(f"Attempting to fetch comment page: {url} [Attempt {attempt + 1}/{max_retries}]")
            response = session.get(url, headers=headers, timeout=10)
            if response.status_code == 200:
                print(f"Successfully fetched comment page: {url}")
                return response.text
            elif response.status_code == 403:
                print(f"[Attempt {attempt + 1}/{max_retries}] 403 Forbidden while fetching comments. Retrying after {delay} seconds...")
                session.cookies.clear()
                headers['Cookie'] = f'bid={get_bid()}'
                time.sleep(delay)  # Wait before retrying
        except Exception as e:
            print(f"[Attempt {attempt + 1}/{max_retries}] Failed to get comment page: {e}. Retrying after {delay} seconds...")
            session.cookies.clear()
            headers['Cookie'] = f'bid={get_bid()}'
            time.sleep(delay)  # Wait before retrying
    print("Max retries reached. Skipping comment page.")
    return None

def get_comment(comment_page):
    """Extract comments from comment page"""
    html = etree.HTML(comment_page)
    result = html.xpath('//div[@class="mod-bd"]//div/div[@class="comment"]/p/span/text()')
    return result

def clear(string):
    """Clean comment data"""
    string = string.strip()
    string = re.sub("[A-Za-z0-9]", "", string)
    string = re.sub(r"[！!？｡。，&;＂★＃＄％＆＇（）＊＋－／：；＜＝＞＠［＼］＾＿｀｛｜｝～｟｠｢｣､、〃「」『』【】"
                    r"〔〕〖〗〘〙#〚〛〜〝〞/?=~〟,〰–—‘’‛“”„‟…‧﹏.]", " ", string)
    string = re.sub(r"[!\'\"#。$%&()*+,-.←→/:~;<=>?@[\\]^_`_{|}~", " ", string)
    cc = OpenCC('t2s')  # Traditional to Simplified Chinese
    return cc.convert(string).lower()

def recreate_database():
    """Drop and recreate the database"""
    conn = pymysql.connect(
        host='127.0.0.1',
        port=3306,
        user='root',
        passwd='yfg,123456',
        charset='utf8mb4'
    )
    cursor = conn.cursor()
    cursor.execute("DROP DATABASE IF EXISTS spider")
    cursor.execute("CREATE DATABASE spider CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci")
    cursor.execute("USE spider")
    cursor.execute('''
        CREATE TABLE movie250 (
            id INT AUTO_INCREMENT PRIMARY KEY,
            info_link VARCHAR(255),
            pic_link VARCHAR(255),
            cname VARCHAR(255),
            ename VARCHAR(255),
            score VARCHAR(10),
            rated VARCHAR(10),
            introduce TEXT,
            info TEXT,
            comments TEXT
        ) CHARSET=utf8mb4
    ''')
    conn.commit()
    cursor.close()
    conn.close()

def getData(baseurl, session, headers):
    """Get movie data with enhanced error handling."""
    for i in range(0, 10):  # 10 pages, 250 items
        url = baseurl + str(i * 25)
        print(f"Fetching page {i + 1}/10: {url}")
        html = askURL(url, session, headers)
        if not html:
            print(f"Failed to fetch page {i + 1}/10: {url}. Skipping...")
            continue
        soup = BeautifulSoup(html, "html.parser")
        for idx, item in enumerate(soup.find_all('div', class_="item")):
            try:
                print(f"Processing movie {i * 25 + idx + 1}...")
                data = []
                item = str(item)

                link = re.findall(findLink, item)[0]
                data.append(link)
                imgSrc = re.findall(findImgSrc, item)[0]
                data.append(imgSrc)
                titles = re.findall(findTitle, item)
                if len(titles) == 2:
                    data.append(titles[0])
                    data.append(titles[1].replace('/', ' '))
                else:
                    data.append(titles[0])
                    data.append('')
                rating = re.findall(findRating, item)[0]
                data.append(rating)
                judgeNum = re.findall(findJudge, item)[0]
                data.append(judgeNum)
                inq = re.findall(findInq, item)
                data.append(inq[0].replace("。", "") if inq else '')
                bd = re.findall(findBd, item)[0]
                bd = re.sub('<br(\\s+)?/>(\\s+)?', " ", bd)
                bd = re.sub('/', " ", bd)
                data.append(bd.strip())

                # Get comments
                comments = []
                for start in range(0, 100, 20):  # Fetch 5 pages, 20 items per page
                    print(f"Fetching comments for movie {i * 25 + idx + 1} (start={start})...")
                    comment_page = get_comment_page(link, start, session, headers)
                    if not comment_page:
                        print(f"Failed to fetch comments for movie {i * 25 + idx + 1} (start={start}). Skipping...")
                        break
                    results = get_comment(comment_page)
                    comments.extend(results)
                    if len(comments) >= 5:
                        break
                comments = [clear(comment) for comment in comments[:5]]  # Clean comment data
                data.append(" | ".join(comments))  # Separate comments with |

                print(f"Scraped movie {i * 25 + idx + 1}: {titles[0]}")
                saveToMysql(data)  # Save each movie data immediately
            except Exception as e:
                print(f"Error processing movie {i * 25 + idx + 1}: {e}")


def saveToMysql(data):
    """Save data to database with utf8mb4 charset."""
    conn = pymysql.connect(
        host='127.0.0.1',
        port=3306,
        user='root',
        passwd='yfg,123456',
        db='spider',
        charset='utf8mb4'
    )
    cursor = conn.cursor()
    sql = '''
        INSERT INTO movie250(info_link, pic_link, cname, ename, score, rated, introduce, info, comments)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    '''
    try:
        cursor.execute(sql, data)
        conn.commit()
    except Exception as e:
        print(f"Error saving to database: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

def main():
    baseurl = "https://movie.douban.com/top250?start="
    session = requests.Session()
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.93 Safari/537.36",
        "Cookie": f"bid={get_bid()}"
    }

    recreate_database()  # Recreate the database at the start
    getData(baseurl, session, headers)
    time.sleep(2)  # Add delay to avoid being blocked

if __name__ == "__main__":
    main()
    print("Scraping completed")