import os
import time
import random
import requests
import logging
import pandas as pd
from config import USER_AGENTS, CNINFO_SEARCH_URL, CNINFO_BASE_URL, PDF_DIR, DATA_DIR

logger = logging.getLogger(__name__)

class Downloader:
    def __init__(self):
        self.session = requests.Session()
        self.headers = {
            'User-Agent': random.choice(USER_AGENTS),
            'Accept': '*/*',
            'Origin': 'http://www.cninfo.com.cn',
            'Referer': 'http://www.cninfo.com.cn/new/disclosure/stock',
            'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8'
        }
        self.session.headers.update(self.headers)

    def get_org_id(self, code):
        """
        根据股票代码获取巨潮资讯网的 orgId
        """
        url = 'http://www.cninfo.com.cn/new/information/topSearch/query'
        params = {'keyWord': code}
        try:
            # Random sleep to avoid ban
            time.sleep(random.uniform(0.5, 1.5))
            
            response = self.session.post(url, data=params, timeout=10)
            response.raise_for_status()
            data = response.json()
            
            for item in data:
                if item.get('code') == code:
                    org_id = item.get('orgId')
                    logger.info(f"获取到 orgId: {code} -> {org_id}")
                    return org_id
            logger.warning(f"无法找到 orgId: {code}")
            return None
        except Exception as e:
            logger.error(f"获取 orgId 失败 ({code}): {e}")
            return None

    def search_prospectus(self, code, org_id):
        """
        搜索招股说明书
        """
        params = {
            'pageNum': 1,
            'pageSize': 30,
            'column': 'szse', # 默认深市，但通常不影响搜索结果，或者需要根据代码判断
            'tabName': 'fulltext',
            'plate': '',
            'stock': f'{code},{org_id}',
            'searchkey': '招股说明书', # 显式搜索关键词
            'secid': '',
            'category': '', # 移除特定分类，扩大搜索范围
            'trade': '',
            'seDate': '', # 可以指定时间范围 '2019-01-01~2023-12-31'
            'sortName': '',
            'sortType': '',
            'isHLtitle': 'true'
        }
        
        # 简单的代码判断板块
        if code.startswith('6'):
            params['column'] = 'sse'
        elif code.startswith('8') or code.startswith('4'): # 北交所
            params['column'] = 'bse'
        
        try:
            time.sleep(random.uniform(1, 3))
            response = self.session.post(CNINFO_SEARCH_URL, data=params, timeout=15)
            response.raise_for_status()
            data = response.json()
            
            if data.get('announcements'):
                return data['announcements']
            return []
        except Exception as e:
            logger.error(f"搜索招股书失败 ({code}): {e}")
            return []

    def download_file(self, url, filepath):
        """
        下载文件
        """
        if os.path.exists(filepath):
            logger.info(f"文件已存在，跳过: {filepath}")
            return True

        try:
            time.sleep(random.uniform(1, 4))
            response = self.session.get(url, stream=True, timeout=30)
            response.raise_for_status()
            
            with open(filepath, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
            logger.info(f"下载成功: {filepath}")
            return True
        except Exception as e:
            logger.error(f"下载失败 ({url}): {e}")
            if os.path.exists(filepath):
                os.remove(filepath)
            return False

    def run(self, stock_list_path=None):
        if not stock_list_path:
            stock_list_path = os.path.join(DATA_DIR, 'stock_list.csv')
        
        if not os.path.exists(stock_list_path):
            logger.error(f"找不到股票列表文件: {stock_list_path}")
            return

        df = pd.read_csv(stock_list_path)
        logger.info(f"加载了 {len(df)} 个待处理股票")

        for index, row in df.iterrows():
            code = str(row['code']).zfill(6) # Ensure 6 digits
            name = row['name']
            
            # Check if already downloaded
            # 简单的检查：只要该目录下有包含该代码的文件即可
            existing_files = [f for f in os.listdir(PDF_DIR) if f.startswith(code)]
            if existing_files:
                logger.info(f"[{index+1}/{len(df)}] {code} {name} 已存在文件，跳过")
                continue

            logger.info(f"[{index+1}/{len(df)}] 正在处理 {code} {name}...")
            
            org_id = self.get_org_id(code)
            if not org_id:
                logger.warning(f"无法获取 orgId: {code}")
                continue
            
            announcements = self.search_prospectus(code, org_id)
            if not announcements:
                logger.warning(f"未找到招股书: {code}")
                continue
            
            # Filter logic: 
            # 1. Title contains "招股说明书"
            # 2. Title does NOT contain "摘要", "更正", "提示"
            # 3. Sort by time (usually API returns sorted, but we pick the best one)
            
            target_announcement = None
            
            # DEBUG: Print all titles
            all_titles = [a['announcementTitle'] for a in announcements]
            logger.info(f"搜索到的公告标题: {all_titles}")

            for ann in announcements:
                title = ann['announcementTitle']
                if "招股说明书" in title and "摘要" not in title and "更正" not in title and "提示" not in title:
                    # 优先找“注册稿”或“上市公告书”(不对，是招股书)
                    # 这里的逻辑是：通常最新的那个（列表第一个）就是最终版
                    target_announcement = ann
                    break
            
            if not target_announcement:
                logger.warning(f"未找到符合条件的招股书文件: {code}. 可用标题: {all_titles[:3]}...")
                continue
                
            # Construct Download URL
            adjunct_url = target_announcement['adjunctUrl']
            download_url = CNINFO_BASE_URL + adjunct_url
            
            # Clean filename
            safe_name = name.replace('*', '').replace(':', '').replace('?', '')
            filename = f"{code}_{safe_name}.pdf"
            filepath = os.path.join(PDF_DIR, filename)
            
            self.download_file(download_url, filepath)

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    downloader = Downloader()
    downloader.run()
