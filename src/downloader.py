import os
import time
import random
import requests
import logging
import pandas as pd
import math
try:
    from src.config import USER_AGENTS, CNINFO_SEARCH_URL, CNINFO_BASE_URL, PDF_DIR, DATA_DIR
except ImportError:
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

    def search_prospectus(self, code, name, org_id):
        """
        搜索招股说明书
        """
        # Common params initialization
        params = {
            'pageNum': 1,
            'pageSize': 30,
            'column': 'szse', 
            'tabName': 'fulltext',
            'plate': '',
            'stock': f'{code},{org_id}',
            'searchkey': '招股说明书',
            'secid': '',
            'category': 'category_30_0202;category_30_0102', 
            'trade': '',
            'seDate': '', 
            'sortName': '',
            'sortType': '',
            'isHLtitle': 'true'
        }
        
        if code.startswith('6'):
            params['column'] = 'sse'
        elif code.startswith('8') or code.startswith('4'):
            params['column'] = 'bse'
        
        try:
            time.sleep(random.uniform(1, 3))
            
            # --- Strategy 1: Standard Search (Stock + Category + Keyword) ---
            # NOTE: Analysis shows some docs are NOT indexed with "招股说明书" keyword properly in the search engine,
            # even if the title contains it! (e.g. 603171, 300929).
            # They ARE found when searching by DATE range or broad browsing.
            
            logger.info(f"搜索尝试 1 (标准): {code}")
            response = self.session.post(CNINFO_SEARCH_URL, data=params, timeout=15)
            response.raise_for_status()
            data = response.json()
            
            announcements = data.get('announcements', [])
            if announcements is None: announcements = []
            
            # --- Strategy 2: Relaxed Category (Stock + Keyword Only) ---
            if not announcements:
                logger.info(f"搜索尝试 2 (放宽分类): {code}")
                params['category'] = '' # Remove category constraint
                time.sleep(random.uniform(1, 3))
                response = self.session.post(CNINFO_SEARCH_URL, data=params, timeout=15)
                response.raise_for_status()
                data = response.json()
                announcements = data.get('announcements', [])
                if announcements is None: announcements = []

            # --- Strategy 3: Name Search (Name + Keyword) ---
            if not announcements and name:
                logger.info(f"搜索尝试 3 (按名称): {name}")
                params['stock'] = '' # Remove stock code constraint
                clean_name = name.replace('ST', '').replace('*', '').strip()
                params['searchkey'] = f'{clean_name} 招股说明书'
                
                time.sleep(random.uniform(1, 3))
                response = self.session.post(CNINFO_SEARCH_URL, data=params, timeout=15)
                response.raise_for_status()
                data = response.json()
                announcements = data.get('announcements', [])
                if announcements is None: announcements = []

            # --- Strategy 4: "Public Offering" Keyword (Name + '公开发行') ---
            if not announcements and name:
                logger.info(f"搜索尝试 4 (公开发行): {name}")
                params['stock'] = ''
                clean_name = name.replace('ST', '').replace('*', '').strip()
                params['searchkey'] = f'{clean_name} 公开发行'
                
                time.sleep(random.uniform(1, 3))
                response = self.session.post(CNINFO_SEARCH_URL, data=params, timeout=15)
                response.raise_for_status()
                data = response.json()
                announcements = data.get('announcements', [])
                if announcements is None: announcements = []

            # --- Strategy 5: Sort by Pub Date ASC for Code (Oldest first often has IPO docs) ---
            # KEY FIX: The previous failure analysis showed that searching by keyword FAILED even if title matched.
            # But searching by DATE worked. Since we don't know the date, we MUST rely on sorting by date (oldest first)
            # and paging through enough results.
            # User feedback indicates: "Look from the last page forward" or "Find the earliest announcements".
            # "sortType": "asc" on "pubdate" should theoretically do this (Oldest -> Newest).
            # But sometimes API default is DESC (Newest -> Oldest).
            # If "asc" works, page 1 should be the oldest.
            # If "asc" is ignored, we need to find total pages and request the last page.
            
            if not announcements:
                logger.info(f"搜索尝试 5 (按日期正序 - 关键策略): {code}")
                # Reset critical params
                params['stock'] = f'{code},{org_id}'
                params['searchkey'] = '' # Clear keyword to find ANYTHING
                params['category'] = '' # Clear category to find ANYTHING
                params['sortName'] = 'pubdate'
                params['sortType'] = 'asc' # Oldest first attempt
                params['pageSize'] = 50 
                
                time.sleep(random.uniform(1, 3))
                response = self.session.post(CNINFO_SEARCH_URL, data=params, timeout=15)
                response.raise_for_status()
                data = response.json()
                announcements = data.get('announcements', [])
                if announcements is None: announcements = []

                # --- Strategy 5b: If 'asc' didn't give us early dates (check first item date?), try getting total pages and fetching LAST page ---
                # Check year of first item. If it's current year (e.g., 2025), then ASC failed.
                is_recent = False
                if announcements:
                    first_date = announcements[0].get('announcementTime', 0)
                    # Simple check: if timestamp is > 2024 roughly
                    if first_date > 1704067200000: # 2024-01-01
                         is_recent = True
                         logger.info(f"策略5返回了近期数据，说明排序可能失效。尝试获取最后一页数据。")
                
                if not announcements or is_recent:
                     # Get total pages from previous response if available, or just guess
                     total_pages = 0
                     if data.get('totalpages'):
                         total_pages = data.get('totalpages')
                     elif data.get('totalRecordNum'):
                         total_pages = math.ceil(int(data.get('totalRecordNum')) / 50)
                     
                     if total_pages > 1:
                         logger.info(f"尝试获取最后一页 (第 {total_pages} 页)...")
                         params['pageNum'] = total_pages
                         params['sortType'] = '' # Reset sort type to default (usually DESC)
                         
                         time.sleep(random.uniform(1, 3))
                         response = self.session.post(CNINFO_SEARCH_URL, data=params, timeout=15)
                         response.raise_for_status()
                         data = response.json()
                         page_announcements = data.get('announcements', [])
                         if page_announcements:
                             # Insert at beginning because these are likely older
                             announcements = page_announcements + announcements
                         
                         # Also try second to last page just in case
                         if total_pages > 1:
                             params['pageNum'] = total_pages - 1
                             time.sleep(random.uniform(1, 2))
                             response = self.session.post(CNINFO_SEARCH_URL, data=params, timeout=15)
                             if response.ok:
                                 prev_page_data = response.json().get('announcements', [])
                                 if prev_page_data:
                                     announcements = prev_page_data + announcements

            # Filter logic
            exclude_keywords = ["摘要", "更正", "提示", "发行结果", "网上路演", "意见", "法律", "反馈", 
                                "H股", "增发", "配股", "可转债", "转换公司债券"]
            
            candidates = []
            for ann in announcements:
                title = ann['announcementTitle']
                # Basic filter - STRICTLY exclude unwanted types
                if any(kw in title for kw in exclude_keywords):
                    continue
                
                # Accept both 招股说明书 and 招股意向书
                if "招股说明书" in title or "招股意向书" in title:
                    candidates.append(ann)

            if candidates:
                return candidates

            # --- Strategy 6: Final Resort - Search for "Listing Announcement" which might reveal prospectus in same context or be acceptable fallback if prospectus is truly missing in this index ---
            if not candidates and name:
                 logger.info(f"搜索尝试 6 (上市公告书 - 间接查找): {name}")
                 params['stock'] = '' # No code
                 clean_name = name.replace('ST', '').replace('*', '').strip()
                 params['searchkey'] = f'{clean_name} 上市公告书'
                 params['category'] = ''
                 params['sortName'] = ''
                 params['sortType'] = ''
                 
                 time.sleep(random.uniform(1, 3))
                 response = self.session.post(CNINFO_SEARCH_URL, data=params, timeout=15)
                 response.raise_for_status()
                 data = response.json()
                 announcements = data.get('announcements', [])
                 
                 # Check if announcements is None
                 if announcements is None: announcements = []
                 
                 for ann in announcements:
                    title = ann['announcementTitle']
                    if any(kw in title for kw in exclude_keywords): continue
                    
                    # If we find the Listing Announcement, sometimes the Prospectus is listed nearby or with similar title
                    # But here we strictly look for "招股"
                    if "招股说明书" in title or "招股意向书" in title:
                        candidates.append(ann)
            
            # --- Strategy 7: Super Broad - Name + "公开发行" with NO category, NO sort ---
            if not candidates and name:
                 logger.info(f"搜索尝试 7 (超级宽泛 - 公开发行): {name}")
                 params['stock'] = ''
                 clean_name = name.replace('ST', '').replace('*', '').strip()
                 params['searchkey'] = f'{clean_name} 公开发行'
                 params['category'] = ''
                 params['pageSize'] = 50 
                 
                 time.sleep(random.uniform(1, 3))
                 response = self.session.post(CNINFO_SEARCH_URL, data=params, timeout=15)
                 response.raise_for_status()
                 data = response.json()
                 announcements = data.get('announcements', [])
                 if announcements is None: announcements = []

                 for ann in announcements:
                    title = ann['announcementTitle']
                    if any(kw in title for kw in exclude_keywords): continue
                    
                    if "招股说明书" in title or "招股意向书" in title:
                        candidates.append(ann)
            
            # --- Strategy 8: Super Broad - Name only + NO category ---
            # This is risky and returns many results, but we filter client side
            if not candidates and name:
                 logger.info(f"搜索尝试 8 (终极尝试 - 仅按名称): {name}")
                 params['stock'] = ''
                 clean_name = name.replace('ST', '').replace('*', '').strip()
                 params['searchkey'] = clean_name
                 params['category'] = ''
                 params['pageSize'] = 100 # Maximum usually allowed
                 params['sortName'] = 'pubdate'
                 params['sortType'] = 'asc' # Try oldest first again with just name
                 
                 time.sleep(random.uniform(1, 3))
                 response = self.session.post(CNINFO_SEARCH_URL, data=params, timeout=15)
                 response.raise_for_status()
                 data = response.json()
                 announcements = data.get('announcements', [])
                 if announcements is None: announcements = []

                 for ann in announcements:
                    title = ann['announcementTitle']
                    if any(kw in title for kw in exclude_keywords): continue
                    
                    if "招股说明书" in title or "招股意向书" in title:
                        candidates.append(ann)

            return candidates

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
            
            temp_filepath = filepath + '.tmp'
            with open(temp_filepath, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
            
            # Atomic rename
            os.replace(temp_filepath, filepath)
            logger.info(f"下载成功: {filepath}")
            return True
        except Exception as e:
            logger.error(f"下载失败 ({url}): {e}")
            if os.path.exists(filepath):
                os.remove(filepath)
            if os.path.exists(filepath + '.tmp'):
                os.remove(filepath + '.tmp')
            return False

    def process_stock(self, code, name):
        """
        处理单个股票：搜索并下载招股书
        """
        code = str(code).zfill(6)
        
        # Check if already exists (simple check)
        # Note: This checks strictly for code_name.pdf. 
        # If name is different in file system, it might re-download.
        # But for concurrency, we rely on file existence check in download_file too.
        safe_name = name.replace('*', '').replace(':', '').replace('?', '')
        filename = f"{code}_{safe_name}.pdf"
        filepath = os.path.join(PDF_DIR, filename)
        
        if os.path.exists(filepath):
            logger.info(f"{code} {name} 已存在文件，跳过")
            return
            
        logger.info(f"正在处理 {code} {name}...")
        
        org_id = self.get_org_id(code)
        if not org_id:
            logger.warning(f"无法获取 orgId: {code}")
            return
        
        announcements = self.search_prospectus(code, name, org_id)
        if not announcements:
            logger.warning(f"未找到招股书: {code}")
            return
        
        # Selection Strategy
        target_announcement = None
        
        # Scoring strategy:
        # 1. Prefer '首次' (Initial) -> +200
        # 2. Prefer '注册稿' or '封卷稿' (Final versions) -> +100
        # 3. Prefer exact '招股说明书' without '申报稿' -> +50
        # 4. Avoid '申报稿' if possible -> -10
        
        def get_score(ann_item):
            t = ann_item['announcementTitle']
            score = 0
            if '首次' in t: score += 200
            if '注册稿' in t: score += 100
            if '封卷稿' in t: score += 90
            if '申报稿' not in t and '注册稿' not in t: score += 50
            if '申报稿' in t: score -= 10
            return score

        announcements.sort(key=get_score, reverse=True)
        target_announcement = announcements[0]
        
        if len(announcements) > 1:
             titles = [c['announcementTitle'] for c in announcements[:3]]
             logger.info(f"多份招股书候选项，已选择: {target_announcement['announcementTitle']}. (候选项: {titles})")

        # Construct Download URL
        adjunct_url = target_announcement['adjunctUrl']
        download_url = CNINFO_BASE_URL + adjunct_url
        
        self.download_file(download_url, filepath)

    def run(self, stock_list_path=None):
        if not stock_list_path:
            stock_list_path = os.path.join(DATA_DIR, 'stock_list.csv')
        
        if not os.path.exists(stock_list_path):
            logger.error(f"找不到股票列表文件: {stock_list_path}")
            return

        df = pd.read_csv(stock_list_path)
        logger.info(f"加载了 {len(df)} 个待处理股票")

        # Optimization: Pre-scan directory to avoid os.listdir() in loop
        # Store just the stock codes (first part of filename) for faster lookup
        existing_codes = set()
        if os.path.exists(PDF_DIR):
            for f in os.listdir(PDF_DIR):
                if '_' in f:
                    existing_codes.add(f.split('_')[0])
        
        logger.info(f"本地已存在 {len(existing_codes)} 个股票的招股书")

        for index, row in df.iterrows():
            code = str(row['code']).zfill(6)
            name = row['name']
            
            if code in existing_codes:
                continue

            self.process_stock(code, name)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - [PID:%(process)d] - %(levelname)s - %(message)s')
    downloader = Downloader()
    downloader.run()
