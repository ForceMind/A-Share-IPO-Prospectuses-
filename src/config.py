import os

# 基础路径配置
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, 'data')
PDF_DIR = os.path.join(DATA_DIR, 'pdfs')
OUTPUT_DIR = os.path.join(DATA_DIR, 'output')
LOG_DIR = os.path.join(BASE_DIR, 'logs')

# 确保目录存在
for d in [DATA_DIR, PDF_DIR, OUTPUT_DIR, LOG_DIR]:
    os.makedirs(d, exist_ok=True)

# 目标日期范围
START_DATE = '2019-01-01'
END_DATE = '2023-12-31'

# 爬虫配置
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0 Safari/605.1.15',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0'
]

# Logging Format
LOG_FORMAT = '%(asctime)s - [PID:%(process)d] - %(levelname)s - %(message)s'

# 巨潮资讯网搜索接口
CNINFO_SEARCH_URL = 'http://www.cninfo.com.cn/new/hisAnnouncement/query'
CNINFO_BASE_URL = 'http://static.cninfo.com.cn/'

# 东方财富列表接口
EASTMONEY_LIST_URL = 'https://push2.eastmoney.com/api/qt/clist/get'
