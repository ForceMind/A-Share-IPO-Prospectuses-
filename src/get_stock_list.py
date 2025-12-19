import requests
import pandas as pd
import logging
import os
from datetime import datetime
from config import EASTMONEY_LIST_URL, START_DATE, END_DATE, DATA_DIR

# 设置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def get_stock_list():
    """
    从东方财富获取所有A股列表，并筛选指定上市日期范围内的公司
    """
    stock_list = []
    page = 1
    page_size = 100
    
    start_ts = int(START_DATE.replace('-', ''))
    end_ts = int(END_DATE.replace('-', ''))

    logger.info("开始获取股票列表...")

    while True:
        params = {
            'pn': page,
            'pz': page_size,
            'po': 1,
            'np': 1,
            'ut': 'bd1d9ddb04089700cf9c27f6f7426281',
            'fltt': 2,
            'invt': 2,
            'fid': 'f26',
            'fs': 'm:0+t:6,m:0+t:80,m:1+t:2,m:1+t:23,m:0+t:81+s:2048',
            'fields': 'f12,f14,f26,f100,f102,f103',
            '_': 1623833739532
        }

        try:
            logger.info(f"正在获取第 {page} 页数据...")
            response = requests.get(EASTMONEY_LIST_URL, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()
            
            if data.get('data') is None or 'diff' not in data['data']:
                logger.info("数据获取完毕或无数据")
                break

            stocks = data['data']['diff']
            if not stocks:
                break
            
            # 统计本页有效数据
            valid_count = 0
            for stock in stocks:
                code = stock['f12']
                name = stock['f14']
                listing_date = stock['f26']
                industry = stock.get('f100', 'Unknown') # f100 seems to be the industry sector (e.g. 半导体)

                if listing_date == '-' or not listing_date:
                    continue
                
                # 排除 92 开头的股票 (北京证券交易所部分股票)
                if code.startswith('92'):
                    continue
                
                try:
                    listing_date_int = int(listing_date)
                except ValueError:
                    continue

                if start_ts <= listing_date_int <= end_ts:
                    date_str = datetime.strptime(str(listing_date_int), '%Y%m%d').strftime('%Y-%m-%d')
                    stock_list.append({
                        'code': code,
                        'name': name,
                        'listing_date': date_str,
                        'industry': industry
                    })
                    valid_count += 1
            
            logger.info(f"第 {page} 页获取 {len(stocks)} 条，其中符合条件的有 {valid_count} 条")
            
            # 如果获取的数量小于页大小，说明是最后一页
            if len(stocks) < page_size:
                break
                
            page += 1

        except Exception as e:
            logger.error(f"获取股票列表失败: {e}")
            break

    # Save results
    if stock_list:
        df = pd.DataFrame(stock_list)
        logger.info(f"总计筛选出 {len(df)} 家在 {START_DATE} 至 {END_DATE} 期间上市的公司")
        
        output_path = os.path.join(DATA_DIR, 'stock_list.csv')
        df.to_csv(output_path, index=False, encoding='utf-8-sig')
        logger.info(f"股票列表已保存至 {output_path}")
    else:
        logger.warning("未找到符合条件的公司")

if __name__ == '__main__':
    get_stock_list()
