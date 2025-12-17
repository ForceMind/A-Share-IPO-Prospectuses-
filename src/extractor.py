import pdfplumber
import re
import logging
import pandas as pd

logger = logging.getLogger(__name__)

class ProspectusExtractor:
    def __init__(self):
        self.keywords = ['股利分配', '现金分红', '利润分配']
        self.context_positive = ['每10股', '派发现金', '含税', '实施完毕', '分红金额', '现金分红', '报告期', '最近三年']
        self.context_negative = ['风险', '不确定性', '......', '目录', '详见', '参见', '分配政策', '分配原则', '章程', '规划', '未来']
        # 正则表达式匹配“年度”和“金额”
        self.year_pattern = re.compile(r'20[1-2][0-9]年?')
        self.amount_pattern = re.compile(r'(\d{1,3}(,\d{3})*(\.\d+)?)')
        self.year_check_pattern = re.compile(r'20(16|17|18|19|20|21|22|23)')

    def extract(self, pdf_path):
        """
        从 PDF 中提取分红数据
        返回: list of dict [{'year': '2020', 'amount': 1000.0, 'page': 123}]
        """
        result = []
        try:
            with pdfplumber.open(pdf_path) as pdf:
                # 1. 定位目标页面
                target_pages = self._locate_target_pages(pdf)
                if not target_pages:
                    logger.warning(f"未定位到分红章节: {pdf_path}")
                    return []
                
                logger.info(f"定位到目标页面: {target_pages}")

                # 2. 尝试从目标页面及其后几页提取数据
                # 扩大搜索范围：通常“具体分红情况”在“分红政策”章节之后 1-3 页
                pages_to_scan = set()
                for p in target_pages:
                    for offset in range(4): # 扫描当前页及后3页
                        if p + offset < len(pdf.pages):
                            pages_to_scan.add(p + offset)
                
                logger.info(f"扩展后的扫描页面: {sorted(list(pages_to_scan))}")

                for page_num in sorted(list(pages_to_scan)):
                    page = pdf.pages[page_num]
                    
                    # 优先尝试提取表格
                    tables = page.extract_tables()
                    data_from_table = self._process_tables(tables, page_num)
                    if data_from_table:
                        result.extend(data_from_table)
                    
                    # 总是尝试正则提取文本（因为有些表格可能是排版错乱的，或者数据在正文中）
                    text = page.extract_text()
                    data_from_text = self._process_text(text, page_num)
                    if data_from_text:
                        result.extend(data_from_text)
                
                # 3. 去重和清洗
                return self._clean_result(result)

        except Exception as e:
            logger.error(f"解析 PDF 失败 {pdf_path}: {e}")
            return []

    def _locate_target_pages(self, pdf):
        """
        定位可能包含分红数据的页码
        """
        scores = {}
        total_pages = len(pdf.pages)
        
        # 跳过前30页（通常是目录）和最后几页（附录）
        start_page = min(30, total_pages // 10)
        end_page = max(total_pages - 10, int(total_pages * 0.9))

        for i in range(start_page, end_page):
            try:
                page = pdf.pages[i]
                text = page.extract_text()
                if not text:
                    continue
                
                score = 0
                
                # 基础关键词匹配
                for kw in self.keywords:
                    if kw in text:
                        score += 10
                
                if score == 0:
                    continue

                # 必须包含具体年份才可能是历史数据
                if self.year_check_pattern.search(text):
                    score += 20
                else:
                    score -= 20 # 没有年份通常是纯政策描述

                # 上下文加分
                for cw in self.context_positive:
                    if cw in text:
                        score += 5
                
                # 负面词减分
                for nw in self.context_negative:
                    if nw in text:
                        score -= 15 # 加大负面词惩罚
                
                # 标题特征 (简单判断：关键词是否在某一行单独出现，或者行首)
                lines = text.split('\n')
                for line in lines:
                    line = line.strip()
                    if any(kw in line for kw in self.keywords):
                        # 如果这行很短，或者是加粗标题格式（pdfplumber难判断加粗，但可以判断长度）
                        if len(line) < 30 and (line.startswith('十') or line.startswith('九') or line.startswith('（')):
                            score += 20
                        # 目录特征
                        if '......' in line:
                            score -= 50
                        # 政策/规划章节特征
                        if '政策' in line or '规划' in line or '原则' in line:
                            score -= 30

                if score > 15:
                    scores[i] = score

            except Exception:
                continue
        
        # 返回分数最高的前 3 页
        sorted_pages = sorted(scores.items(), key=lambda x: x[1], reverse=True)
        return [p[0] for p in sorted_pages[:3]]

    def _process_tables(self, tables, page_num):
        """
        处理提取到的表格
        """
        extracted_data = []
        if not tables:
            return []

        for table in tables:
            # 检查表头
            # 我们寻找包含 "年度" 和 "分红"|"金额" 的列
            # 很多表格第一行是header
            
            # 扁平化处理，寻找包含年份的行
            for row in table:
                # 清洗 None
                row = [str(cell).strip() if cell else '' for cell in row]
                row_text = ' '.join(row)
                
                # 匹配年份 (2019-2023)
                year_match = self.year_pattern.search(row_text)
                if not year_match:
                    continue
                
                year = year_match.group()
                
                # 寻找金额
                # 简单的启发式：寻找行中最大的数字，或者出现在“现金分红”列下的数字
                # 这里做简化处理：提取行中所有的数字，假设分红金额通常较大（但不是年份）
                
                amounts = []
                for cell in row:
                    # 去除逗号
                    cell_clean = cell.replace(',', '')
                    try:
                        val = float(cell_clean)
                        # 排除年份本身
                        if 2010 < val < 2030: 
                            continue
                        if val > 0:
                            amounts.append(val)
                    except ValueError:
                        continue
                
                if amounts:
                    # 假设最大的那个是分红金额（这有风险，可能是股本）
                    # 更好的逻辑是结合表头，但表头很难通用解析
                    # 此时记录所有候选，留给后续清洗
                    extracted_data.append({
                        'year': year,
                        'amount_candidates': amounts,
                        'page': page_num + 1, # 人类阅读习惯从1开始
                        'raw_row': row
                    })

        return extracted_data

    def _process_text(self, text, page_num):
        """
        从文本中正则提取
        模式：20xx年度......现金分红......xxx万元
        """
        results = []
        lines = text.split('\n')
        for line in lines:
            year_match = self.year_pattern.search(line)
            if year_match and ('分红' in line or '派发' in line):
                # 提取金额
                # 寻找 "xxx万元" 或 "xxx元"
                amount_match = re.search(r'(\d{1,3}(,\d{3})*(\.\d+)?)\s*(万?元)', line)
                if amount_match:
                    amount_str = amount_match.group(1).replace(',', '')
                    unit = amount_match.group(4)
                    try:
                        val = float(amount_str)
                        if '万' not in unit: # 如果单位是元，转换为万元
                            val = val / 10000
                        
                        results.append({
                            'year': year_match.group(),
                            'amount': val,
                            'page': page_num + 1,
                            'source': 'text'
                        })
                    except ValueError:
                        continue
        return results

    def _clean_result(self, raw_results):
        """
        清洗和去重
        """
        final_data = {}
        for item in raw_results:
            year = item['year']
            # 标准化年份
            if '年' in year:
                year = year.replace('年', '')
            
            # 如果已有该年份数据，保留金额较大的（通常是准确的，或者是合计数）
            # 或者保留来自表格的数据优先
            
            amount = 0
            if 'amount' in item:
                amount = item['amount']
            elif 'amount_candidates' in item:
                # 简单策略：取最大值
                amount = max(item['amount_candidates']) if item['amount_candidates'] else 0
            
            # 过滤异常值
            if amount < 10 or amount > 10000000: # 太小或太大都不对（单位万元）
                continue
                
            if year not in final_data:
                final_data[year] = {'year': year, 'amount': amount, 'page': item['page']}
            else:
                # Update logic
                if amount > final_data[year]['amount']:
                    final_data[year] = {'year': year, 'amount': amount, 'page': item['page']}
        
        return list(final_data.values())
