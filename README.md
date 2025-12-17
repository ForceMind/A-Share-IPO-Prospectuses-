# A股招股书分红数据自动化提取工具

本项目旨在自动化提取 2019-2023 年 A 股上市公司招股说明书中的“上市前三年”分红数据。

## 功能特性
*   **自动化爬虫**: 自动从巨潮资讯网下载招股说明书 PDF。
*   **智能解析**: 结合关键词定位、表格提取和正则匹配，从长达数百页的 PDF 中精确定位分红数据。
*   **数据清洗**: 自动标准化金额单位（统一为万元），处理跨页表格。
*   **结果汇总**: 输出 Excel 报表，包含公司代码、名称、年度、分红金额及来源页码。

## 环境要求
*   Python 3.8+
*   依赖库: `requests`, `pandas`, `pdfplumber`, `openpyxl`, `tqdm`

## 快速开始

### 1. 安装依赖
```bash
python -m pip install -r requirements.txt
```

### 2. 获取股票列表
首先获取 2019-2023 年上市的公司列表：
```bash
python src/get_stock_list.py
```
这将在 `data/` 目录下生成 `stock_list.csv`。

### 3. 运行全流程 (下载 + 解析)
```bash
python src/main.py --action all
```
程序会自动下载 PDF 到 `data/pdfs/`，并将解析结果保存至 `data/output/dividends_summary.xlsx`。

### 仅下载
```bash
python src/main.py --action download
```

### 仅解析 (需已有 PDF)
```bash
python src/main.py --action extract
```

### 运行测试 (使用测试列表)
```bash
python src/main.py --csv stock_list_test.csv
```

## 目录结构
```
project_root/
├── data/
│   ├── pdfs/          # 下载的 PDF 文件
│   ├── output/        # 结果 Excel
│   └── stock_list.csv # 股票列表
├── logs/              # 运行日志
├── src/
│   ├── config.py      # 配置项
│   ├── get_stock_list.py # 获取列表脚本
│   ├── downloader.py  # 爬虫模块
│   ├── extractor.py   # PDF 解析模块
│   └── main.py        # 主入口
└── requirements.txt
```

## 常见问题
*   **下载失败**: 请检查网络连接，或增加 `src/config.py` 中的重试次数。
*   **解析为空**: 招股书格式差异巨大，部分扫描件或非标准表格可能无法解析，建议查看 `dividends_summary.xlsx` 中的 `note` 列，并人工复核。
