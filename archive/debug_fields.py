import requests
import json

EASTMONEY_LIST_URL = 'https://push2.eastmoney.com/api/qt/clist/get'

def test_fields():
    # Trying likely fields for industry: f100, f102, f103, f127, f128, f139
    # Also keeping f12 (code), f14 (name) for identification
    fields_to_test = 'f12,f14,f100,f102,f103,f127,f128,f139'
    
    params = {
        'pn': 1,
        'pz': 5,
        'po': 1,
        'np': 1,
        'ut': 'bd1d9ddb04089700cf9c27f6f7426281',
        'fltt': 2,
        'invt': 2,
        'fid': 'f26',
        'fs': 'm:0+t:6,m:0+t:80,m:1+t:2,m:1+t:23,m:0+t:81+s:2048',
        'fields': fields_to_test,
        '_': 1623833739532
    }

    try:
        response = requests.get(EASTMONEY_LIST_URL, params=params, timeout=10)
        data = response.json()
        if data.get('data') and 'diff' in data['data']:
            print(json.dumps(data['data']['diff'], indent=2, ensure_ascii=False))
        else:
            print("No data found")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == '__main__':
    test_fields()
