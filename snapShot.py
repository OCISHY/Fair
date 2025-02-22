import requests
import csv
import time
from typing import Set
from datetime import datetime
import concurrent.futures
from concurrent.futures import ThreadPoolExecutor

# BscScan API Keys
API_KEYS = [
    "",
    "",
    "",
    "",
    ""
]
# 查询地址（空投目标地址）
TARGET_ADDRESS = "0x28816c4C4792467390C90e5B426F198570E29307"

# 查询时间范围（UTC 时间 2025-02-19 00:00:00 ~ 2025-02-19 23:59:59）
START_TIMESTAMP = 1739923200  # UTC 2025-02-19 00:00:00
END_TIMESTAMP = 1740009599    # UTC 2025-02-19 23:59:59
START_BLOCK = 46780564
END_BLOCK = 46832250

def get_transactions(start_block: int, api_key: str) -> dict:
    url = "https://api.bscscan.com/api"
    params = {
        "module": "account",
        "action": "txlist",
        "address": TARGET_ADDRESS,
        "startblock": start_block,
        "endblock": start_block + 1,
        "page": 1,
        "offset": 10000,
        "sort": "asc",
        "apikey": api_key
    }
    
    try:
        response = requests.get(url, params=params)
        return response.json()
    except Exception as e:
        print(f"请求出错: {e}")
        return {"status": "0", "result": []}

def save_to_csv(addresses: Set[str], filename: str, mode: str = 'w'):
    with open(filename, mode, newline="") as f:
        writer = csv.writer(f)
        if mode == 'w':  # 只在新文件时写入标题
            writer.writerow(["Address"])
        for addr in addresses:
            writer.writerow([addr])
    print(f"已保存 {len(addresses)} 个地址至 {filename}")

def process_transactions(start_block: int, end_block: int, api_key: str):
    valid_addresses = set()
    temp_addresses = set()
    filename = f"airdrop_addresses_{start_block}_{end_block}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    
    current_block = start_block
    total_blocks = end_block - start_block
    
    print(f"开始处理区块 {start_block} 到 {end_block}")
    print(f"总共需要处理 {total_blocks} 个区块")
    
    while current_block <= end_block:
        try:
            progress = (current_block - start_block) / total_blocks * 100
            print(f"正在处理区块 {current_block}... (进度: {progress:.2f}%)")
            
            # 添加延迟以遵守API速率限制
            time.sleep(0.2)
            
            data = get_transactions(current_block, api_key)
            
            if data["status"] == "1" and data["result"]:
                for tx in data["result"]:
                    timestamp = int(tx["timeStamp"])
                    value = int(tx["value"]) / 10**18
                    sender = tx["from"]
                    
                    if START_TIMESTAMP <= timestamp <= END_TIMESTAMP and value == 0.0004:
                        temp_addresses.add(sender)
                        valid_addresses.add(sender)
            
            # 当临时集合达到5000个地址时，保存并清空
            if len(temp_addresses) >= 5000:
                save_to_csv(temp_addresses, filename, 'a' if current_block > start_block else 'w')
                temp_addresses.clear()
                print(f"当前总共找到 {len(valid_addresses)} 个合格地址")
            
            current_block += 1
            
        except Exception as e:
            print(f"处理区块 {current_block} 时出错: {e}")
            time.sleep(1)  # 出错时等待较长时间
            continue
    
    # 保存剩余的地址
    if temp_addresses:
        save_to_csv(temp_addresses, filename, 'a')
    
    return valid_addresses, filename

def main():
    try:
        total_blocks = END_BLOCK - START_BLOCK
        blocks_per_key = total_blocks // len(API_KEYS)
        
        # 创建线程池
        with ThreadPoolExecutor(max_workers=len(API_KEYS)) as executor:
            futures = []
            
            # 提交所有任务到线程池
            for i, api_key in enumerate(API_KEYS):
                start = START_BLOCK + (i * blocks_per_key)
                end = START_BLOCK + ((i + 1) * blocks_per_key) if i < len(API_KEYS) - 1 else END_BLOCK
                
                print(f"提交任务: API Key {api_key[:6]}... 处理区块范围: {start} - {end}")
                future = executor.submit(process_transactions, start, end, api_key)
                futures.append(future)
            
            # 等待所有任务完成并获取结果
            for future in concurrent.futures.as_completed(futures):
                try:
                    valid_addresses, filename = future.result()
                    print(f"✅ 完成处理: 找到 {len(valid_addresses)} 个合格地址")
                    print(f"✅ 地址已保存至 {filename}")
                except Exception as e:
                    print(f"❌ 任务执行出错: {e}")
                    print(f"错误详情: {str(e.__class__.__name__)}")
    
    except Exception as e:
        print(f"❌ 主程序发生错误: {e}")
        print(f"错误详情: {str(e.__class__.__name__)}")

if __name__ == "__main__":
    main()