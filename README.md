# BiliVideosSearch
Help finding all videos you want to search in Bilibili

## 安装
- Python 3.10.12 及以上
- Python库 requests, pyyaml, pymongo, asyncio, aiohttp, tqdm
- Mongodb 7.0.0 及以上

## 配置文件（config.yaml）
- 填入cookie文件路径和w_rid值。这个可能需要浏览器F12打开复制下来
- 搜索关键词和匹配模式，
  例子：
  ```
  up_name: lulu
  search_keywords:
    - 雫るる
    - 雫lulu
    - lulu
    - 雫露露
  
  up_uid: 387636363
  
  match_pattern: (雫るる)|(雫露露)|(雫lulu)
  ```

## 运行
运行main.py文件，命令行参数在代码内有介绍
