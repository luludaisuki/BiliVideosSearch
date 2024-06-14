from .database import exportVideosToExcel,getDatabase
from .searchVideos import taskSearch,data_base_name,data_base_url,keywords,pattern,mid,w_rid,cookie_path
from .graphSearch import graph_search_sync,research_failed_sync
import argparse

def main():

    if not w_rid or not cookie_path:
        raise ValueError('w_rid or cookie_path is not set in config.yaml')

    parser = argparse.ArgumentParser(description='Search videos on bilibili')
    parser.add_argument('command', type=str, choices=['search','graph_search','graph_research','search_latest','export'],help='''

search: search related videos via offcial search engine, trying all search conditions
graph_search: search related videos via recommends and videos of same authors
graph_research: search failed videos(due to network error) in graph_search
search_latest: search related videos via offcial search engine, only search videos that have not been searched
export: export video information into xlsx format file
''')
    args = parser.parse_args()

    print('search for:',data_base_name)
    print('UID:',mid)
    print('keywords:',keywords)
    print('match pattern(for tags,titles and description):',pattern)
    
    cmd=args.command
    if cmd =='search':
        taskSearch(
            search_all=True
        )
    elif cmd=='graph_search':
        graph_search_sync()
    elif cmd=='graph_research':
        research_failed_sync()
    elif cmd=='search_latest':
        taskSearch(
            search_all=False
        )
    elif cmd=='export':
        db=getDatabase(data_base_url,data_base_name)
        exportVideosToExcel(db,f'./{data_base_name}_videos.xlsx')
    