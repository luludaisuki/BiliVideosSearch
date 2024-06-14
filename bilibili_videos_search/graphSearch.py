import time
from tqdm import tqdm
import random
import math
import requests
import asyncio
import aiohttp
import asyncio.tasks

import time
# from proxies import get_proxy
import urls
from .searchVideos import isUpVideo,readCookie,data_base_name,data_base_url,keywords,w_rid
from collections import deque
from typing import Callable,Awaitable,Any
import database
import asyncio.exceptions
import aiohttp.client_exceptions
import os

PROXY=os.environ.get('http_proxy',None)
if PROXY:
    print(f'use proxy {PROXY}')

headers = {'user-agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) \
    AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.77 Safari/537.36'}

AsyncFunc = Callable[[Any, Any], Awaitable[Any]]
max_request=3

class RequestStatus:
    def __init__(self,max_request=1) -> None:
        self.cnt=0
        self.sem=asyncio.Semaphore(max_request)
request_status=RequestStatus()

async def request_dom(url,headers=headers,cookies=None,max_try=0,params=None,json=True,status=request_status):
    '''async version'''
    '''请求url，返回dom文件'''
    
    while True:
        try:
            async with status.sem:
                # await asyncio.sleep(random.uniform(1.5,2.4))
                status.cnt+=1
                async with aiohttp.ClientSession(cookies=cookies,headers=headers) as session:
                    async with session.get(url,params=params,proxy=PROXY) as resp:
                    # async with session.get(url,params=params) as resp:
                        if resp.status == 200:
                            if json:
                                json_data=await resp.json()
                                if json_data['code']==0:
                                    return json_data
                            else:
                                return await resp.read()

                        print('-'*30,'request doc error:',resp.status)
                        # print(resp.headers)
                        print(await resp.json())
                        raise aiohttp.client_exceptions.ClientConnectionError()
                        # raise BaseException()

        except asyncio.exceptions.TimeoutError:
            if max_try>0:
                max_try-=1
                print(f'wait and try again...\nmax_try={max_try}')
                time.sleep(10)
            else:
                print('timeout')
                raise TimeoutError()

async def getTags(bvid):
    response=await request_dom(url=urls.get_tag,params={'bvid':bvid},json=True)
    data=response['data']
    tags=[tag['tag_name'] for tag in data]
    return ','.join(tags)

async def getTagsAndCheck(bvid,video_info,results:list,checkFunc):
    tag=await getTags(bvid)
    video_info['tag']=tag
    if checkFunc(video_info):
        results.append(video_info)

async def getAuthorVideosPageNum(mid,cookie=readCookie(),page_size=30,w_rid=w_rid,keyword=None):
    params={'mid':mid,'ps':page_size,'pn':1,'w_rid':w_rid}
    if keyword is not None:
        params['keyword']=keyword
    response=await request_dom(url=urls.get_author_video,cookies=cookie,params=params)

    if response['code']!=0:
        print(response)
        # raise RuntimeError()
        raise aiohttp.ClientConnectionError()

    data=response['data']
    
    count=data['page']['count']
    numPages=math.ceil(count/page_size)

    return numPages

async def getSeriesVideosPageNum(mid,series_id,cookie=readCookie(),page_size=30):
    params={'mid':mid,'series_id':series_id,'only_normal':'true','sort':'desc','pn':1,'ps':page_size}
    response=await request_dom(url=urls.get_series_archives,cookies=cookie,params=params)

    if response['code']!=0:
        print(response)
        # raise RuntimeError()
        raise aiohttp.ClientConnectionError()

    data=response['data']
    
    count=data['page']['total']
    numPages=math.ceil(count/page_size)

    return numPages

async def getSeriesVideos(mid,series_id,page=1,cookie=readCookie(),page_size=30):
    params={'mid':mid,'series_id':series_id,'only_normal':'true','sort':'desc','pn':page,'ps':page_size}
    response=await request_dom(url=urls.get_series_archives,cookies=cookie,params=params)

    if response['code']!=0:
        print(response)
        raise aiohttp.ClientConnectionError()
        # raise RuntimeError()

    data=response['data']

    videos=data['archives']

    return videos

async def getAuthorVideos(mid,page=1,cookie=readCookie(),page_size=30,w_rid=w_rid,keyword=None):
    params={'mid':mid,'ps':page_size,'pn':page,'w_rid':w_rid}
    if keyword is not None:
        params['keyword']=keyword
    response=await request_dom(url=urls.get_author_video,cookies=cookie,params=params)

    if response['code']!=0:
        print(response)
        raise aiohttp.ClientConnectionError()
        # raise RuntimeError()

    data=response['data']

    videos=data['list']['vlist']

    return videos

async def getRelatedVideos(bvid):
    params={'bvid':bvid}
    response=await request_dom(url=urls.get_related_video,params=params)

    data=response.get('data',None)
    if data is None:
        print(response)
        return None
    return data

async def getVideo(bvid):
    params={'bvid':bvid}
    response=await request_dom(url=urls.get_video,params=params)

    data=response.get('data',None)
    if data is None:
        print(response)
        return None
    return data

async def fetchAndProcessAuthorVideos(mid,procesFunc,keyword=None):
    numPage = await getAuthorVideosPageNum(mid=mid,keyword=keyword)
    print(f'search on author uid={mid} with approximately {numPage*30} videos')
    
    async def processPage(page):
        # print(f'start page {page}')
        videos=await getAuthorVideos(mid=mid,page=page,keyword=keyword)
        tasks=[procesFunc(video) for video in videos]
        await asyncio.gather(*tasks)
        # print(f'page {page} done')
        
    tasks=[processPage(page) for page in range(1,numPage+1)]
    await asyncio.gather(*tasks)

async def fetchAndProcessSeriesVideos(mid,series_id,procesFunc):
    numPage = await getSeriesVideosPageNum(mid=mid,series_id=series_id)
    print(f'search on author uid={mid} with approximately {numPage*30} videos')
    
    async def processPage(page):
        # print(f'start page {page}')
        videos=await getSeriesVideos(mid=mid,series_id=series_id,page=page)
        tasks=[procesFunc(video) for video in videos]
        await asyncio.gather(*tasks)
        # print(f'page {page} done')
        
    tasks=[processPage(page) for page in range(1,numPage+1)]
    await asyncio.gather(*tasks)

async def fetchAndProcessRelatedVideos(bvid,procesFunc):
    videos=await getRelatedVideos(bvid)
    print(f'search related videos of video {bvid}')
    for video in videos:
        video['mid']=video['owner']['mid']
        video['description']=video['desc']

    tasks=[procesFunc(video) for video in videos]
    await asyncio.gather(*tasks)

async def processVideo_(db,recorded:set,video,refineFunc:AsyncFunc,matchFunc=isUpVideo):
    bvid=video['bvid']
    if bvid in recorded:
        return
    
    if database.checkUnrelatedVideo(bvid,db):
        return
    
    # time.sleep(getSleepTime())
    print(f"find video {bvid}:{video['title']}")

    try:
        if not video.get('tag',None):
            video['tag']=await getTags(bvid)
        if not matchFunc(video):
            database.addUnrelatedVideo(bvid,db)
            return
            
        video = await refineFunc(video)
        recorded.add(bvid)
        database.saveVideoAndAuthor(video,db)

    except TimeoutError:
        print(f"failed to get video {bvid}:{video['title']}")
        database.addFailedVideo(bvid,db)

async def fetchAndProcessFailedVideos(procesFunc,db,check_exist=True):
    async def ProcessFunc_(video):
        bvid=video['bvid']
        video=await refineVideos(video)
        await procesFunc(video)
        database.removeFailedVideo(bvid,db)
        
    tasks=[]
    for video in database.getFailedVideos(db):
        if check_exist and database.checkInVideos(video['bvid'],db):
            database.removeFailedVideo(video['bvid'],db)
            continue
        tasks.append(asyncio.create_task(ProcessFunc_(video)))
        await asyncio.sleep(random.uniform(0.2,0.5))
        
    print(f'to find {len(tasks)} failed videos')

    await asyncio.gather(*tasks)

    
async def refineVideosFromAuthor(video,add_tag=False):
    bvid=video['bvid']
    if add_tag:
        tag=await getTags(bvid)
        video['tag']=tag

    video_detailed=await getVideo(bvid)
    video['id']=video['aid']
    video['typename']=video_detailed['tname']
    video['senddate']=video['created']
    video['pubdate']=video_detailed['pubdate']
    video['duration']=video['created']
    
    return video

async def refineVideos(video,add_tag=False):
    bvid=video['bvid']
    if add_tag:
        tag=await getTags(bvid)
        video['tag']=tag

    video=await getVideo(bvid)
    video['mid']=video['owner']['mid']
    video['author']=video['owner']['name']
    video['typeid']=video['tid']
    video['typename']=video['tname']
    video['description']=video['desc']
    video['id']=video['aid']
    video['senddate']=video['ctime']
    
    return video

async def refineRelatedVideos(video,add_tag=False):
    if add_tag:
        bvid=video['bvid']
        video['tag']=await getTags(bvid)
    video['mid']=video['owner']['mid']
    video['author']=video['owner']['name']
    video['typeid']=video['tid']
    video['typename']=video['tname']
    video['description']=video['desc']
    video['id']=video['aid']
    video['senddate']=video['ctime']
    
    return video

async def graphSearchVideos():
    db=database.getDatabase(url=data_base_url,name=data_base_name)
    last_time=database.findLastGraphSearchTime(db)
    que=deque()
    enqueued=set()
    author_visited=set()
    
    videos=list(database.findAllVideos(db))
    random.shuffle(videos)
    
    for video in videos:
        if video['last_access_time']<last_time:
            que.append(video)
        enqueued.add(video['bvid'])

    # for video in database.findAllVideos(db):
    #     if video['last_access_time']<last_time:
    #         que.append(video)
    #     enqueued.add(video['bvid'])
        
    for author in database.findAllAuthors(db):
        if author['last_access_time']>=last_time:
            author_visited.add(author['mid'])
        
    INIT_TIME_TO_SLEEP=1
    FAILED_SLEEP_TIME=450 # 400
    def getSleepTime():
        return random.uniform(0.11,0.24)

    async def processVideo(video,refineFunc:AsyncFunc,matchFunc=isUpVideo):
        bvid=video['bvid']
        if bvid in enqueued:
            return
        
        if database.checkUnrelatedVideo(bvid,db):
            return
        
        # time.sleep(getSleepTime())
        print(f"find video {bvid}:{video['title']}")

        try:
            if not video.get('tag',None):
                video['tag']=await getTags(bvid)
            if not matchFunc(video):
                database.addUnrelatedVideo(bvid,db)
                return
                
            if refineFunc is not None:
                video = await refineFunc(video)
            enqueued.add(bvid)
            que.append(video)
            database.saveVideoAndAuthor(video,db)

        except TimeoutError:
            print(f"failed to get video {bvid}:{video['title']}")
            database.addFailedVideo(bvid,db)
        
    async def processVideoFromAuthor(video,matchFunc=isUpVideo):
        await processVideo(video,refineVideosFromAuthor,matchFunc)

    async def processRelatedVideo(video,matchFunc=isUpVideo):
        await processVideo(video,refineRelatedVideos,matchFunc)
        
    loop_start=time.time()
    loop_cnt_start=request_status.cnt

    while len(que)>0:
        video=que.popleft()
        tasks=[]
        mid=video['mid']
        bvid=video['bvid']
        print(f"Search on node {bvid}:{video['title']}, {len(que)} nodes in the queue")
        new_author=False
        
        try:
            start=time.time()
            cnt_start=request_status.cnt
            if not mid in author_visited:
                new_author=True
                author_visited.add(mid)
                for keyword in keywords:
                    tasks.append(fetchAndProcessAuthorVideos(mid,processVideoFromAuthor,keyword=keyword))
                
            tasks.append(fetchAndProcessRelatedVideos(bvid,processRelatedVideo))
                
            await asyncio.gather(*tasks)
            database.touchVideo(bvid,db)
            if new_author:
                database.touchAuthor(mid,db)
                
            end=time.time()
            cnt_end=request_status.cnt
            print(f"Handle {cnt_end-cnt_start} requests of node {bvid}:{video['title']} in {end-start} secs")

        except (TimeoutError,RuntimeError,aiohttp.client_exceptions.ClientConnectionError,
                aiohttp.client_exceptions.ClientError,asyncio.exceptions.TimeoutError) as e:
            print(e)

            loop_end=time.time()
            loop_cnt_end=request_status.cnt
            print(f"Handle {loop_cnt_end-loop_cnt_start} requests in {loop_end-loop_start} secs")
            
            sleep_time=FAILED_SLEEP_TIME

            print(f'will restart in {sleep_time} secs...')
            if new_author:
                author_visited.discard(mid)
            que.append(video)

            time.sleep(sleep_time)

            print('restart ...')
            loop_start=time.time()
            loop_cnt_start=request_status.cnt

    loop_end=time.time()
    loop_cnt_end=request_status.cnt
    print(f"Handle {loop_cnt_end-loop_cnt_start} requests in {loop_end-loop_start} secs")
    
async def refindFailedVideos():
    db=database.getDatabase(url=data_base_url,name=data_base_name)
    recorded=set()
    
    async def f(video):
        await processVideo_(db,recorded,video,None,isUpVideo)
    
    await fetchAndProcessFailedVideos(procesFunc=f,db=db,check_exist=False)

async def getAuthorVideosList(mid,series_id=None):
    result=[]
        
    async def processVideoFromAuthor(video):
        bvid=video['bvid']
        if str(video['mid'])==str(mid):
            result.append(bvid)

    async def processVideoFromSeries(video):
        bvid=video['bvid']
        # print(bvid)
        result.append(bvid)

    await fetchAndProcessAuthorVideos(mid,procesFunc=processVideoFromAuthor)

    if series_id is not None:
        await fetchAndProcessSeriesVideos(mid=mid,series_id=series_id,procesFunc=processVideoFromSeries)

    return result
        
async def main():
    await graphSearchVideos()
    # await refindFailedVideos()
def graph_search_sync():
    asyncio.run(graphSearchVideos())

def research_failed_sync():
    asyncio.run(refindFailedVideos())
    # await refindFailedVideos()
if __name__ == '__main__':
    start=time.time()

    asyncio.run(main())

    end=time.time()
    print(f'it takes {end-start} seconds')

