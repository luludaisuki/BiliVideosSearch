import time
import math

from requesters import request_dom
from tqdm import tqdm

import pymongo
import re
from .config import loadConfig

config=loadConfig('config.yaml')

mid=config['mid']
pattern=re.compile(config['pattern'])
keywords=config['keywords']
cookie_path=config['cookie_path']
w_rid=config['w_rid']
data_base_name=config['up_name']
data_base_url=config['mongo_url']

def readCookie(cache=[]):
    if len(cache)>0:
        return cache[0]
    with open(cookie_path,'r')as f:
        cookies=f.read().strip()
        # print(cookies)
    cookie = {i.split("=")[0].strip():i.split("=")[1].strip() for i in cookies.split(";")} 
    cache.append(cookie)
    return cookie
    
def isUpVideo(video_info:dict,mid=mid,pattern=pattern)->bool:
    #主播本人
    if video_info['mid']==mid:
        return True

    title,tag,description=video_info['title'],video_info.get('tag',''),video_info['description']
    
    for info in title,tag,description:
        info=info.lower()
        if re.search(pattern=pattern,string=info) is not None:
            return True
        
    return False
               
def getDatabase(url=data_base_url,name=data_base_name,**kwargs):
    db = pymongo.MongoClient(url)[name]
    return db
    
def save(result:dict,database,no_cache=True,video_keys=['id','mid','typeid','typename','aid','bvid','title','description','pic','tag','pubdate','senddate','duration']):
    if not no_cache:
        raise NotImplementedError()

    mid=result["mid"]
    query={"mid":mid}
    query_result=database.Authors.find_one(query,{'_id':1})
    if query_result is None:
        author={'mid':mid,'name':result['author'],'last_access_time':0}
        database.Authors.insert_one(author)
    
    bvid=result['bvid']
    query={"bvid":bvid}
    query_result=database.Videos.find_one(query,{'_id':1})
    if query_result is None:
        video={key:result[key] for key in video_keys}
        video['last_access_time']=0
        database.Videos.insert_one(video)
    
def searchOnce(database,url,matchFunc,no_cache=True,max_try=3,cookie=readCookie()):
    '''
        return number of pages
        return None if error
    '''
    response = request_dom(url=url,cookies=cookie)
    max_try-=1

    while response is None and max_try>0:
        time.sleep(10)
        response = request_dom(url=url,cookies=cookie)
        max_try-=1
        

    if response!=None:
        # cookie= response.cookies.get_dict()
        # print(cookie)
        response=response.json()

        if response:
            data=response['data']
            videos=data.get('result',None)
            if videos is None:
                return 0
            numPages=data['numPages']

            videos=[video for video in videos if matchFunc(video)]
            for video in videos:
                save(video,database=database,no_cache=no_cache)
            
            return numPages
        else:
            print('-'*30,'404')
            return None
    else:
        print('None response')
        return None
    
def getSearchURL(keyword:str,order='totalrank',tids=0,duration=0,page=1):
    return f'https://api.bilibili.com/x/web-interface/search/type?keyword={keyword}&order={order}&tids={tids}&duration={duration}&page={page}&search_type=video'

def search(keyword:str,db,order='totalrank',tids=0,duration=0,no_cache=True,matchFunc=isUpVideo):
    '''
        no_cache: every time get response, insert to database
        return: True if all videos satisfied is found
    '''
    def getSearchURLInner(page=1):
        return getSearchURL(keyword=keyword,order=order,tids=tids,duration=duration,page=page)
    
    url=getSearchURLInner()
    numPages=searchOnce(db,url,matchFunc=matchFunc,no_cache=no_cache)
    
    if numPages is not None:
        for i in tqdm(range(2,numPages+1)):
            url=getSearchURLInner(page=i)
            # time.sleep(10)
            if searchOnce(db,url,matchFunc=matchFunc,no_cache=no_cache) is None:
                print(f'error occured when searching when searching page {i}')
                return None
    else:
        print('error occured when searching page 1')
        return None
        
    if numPages<50:
        return True
    return False

def getTags(bvid):
    url=f'https://api.bilibili.com/x/tag/archive/tags?bvid={bvid}'
    response=request_dom(url=url).json()
    data=response['data']
    tags=[tag['tag_name'] for tag in data]
    return ','.join(tags)

def getAuthorVideos(mid,page=1,matchFunc=isUpVideo,cookie=readCookie(),page_size=30,w_rid=w_rid):
    url=f'https://api.bilibili.com/x/space/wbi/arc/search?mid={mid}&ps={page_size}&pn={page}&w_rid={w_rid}'

    response=request_dom(url,cookies=cookie)

    if response!=None:
        response=response.json()

        if response:
            data=response.get('data',None)
            if data is None:
                print(response)
                return None
            videos=data['list']['vlist']
            
            # add tag info
            for video in videos:
                bvid=video['bvid']
                video['tag']=getTags(bvid)

            count=data['page']['count']
            numPages=math.ceil(count/page_size)

            videos=[video for video in videos if matchFunc(video)]
            
            return numPages,videos
        else:
            print('-'*30,'404')
            return None
    else:
        print('None response')
        return None

def getRelatedVideos(bvid,matchFunc=isUpVideo):
    url=f'https://api.bilibili.com/x/web-interface/archive/related?bvid={bvid}'

    response=request_dom(url)

    if response!=None:
        response=response.json()

        if response:
            data=response.get('data',None)
            if data is None:
                print(response)
                return None
            videos=data
            
            # add tag info
            for video in tqdm(videos):
                bvid=video['bvid']
                video['tag']=getTags(bvid)
                video['mid']=video['owner']['mid']
                video['description']=video['desc']


            videos=[video for video in videos if matchFunc(video)]
            
            return videos
        else:
            print('-'*30,'404')
            return None
    else:
        print('None response')
        return None

        
########## TASK ############
def taskSearch(keywords,search_all):
    db=getDatabase()
    
    if search_all:
        tids=[0,1,160,3,129,211,4,5,181,217,119,36,188,223,155]
        durations=[0,1,2,3,4]
        order=['totalrank','click','pubdate','dm','stow','scores']
    else:
        # only search newst videos
        tids=[0]
        durations=[0]
        order=['pubdate']
    
    for typeId in tids:
        for keyword in keywords:
            for orderId in order:
                finish=False
                for duration in durations:
                    print(f'search with tid={typeId},keyword={keyword},order={orderId},duration={duration}...')
                    result=search(keyword=keyword,db=db,order=orderId,tids=typeId,duration=duration)
                    if result is None:
                        print('terminate due to error')
                        return
                    if (duration==0) and result is True:
                        print('find all videos in current tid and keyword.')
                        finish = True 
                        break
                if finish:
                    break
                
if __name__=='__main__':
    taskSearch(search_all=True,keywords=keywords)
