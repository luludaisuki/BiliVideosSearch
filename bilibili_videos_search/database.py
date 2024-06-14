import pymongo
from tqdm import tqdm
import time
import datetime
import pandas as pd
from pymongo.database import Database

'''
    bilibili videos database:

    collections
        Authors: {mid,name,last_access_time} key:mid, last_access_time=int(time.time())
        
        Videos: {id,mid,typeid,typename,aid,bvid,title,description,pic,
            tag,pubdate,sendate,duration,last_access_time} key:bvid

        GraphSearchLogs: {time}

        UnrelatedVideos: {bvid} key:bvid
        FailedVideos: {bvid} key:bvid
'''

def datetimeFromBiliTime(bili_time:float):
    past=datetime.datetime(2023,9,5,8,0,0,0)
    past_bili_time=1693872000
    past_timestamp=past.timestamp()
    
    new_timestamp=bili_time-past_bili_time+past_timestamp
    
    return datetime.datetime.fromtimestamp(new_timestamp)


def time_to_seconds(time_str):
    time_parts = time_str.split(':')
    total_seconds = 0
    
    if len(time_parts) == 2:
        total_seconds = int(time_parts[0]) * 60 + int(time_parts[1])
    elif len(time_parts) == 3:
        total_seconds = int(time_parts[0]) * 3600 + int(time_parts[1]) * 60 + int(time_parts[2])
    
    return total_seconds

def format_time(seconds):
    # 计算小时，分钟和秒数
    hours = seconds // 3600
    minutes = (seconds % 3600) // 60
    seconds = seconds % 60
    # 根据时长的不同，选择合适的输出格式
    if hours > 0:
        return f"{hours:02d}:{minutes:02d}:{seconds:02d}"
    else:
        return f"{minutes:02d}:{seconds:02d}"
    # elif minutes > 0:
    #     return f"{minutes:02d}:{seconds:02d}"
    # else:
    #     return f"{seconds:02d}"

def now():
    return int(time.time())

def saveVideoAndAuthor(
        result:dict,
        database,
        no_cache=True,
        record_time=False,
        video_keys=['id','mid','typeid','typename','aid','bvid','title','description','pic','tag','pubdate','senddate','duration']):

    if not no_cache:
        raise NotImplementedError()
    
    if record_time:
        last_access_time=now()
    else:
        last_access_time=0

    mid=result["mid"]
    query={"mid":mid}
    query_result=database.Authors.find_one(query,{'_id':1})
    if query_result is None:
        author={'mid':mid,'name':result['author'],'last_access_time':last_access_time}
        database.Authors.insert_one(author)
    
    bvid=result['bvid']
    query={"bvid":bvid}
    query_result=database.Videos.find_one(query,{'_id':1})
    if query_result is None:
        video={key:result[key] for key in video_keys}
        video['last_access_time']=last_access_time
        database.Videos.insert_one(video)
        
def exportVideosToExcel(db:Database,save_path,query=None):
    video_keys=['bvid','title','author','mid','typename','description','tag','pubdate','pub_timestamp','duration','duration(s)']
    video_keys_cn=['BV号','标题','作者','UID','分区','描述','tag','发布时间','发布时间戳','时长','时长（秒）']
    data={key:[] for key in video_keys}
    data_cn={}
    
    if query is not None:
        results=db.Videos.find(query)
    else:
        results=db.Videos.find()

    for video in tqdm(results):
        video['author']=getAuthorName(video['mid'],database=db)
        video['pub_timestamp']=video['pubdate']
        video['pubdate']=str(datetimeFromBiliTime(video['pubdate']))
        title=str.replace(video['title'],'<em class="keyword">','')
        video['title']=str.replace(title,'</em>','')
        duration=video['duration']
        
        if type(duration)==int:
            video['duration(s)']=duration
            video['duration']=format_time(duration)
        else:
            video['duration(s)']=time_to_seconds(duration)


        for key in video_keys:
            data[key].append(video[key])
            
        
    for key,key_cn in zip(video_keys,video_keys_cn):
        data_cn[key_cn]=data[key] 
        
    df=pd.DataFrame(data_cn)
    df.to_excel(save_path, index=False)
        
        
def checkUnrelatedVideo(
        bvid,
        database,
        ):

    query={"bvid":bvid}
    query_result=database.UnrelatedVideos.find_one(query,{'_id':1})
    return query_result is not None

def checkInVideos(
        bvid,
        database,
        ):

    query={"bvid":bvid}
    query_result=database.Videos.find_one(query,{'_id':1})
    return query_result is not None

def getVideo(
        bvid,
        database,
        ):

    query={"bvid":bvid}
    query_result=database.Videos.find_one(query)
    return query_result

def addUnrelatedVideo(
        bvid,
        database,
        check=False,
        ):

    query={"bvid":bvid}
    if check:
        query_result=database.UnrelatedVideos.find_one(query,{'_id':1})
        if query_result is None:
            database.UnrelatedVideos.insert_one(query)
    else:
        database.UnrelatedVideos.insert_one(query)
        
def addFailedVideo(
        bvid,
        database,
        ):

    query={"bvid":bvid}
    query_result=database.FailedVideos.find_one(query,{'_id':1})
    if query_result is None:
        database.FailedVideos.insert_one(query)

def getFailedVideos(
        database,
        ):

    query_result=database.FailedVideos.find()
    return query_result

def getAuthorVideos(
        mid,
        database:Database,
        ):
    query={'mid':mid}
    query_result=database.Videos.find(query)
    return query_result

def removeFailedVideo(
        bvid,
        database,
        ):

    query={"bvid":bvid}
    database.FailedVideos.delete_one(query)


def touchVideo(bvid,database:Database):
    query={"bvid":bvid}
    database.Videos.update_one(query,{'$set':{'last_access_time':now()}})

def touchAuthor(mid,database:Database):
    query={"mid":mid}
    database.Authors.update_one(query,{'$set':{'last_access_time':now()}})

def getAuthorName(mid,database:Database):
    query={"mid":mid}
    return database.Authors.find_one(query,{'name':1})['name']
        
def updateGraphSearchTime(database):
    database.GraphSearchLogs.insert_one({'time':now()})

def findLastGraphSearchTime(database:Database):
    # database.GraphSearchLogs.insert_one({'time':now()})
    return next(database.GraphSearchLogs.aggregate([{'$group':{'_id':'','time':{'$max':'$time'}}}]))['time']

def findVideosAccessedEarlyThan(db:Database,time:int):
    return db.Videos.find({'last_access_time':{'lt':time}})

def findUnvisitedVideos(db:Database):
    last_time=findLastGraphSearchTime(db)
    return findVideosAccessedEarlyThan(db,last_time)

def findAllVideos(db:Database):
    return db.Videos.find()

def findAllAuthors(db:Database):
    return db.Authors.find()

def getDatabase(url='mongodb://localhost:27017/',name='up_videos',**kwargs):
    db = pymongo.MongoClient(url)[name]
    initDatabase(db)
    return db

def initDatabase(db:Database):
    # updateGraphSearchTime(db)    
    try:
        findLastGraphSearchTime(db)
    except StopIteration:
        updateGraphSearchTime(db)

    authors=db.Authors
    videos=db.Videos

    failed_videos=db.FailedVideos
    unrelated_videos=db.UnrelatedVideos

    
    authors.create_index({'mid':1})
    videos.create_index({'bvid':1})
    failed_videos.create_index({'bvid':1})
    unrelated_videos.create_index({'bvid':1})
