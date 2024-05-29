from googleapiclient.discovery import build
import pymongo
import pandas as pd
import streamlit as st
import psycopg2


def Api_connect():
    Api_Id           ="AIzaSyDcYtvS759fmBDDKRlftsCQWUDOtlJlR30"
    api_service_name = "youtube"
    api_version      = "v3"
    youtube = build(api_service_name, api_version, developerKey=Api_Id)
    return youtube

youtube=Api_connect()

def get_channel_info(channel_id):
    request=youtube.channels().list(
                      part="snippet,ContentDetails,statistics",
                      id  =channel_id
    )
    response = request.execute()

    for i in response['items']:
        data=dict(Channel_Name       = i["snippet"]["title"],
                  Channel_Id         = i["id"],
                  Subscribers        = i['statistics']['subscriberCount'],
                  Views              = i["statistics"]["viewCount"],
                  Total_Videos       = i['statistics']['videoCount'],
                  Channel_Description= i['snippet']['description'],
                  Playlists_Id       = i['contentDetails']['relatedPlaylists']['uploads'])
    return data


def get_videos_ids(channel_id):
    video_ids       =[]
    response        = youtube.channels().list(id=channel_id,part='contentDetails').execute()
    Playlist_Id     =response['items'][0]['contentDetails']['relatedPlaylists']['uploads']     
    next_page_token =None

    while True:
        response1=youtube.playlistItems().list(part='snippet',playlistId=Playlist_Id,maxResults=50,pageToken=next_page_token).execute()
        for i in range(len(response1['items'])):
            video_ids.append(response1['items'][i]['snippet']['resourceId']['videoId'])
        next_page_token=response1.get('nextPageToken')
        
        if next_page_token is None:
            break
    return video_ids


def get_video_info(video_ids):
    video_data=[]
    for video_id in video_ids:
            request =youtube.videos().list(part="snippet,ContentDetails,statistics",id=video_id)
            response = request.execute()

            for item in response["items"]:
                data=dict(Channel_Name= item['snippet']['channelTitle'],
                          Channel_Id        = item['snippet']['channelId'],
                          Video_Id          = item['id'],
                          Tags              = item['snippet'].get('tags'),
                          Title             = item['snippet']['title'],
                          Likes             = item['statistics'].get('likeCount'),
                          Comments          = item['statistics'].get('commentCount'),
                          viewCount         = item['statistics']['viewCount'])

                video_data.append(data)
    return video_data  


def get_comment_info(video_ids):
    Comment_data=[]
    try:
        for video_id  in video_ids:
            request   = youtube.commentThreads().list(part="snippet",videoId=video_id,maxResults=10)
            response  = request.execute()
            
            for item in response['items']:
                data=dict(Comment_Id        =item['snippet']['topLevelComment']['id'],
                          Video_Id          =item['snippet']['topLevelComment']['snippet']['videoId'],
                          Comment_Author    =item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                          Comment_Text      =item['snippet']['topLevelComment']['snippet']['textDisplay'],
                          Comment_Published =item['snippet']['topLevelComment']['snippet']['publishedAt'])
                    
                Comment_data.append(data)
    except:
        pass
    return Comment_data


def get_playlist_details(channel_id):
    next_page_token=None
    All_data=[]
    while True:
        request =youtube.playlists().list(part='snippet,contentDetails',channelId=channel_id,maxResults=50,pageToken=next_page_token)
        response=request.execute()

        for item in response['items']:
            data=dict(Playlist_Id=item['id'],
                        Title=item['snippet']['title'],
                        Channel_Id=item['snippet']['channelId'],
                        Channel_Name=item['snippet']['channelTitle'],
                        Video_Count=item['contentDetails']['itemCount'])
            All_data.append(data)

        next_page_token=response.get('nextPageToken')
        if next_page_token is None:
            break  
    return All_data      


client=pymongo.MongoClient("mongodb+srv://santhoshkumar349:podafool@cluster0.ausoaqz.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0")
db=client["Youtube_Data"]


def channel_details(channel_id):
    ch_details  =get_channel_info(channel_id)
    pl_details  =get_playlist_details(channel_id)
    vi_ids      =get_videos_ids(channel_id)
    vi_details  =get_video_info(vi_ids)
    com_details =get_comment_info(vi_ids)

    coll1=db["channel_details"]
    coll1.insert_one({"channel_information":ch_details,"playlist_information":pl_details,"video_information":vi_details,"comment_information":com_details})

    return "upload completed successfully"

def channels_table():
    mydb=psycopg2.connect(host="localhost",user="postgres",password="Podafool348",database="youtube_data",port="5432")
    cursor=mydb.cursor()

    drop_query='''drop table if exists channels'''
    cursor.execute(drop_query)
    mydb.commit()

    
    create_query="create table if not exists channels(Channel_Name varchar(100),Channel_Id varchar(100) primary key,Subscribers bigint,Views bigint,Total_Videos int,Channel_Description text,Playlists_Id varchar(80))"
    cursor.execute(create_query)
    mydb.commit()

    ch_list=[]
    db=client["Youtube_Data"]
    coll1=db["channel_details"]
    for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
        ch_list.append(ch_data["channel_information"])

    df=pd.DataFrame(ch_list)

    for index,row in df.iterrows():
        insert_query='''insert into channels(Channel_Name ,
                                         Channel_Id,  
                                         Subscribers,
                                         Views,
                                         Total_Videos,
                                         Channel_Description,
                                         Playlists_Id)
                                         
                                         values(%s,%s,%s,%s,%s,%s,%s)'''

    values=(row['Channel_Name'],row['Channel_Id'],row['Subscribers'],row['Views'],row['Total_Videos'],row['Channel_Description'],row['Playlists_Id'])

   
    cursor.execute(insert_query,values)
    mydb.commit()   

def playlist_table():   
    mydb=psycopg2.connect(host="localhost",user="postgres",password="Podafool348",database="youtube_data",port="5432")
    cursor=mydb.cursor()

    drop_query='''drop table if exists playlists'''
    cursor.execute(drop_query)
    mydb.commit()

    create_query="create table if not exists playlists(Playlist_Id varchar(100) primary key,Title varchar(100) ,Channel_Id varchar(100),Channel_Name varchar(100),Video_Count int)"
    cursor.execute(create_query)
    mydb.commit()

    pl_list=[]
    db=client["Youtube_Data"]
    coll1=db["channel_details"]
    for pl_data in coll1.find({},{"_id":0,"playlist_information":1}):
        for i in range(len(pl_data["playlist_information"])):
            pl_list.append(pl_data["playlist_information"][i])

    df1=pd.DataFrame(pl_list)
    for index,row in df1.iterrows():
        insert_query='''insert into playlists(Playlist_Id ,
                                            Title,  
                                            Channel_Id,
                                            Channel_Name,
                                            Video_Count)
                                            
                                            values(%s,%s,%s,%s,%s)'''

        values=(row['Playlist_Id'],row['Title'],row['Channel_Id'],row['Channel_Name'],row['Video_Count'])

        cursor.execute(insert_query,values)
        mydb.commit() 


def videos_table():
    mydb=psycopg2.connect(host="localhost",user="postgres",password="Podafool348",database="youtube_data",port="5432")
    cursor=mydb.cursor()

    drop_query='''drop table if exists videos'''
    cursor.execute(drop_query)
    mydb.commit()

    create_query="create table if not exists videos(Channel_Name varchar(100),Channel_Id varchar(100) ,Video_Id varchar(100) primary key,Tags text,Title varchar(255),Likes bigint,Comments int,viewCount bigint)"
    cursor.execute(create_query)
    mydb.commit()

    vi_list=[]
    db=client["Youtube_Data"]
    coll1=db["channel_details"]
    for vi_data in coll1.find({},{"_id":0,"video_information":1}):
        for i in range(len(vi_data["video_information"])):
            vi_list.append(vi_data["video_information"][i])

    df2=pd.DataFrame(vi_list)
    for index,row in df2.iterrows():
        insert_query='''insert into videos(Channel_Name ,
                                            Channel_Id,  
                                            Video_Id,
                                            Tags,
                                            Title,
                                            Likes,
                                            Comments,
                                            viewCount)
                                                
                                            values(%s,%s,%s,%s,%s,%s,%s,%s)'''

        values=(row['Channel_Name'],row['Channel_Id'],row['Video_Id'],row['Tags'],row['Title'],row['Likes'],row['Comments'],row['viewCount'])

        cursor.execute(insert_query,values)
        mydb.commit()   

def comments_table():
        mydb=psycopg2.connect(host="localhost",user="postgres",password="Podafool348",database="youtube_data",port="5432")
        cursor=mydb.cursor()

        drop_query='''drop table if exists comments'''
        cursor.execute(drop_query)
        mydb.commit()

        create_query="create table if not exists comments(Comment_Id varchar(100) primary key,Video_Id varchar(100),Comment_Author varchar(150),Comment_Text text,Comment_Published timestamp)"
        cursor.execute(create_query)
        mydb.commit()

        com_list=[]
        db=client["Youtube_Data"]
        coll1=db["channel_details"]
        for com_data in coll1.find({},{"_id":0,"comment_information":1}):
                for i in range(len(com_data["comment_information"])):
                        com_list.append(com_data["comment_information"][i])

        df3=pd.DataFrame(com_list)

        for index,row in df3.iterrows():
                insert_query='''insert into comments(Comment_Id ,
                                                        Video_Id,  
                                                        Comment_Author,
                                                        Comment_Text,
                                                        Comment_Published)
                                                        
                                                        values(%s,%s,%s,%s,%s)'''

                values=(row['Comment_Id'],row['Video_Id'],row['Comment_Author'],row['Comment_Text'],row['Comment_Published'])

                cursor.execute(insert_query,values)
                mydb.commit()

def tables():
    channels_table()
    playlist_table()
    videos_table()
    comments_table()

    return "Tables Created Successfully"

def show_channels_table():
    ch_list=[]
    db=client["Youtube_Data"]
    coll1=db["channel_details"]
    for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
        ch_list.append(ch_data["channel_information"])

    df=st.dataframe(ch_list)

    return df

def show_playlists_table():
    pl_list=[]
    db=client["Youtube_Data"]
    coll1=db["channel_details"]
    for pl_data in coll1.find({},{"_id":0,"playlist_information":1}):
        for i in range(len(pl_data["playlist_information"])):
            pl_list.append(pl_data["playlist_information"][i])

    df1=st.dataframe(pl_list)

    return df1

def show_videos_table():
    vi_list=[]
    db=client["Youtube_Data"]
    coll1=db["channel_details"]
    for vi_data in coll1.find({},{"_id":0,"video_information":1}):
        for i in range(len(vi_data["video_information"])):
            vi_list.append(vi_data["video_information"][i])

    df2=st.dataframe(vi_list)

    return df2

def show_comments_table():
        com_list=[]
        db=client["Youtube_Data"]
        coll1=db["channel_details"]
        for com_data in coll1.find({},{"_id":0,"comment_information":1}):
                for i in range(len(com_data["comment_information"])):
                        com_list.append(com_data["comment_information"][i])

        df3=st.dataframe(com_list)

        return df3

with st.sidebar:
    st.title(":purple[YOUTUBE DATA HARVESTING AND WAREHOUSING]")
    st.header("Skill Take Away")
    st.caption("Python Scripting")
    st.caption("API Integration")
    st.caption("Data Harvesting")
    st.caption("MongoDB")
    st.caption("SQL")
    st.caption("Streamlit")

channel_id=st.text_input("Enter the Channel ID")

if st.button("collect and store data"):
    ch_ids=[]
    db=client["Youtube_Data"]
    coll1=db["channel_details"]
    for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
        ch_ids.append(ch_data["channel_information"]["Channel_Id"])

    if channel_id in ch_ids:
        st.success("Channel Details of the given channel id already exists")

    else:
        insert=channel_details(channel_id)
        st.success(insert)
if st.button("Migrate to Sql"):
    Table=tables()
    st.success(Table)

show_table=st.radio("SELECT THE TABLE FOR VIEW",("CHANNELS","PLAYLISTS","VIDEOS","COMMENTS"))

if show_table=="CHANNELS":
    show_channels_table()

if show_table=="PLAYLISTS":
    show_playlists_table()

if show_table=="VIDEOS":
    show_videos_table()

if show_table=="COMMENTS":
    show_comments_table()


mydb=psycopg2.connect(host="localhost",user="postgres",password="Podafool348",database="youtube_data",port="5432")
cursor=mydb.cursor()

question=st.selectbox("Select your question",("1. Names of all the Videos and their Corresponding Channels",
                                              "2. Channels have the most number of Videos",
                                              "3. Top 10 most viewed Videos and their respective Channels",
                                              "4. Comments were made on each Video",
                                              "5. Videos highest number of Likes",
                                              "6. Total number of Likes",
                                              "7. Total number of Views for each Channel",
                                              "8. Videos published in the year 2022",
                                              "9. Average duration of all Videos",
                                              "10. Videos with highest number of comments")
                                    