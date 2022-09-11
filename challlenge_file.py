import os
import time
from selenium import webdriver
from selenium.webdriver.common.by import By
import pandas as pd
import numpy as np
from selenium.webdriver.common.keys import Keys
from pprint import pprint
import googleapiclient.discovery
import mysql.connector as conn
import sys
import pymongo
from pytube import YouTube
import boto3
from botocore.exceptions import NoCredentialsError
from flask import Flask, render_template, request
from flask_cors import CORS,cross_origin

DRIVER_PATH = os.environ.get("DRIVER_PATH")
ACCESS_KEY = os.environ.get("ACCESS_KEY")
SECRET_KEY = os.environ.get("SECRET_KEY")
DEVELOPER_KEY = os.environ.get("DEVELOPER_KEY")
SQL_HOSTNAME = os.environ.get("SQL_HOSTNAME")
SQL_USER = os.environ.get("SQL_USER")
SQL_PASSWORD = os.environ.get("SQL_PASSWORD")
MONGODB_PASSWORD = os.environ.get("MONGODB_PASSWORD")
MONGODB_ENDPOINT = "mongodb+srv://Puneet681:{}@challenge28082022.sxuqacj.mongodb.net/test".format(MONGODB_PASSWORD)




if not os.path.exists(r"./video"):
    os.makedirs(r"./video")


# for headless webdriver
op = webdriver.ChromeOptions()
op.add_argument('headless')
wd = webdriver.Chrome(executable_path=DRIVER_PATH,options=op)

# for with head driver
# wd = webdriver.Chrome()


#imp variables for Get_Comments_By_V_ID
os.environ["OAUTHLIB_INSECURE_TRANSPORT"] = "1"
api_service_name = "youtube"
api_version = "v3"

#for SQL querry
def run_query(query, database=None, host="localhost", user="root", password=SQL_PASSWORD):
    try:
        if database is None:
            db = conn.connect(host=host, user=user, password=password)
        else:
            db = conn.connect(host=host, user=user, password=password, database=database)
        cursor = db.cursor()
        cursor.execute(query)
        if cursor.rowcount > 0:
            db.commit()
        else:
            result = cursor.fetchall()
            return result
    except conn.Error:
        a, b, c = sys.exc_info()
    finally:
        db.close()


def Get_Comments_By_V_ID(videoId=str,max_results=10):
    youtube = googleapiclient.discovery.build(
        api_service_name, api_version, developerKey = DEVELOPER_KEY)
    request = youtube.commentThreads().list(
        part="snippet",
        maxResults=max_results,
        order="orderUnspecified",
        videoId=videoId
    )
    response = request.execute()
    Comment_Result=[]
    for i in range(len(response['items'])):
        comment=(((((response['items'][i]))['snippet'])['topLevelComment'])['snippet'])['textOriginal']
        commenter_name=(((((response['items'][i]))['snippet'])['topLevelComment'])['snippet'])['authorDisplayName']
        Comment_Result.append({"videoId": videoId,'commenter_name':commenter_name,'Comment':comment})
    return Comment_Result


def Get_Line_By_V_ID(videoId=str):
    youtube = googleapiclient.discovery.build(
        api_service_name, api_version, developerKey = DEVELOPER_KEY)
    request = youtube.videos().list(
        part='snippet,statistics',
        id=videoId
    )
    response = request.execute()
    likes = response['items'][0]['statistics']['likeCount']
    views = response['items'][0]['statistics']['viewCount']
    comments = response['items'][0]['statistics']['commentCount']
    return {"videoId": videoId,'likes':likes,'views':views,"comments":comments}

def Search(input_url,V_Count):
    Search_Results=[]
    Comments_Result=[]
    ch_video_url=input_url+"/videos"
    wd.get(ch_video_url)
    Ch_Name=wd.find_elements(by=By.CSS_SELECTOR, value='yt-formatted-string#text.style-scope.ytd-channel-name')[0].text
    video_result=wd.find_elements(by=By.XPATH, value='//div[1]/div[1]/div[1]/h3/a')
    V_Thumbnail=wd.find_elements(by=By.XPATH,value='//div[1]/ytd-thumbnail/a/yt-img-shadow/img')
    i = 0
    bag = 0
    while True:
        if bag == V_Count:
            break
        if i%14==0:
            while(True):
                height = wd.execute_script("return document.body.scrollHeight")
                time.sleep(1)
                video_result=wd.find_elements(by=By.XPATH, value='//div[1]/div[1]/div[1]/h3/a')
                V_Thumbnail=wd.find_elements(by=By.XPATH,value='//div[1]/ytd-thumbnail/a/yt-img-shadow/img')
                wd.find_element(By.TAG_NAME,'body').send_keys(Keys.END)
                if int(height)==0:
                    break
#From Wed Scrapping
        V_Title=video_result[i].get_attribute('title')
        V_URL=video_result[i].get_attribute('href')
        V_Thumbnail_src = V_Thumbnail[i].get_attribute("src")
        V_ID=(V_URL.replace("https://www.youtube.com/watch?v=",""))
        if len(V_ID) > 11:
            i+=1
            continue
        Search_Results.append({'V_ID':V_ID,'Ch_Name':Ch_Name,'V_Title':V_Title})

# From YouTube API
        V_Data=Get_Line_By_V_ID(V_ID)
        Likes=V_Data['likes']
        Views=V_Data['views']
        Comments_count=V_Data['comments']
        Search_Results[bag]['Comments_count']=Comments_count
        Search_Results[bag]['likes']=Likes
        Search_Results[bag]['views']=Views

# From YouTube API
        comments=Get_Comments_By_V_ID(V_ID,max_results=10)
        for j in range(len(comments)):
            Commenter_Name=comments[j]['commenter_name']
            Comment=comments[j]['Comment']
            Comments_Result.append({'V_ID':V_ID,'Commenter_Name':Commenter_Name,'Comment':Comment,'V_URL':V_URL,'V_Thumbnail':V_Thumbnail_src})
        i+=1
        bag+=1
    return Search_Results,Comments_Result

def loding_in_SQL(SQL_data):
    a=[]
    #to creat table
    run_query("CREATE TABLE IF NOT EXISTS data_table(V_ID VARCHAR(15),Ch_Name VARCHAR(100),V_Title VARCHAR(150),Comments_count INT(20),likes INT(20),views INT)",database='video_details',host=SQL_HOSTNAME,user=SQL_USER)
    for i in range(len(SQL_data)):
        run_query("INSERT INTO data_table(V_ID, Ch_Name, V_Title, Comments_count, likes, views) SELECT * FROM (SELECT '{a}', '{b}','{c}', '{d}', '{e}', '{f}') as temp WHERE NOT EXISTS (SELECT V_ID FROM data_table WHERE V_ID = '{a}') LIMIT 1".format(a=SQL_data[i]['V_ID'],b=SQL_data[i]['Ch_Name'],c=SQL_data[i]['V_Title'],d=int(SQL_data[i]['Comments_count']),e=int(SQL_data[i]['likes']),f=int(SQL_data[i]['views'])),database='video_details',host=SQL_HOSTNAME,user=SQL_USER)
        a.append(SQL_data[i]['V_ID'])
    b=tuple(a)
    return b

def loding_data_from_SQL(V_IDs=tuple):
    return run_query("select distinct * from data_table where V_ID in {}".format(V_IDs),database='video_details',host=SQL_HOSTNAME,user=SQL_USER)

def downloade_yt_video(v_id,downloade_path):
    yt = YouTube("https://www.youtube.com/watch?v="+v_id)
    try:
        yt.streams.filter(progressive=True, file_extension='mp4').order_by('resolution').asc().first().download(output_path=downloade_path,filename=v_id+".mp4")
        return True
    except :
        print("video is in live strean")
        return False

def delete_yt_video_from_local(v_id,downloade_path):
    if os.path.exists("{}/{}.mp4".format(downloade_path,v_id)):
        os.remove("{}/{}.mp4".format(downloade_path,v_id))

def upload_to_aws(local_file, bucket, s3_file):
    s3 = boto3.client('s3', aws_access_key_id=ACCESS_KEY,
                      aws_secret_access_key=SECRET_KEY)
    try:
        s3.upload_file(local_file, bucket, s3_file)
        print("Upload Successful")
        location = boto3.client('s3', aws_access_key_id=ACCESS_KEY,aws_secret_access_key=SECRET_KEY).get_bucket_location(Bucket=bucket)['LocationConstraint']
        url = "https://s3-%s.amazonaws.com/%s/%s" % (location, bucket, s3_file)
        return url
    except FileNotFoundError:
        print("The file was not found")
        return False
    except NoCredentialsError:
        print("Credentials not available")
        return False


def downloade_video_and_upload_to_s3(vid,local_folder,bucket):
    status = downloade_yt_video(vid,local_folder)
    if status:
        s3_url = upload_to_aws('{}/{}.mp4'.format(local_folder,vid), bucket, '{}.mp4'.format(vid))
    else:
        s3_url = "Video is Streaming Live cannot download."
    delete_yt_video_from_local(vid,local_folder)
    return s3_url

def mongo_connection():
    client = pymongo.MongoClient(MONGODB_ENDPOINT)
    db = client.test
    database=client['video_ditails']
    collection = database['data_table']
    return collection

def loding_in_Mongo(Mongo_Data=list):
  for i in range(len(Mongo_Data)):
    data_check={}
    data_check['V_ID']=Mongo_Data[i]['V_ID']
    data_check['Commenter_Name']=Mongo_Data[i]['Commenter_Name']
    data_check['Comment']=Mongo_Data[i]['Comment']

    count = (mongo_connection()).count_documents(data_check)
    if count == 0:
        (mongo_connection()).insert_one(Mongo_Data[i])

def data_from_mongo(V_ID=tuple):
    all_data=[]
    for i in range(len(V_ID)):
        data=(mongo_connection()).find({'V_ID': V_ID[i]})
        for j in data:
            all_data.append(j)
    return all_data

def s3_urls(V_IDs=list) :
    s3_URL={}
    for i in range(len(V_IDs)):
        s3_URL[V_IDs[i]]=downloade_video_and_upload_to_s3(vid=V_IDs[i],local_folder=r"./video",bucket='aws-data-sanjiv')
    return s3_URL



app = Flask(__name__)

@app.route('/',methods=['GET'])  # route to display the home page
@cross_origin()
def homePage():
    return render_template("index.html")

@app.route('/channelData',methods=['POST','GET']) # route to show the review comments in a web UI
@cross_origin()
def index():
    if request.method == 'POST':
        try:
            searchString = request.form['content'].replace(" ","")
            print("*****************code running*****************")
            try:
                SQL1,Mongo1=Search(searchString,3)
            except Exception:
                return "Enter Correct URL"
            print("*****************Loading into SQL*****************")
            V_IDs=loding_in_SQL(SQL_data=SQL1)
            # DO NOT DELETE THIS SECTION    
            ######################################
            print("*****************Uploade data in S3*****************")
            s3_url_dict=s3_urls(V_IDs)
            for i in range(len(Mongo1)):
                Mongo1[i]['s3_URL']=s3_url_dict[Mongo1[i]['V_ID']]
            print("*****************Loading into Mongodb*****************")
            loding_in_Mongo(Mongo1)
            print("*****************getting data from mondo*****************")
            Mongo_df=pd.DataFrame(data_from_mongo(V_IDs))
            print("*****************getting data from SQL*****************")
            SQL_df=pd.DataFrame(loding_data_from_SQL(V_IDs=V_IDs),columns=['V_ID','Ch_Name','V_Title','Comments_count','likes','views'])
            Mongo_df.drop('_id',inplace=True,axis=1)
            final_data = pd.merge(SQL_df,Mongo_df ,on='V_ID', how='inner')
            print("*****************code endded*****************")
            return render_template('results.html', final_data=final_data.T.to_dict().values())
        except Exception as e:
            print('The Exception message is: ',e)
            return 'something is wrong'
    else:
        return render_template('index.html')

if __name__ == "__main__":
    #app.run(host='127.0.0.1', port=8001, debug=True)
    app.run(host = "0.0.0.0",debug=True)
