from google.cloud import pubsub_v1
from google.cloud import storage
import json
import time

if __name__=="__main__":
    PROJECT_ID="effective-relic-431713-q6"

    pubsub_topic="projects/effective-relic-431713-q6/topics/topic1"

    #Create a storage client
    storage_client = storage.Client()

    #Specify the bucket and file names
    bucket_name = 'sourcebucketjson1249'
    file_name = 'jsonfile1.json'

    #Get the bucket and blob
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_name)

    print(f"bucket {bucket}")

    print(f"blob {blob}")
    read_output=blob.download_as_string()       #This will convert the entire data to string format still 1 record will be splitted in 5 rows if 5 columns are there
    data = json.loads(read_output)              #This will convert String data to dictionary format so that 1 record for 5 columns

    pubsub_topic="projects/effective-relic-431713-q6/topics/topic1"
    publisher=pubsub_v1.PublisherClient()
    
    for line in data:
        dict_data=line   #Each record is loaded into dict_data which is in dictionary format
        
        text_data=json.dumps(dict_data)  #This will convert from dictionary format to text format in single row only because pubsub will accept only text format 
        print(text_data)
        future=publisher.publish(pubsub_topic,bytes((text_data),encoding='utf-8'))
        future.result()
        #print(f"result: {future.result()}")

        time.sleep(2)