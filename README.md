# Problem Statement
Create a Pipeline that will ingest data from external stage(In this situation it is S3) to raw_table and from raw table , finally load the data into target table(final table).This whole process need to be done automatically .

# Project Architecture
![project_architecture](https://user-images.githubusercontent.com/62836744/230765554-6eeae20f-9605-4055-9822-445fe451ce66.jpg)

# 👨‍💻 Techstack used
1. Snowflake
2. AWS

# Folder Architecture
S3/Bucket_name/folder_name/file.json
when any new file come into folder it will generate notification that wll trigger snowpipe  present in snowflake.

# Steps
1. Create S3 bucket that will act as external stage(assign proper role so snowflake will read data from S3 bucket).
2. Create event notification and add SQS as destination so that it will trigger snowpipe in snowflake when new data comes to S3 bucket.
3. Create all Required Tables,Schemas and assign permission to 'Sysadmin' role from 'AccountAdmin' Role.
4. Create storage integration provide necessary credentials and create external stage which will help to connect with S3 bucket.
5. Create Stream  on raw table which will capture the new arrival data into raw table.
6. Create task which run on interval of '1 minute' that will merge data into final table if stream data will already present in final table but if stream data is completely new then it will insert complete data into final table.
7. Create snowpipe that will ingest data  from S3 bucket automatically into raw table.


# 🧐 Conclusion
We can create end to end datwarehousing pipeline with the help of Snowflake , it reduce the human effort and do all the task automatically
