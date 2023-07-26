# Spotify-ETL-project-AWS
This is an end-to-end Data Engineering project to extract data from Spotify API, perform some transformation on the data using Python and then query the data with Amazon Athena 

## Objectives:
The main objective of the project is to build an entire ETL project using AWS services like S3, CloudWatch, Lambda Function, Crawler, AWS Glue and Athena

## Requirments:
1. AWS account: you may need more than the basic AWS account as some functionalities (like Athena Query) won't work on the basic account. [AWS Free account](https://aws.amazon.com/free/)
2. Spotify Developer account. [Spotify API] (https://developer.spotify.com/documentation/web-api)
3. Python packages/libraries:
   - Spotipy [Documentation](https://spotipy.readthedocs.io/en/2.22.1/)
   - Pandas [Documentation](https://pandas.pydata.org/docs/getting_started/install.html#installing-from-pypi)
   - Boto3 [AWS Documentation](https://aws.amazon.com/sdk-for-python/)


## Project Description:
1. We start extracting Spotify data using Spotipy library with Python deployed on Lambda Function. We upload the extracted JSON file into Amazon s3 Bucket. For the purpose of automating this step, we created a Trigger that runs every 24 hours to extract the data from Spotify API and save it into s3 bucket.
2. Next, everytime there is a new JSON file stored in s3 Bucket, a new event will be triggered that runs the second Python code on Lambda Function. This Python code reads the new raw JSON file and transform it into three separate .csv files, each file contains different information (Songs data, Albums data, Artists data), then upload these .csv files into a separate folder in s3 Bucket.
3. Now that we have our transformed data stored on s3, we create a Crawler to populate the AWS Glue Data Catalog with tables. [More details on Crawlers](https://docs.aws.amazon.com/glue/latest/dg/add-crawler.html)
4. Finally, we can query our created tables in AWS Glue Tables using Athena queries.

## Project Architecture:
![Project Architecture](https://github.com/ahmad-beshr/Spotify-ETL-project-AWS/blob/main/arch.drawio.png)

## Project Source:
This is a replication of darshilparmar project [here](https://github.com/darshilparmar/python-for-data-engineering)
