# Formula1 Race Analytics Pipeline on Azure 
Formula1 Data pipeline using Azure and Databricks

![image](https://user-images.githubusercontent.com/41427089/230989132-db8e23e8-3aea-4e45-bf38-7a26a8e28142.png)

# Pipeline Tech Stack
## Source: Ergast API
## Data Loading Pattern - (Incremental + Full load)
## Storage: Azure Data Lake Storage Gen2
## Processing: Databricks (PySpark and SparkSQL)
## Updation and Upserts: Delta Lake
## Orchestration: Azure Data Factory
## Presentation: PowerBI and Databricks dashboards

##Source ER:

![image](https://user-images.githubusercontent.com/41427089/230990504-3b66070c-7992-4be3-9822-e8488cf4b4d4.png)

### RAW data storage

 - Data stored in Data lake Raw container
 - Stored as external tables
 
 ![image](https://user-images.githubusercontent.com/41427089/230990800-ea360f42-bcfb-4f69-a7ca-a25242bc5051.png)
### Processed data storage

 - Data stored in Data lake processed container
 - Stored as managed tables
### Transformation 

 - Data stored in Data lake Presentation container
 - Stored as managed table
 - Transformed using PySpark and SparkSQL
 
### Data Updation and deletion
 
- Used Delta Lake on top of databricks to update and upsert the data

 
### Orchestration 

 - Pipeline automated using Azure Data Factory
 - Pulls Data from API, transforms and publishes to PowerBI dashboard
 ![image](https://user-images.githubusercontent.com/41427089/232262943-021ffb63-4d9d-49b6-ae6a-a57929c10ec9.png)
![image](https://user-images.githubusercontent.com/41427089/232262972-9db66a94-992e-4a00-83e0-d8a9dd8297bd.png)


![image](https://user-images.githubusercontent.com/41427089/232262824-998aa562-5b6e-4402-8a17-31edc470ba17.png)
