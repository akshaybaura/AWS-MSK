# Data Engineering Assessment Test:

## About the architecture

![alt text](/images/honest-tasks.jpeg)

### Task1
Using just the data in 2007, we are interested in the Moving Average of the Risk_Score column (50 rows back). Calculate the MA(50) of the Risk_Score and put it in a new column called RiskScoreMA50. Store this in a Data Store of your choice.  
#### Services
1. **S3** to host the source data
2. **Glue(spark)** to do ETL
3. **Redshift** as data store

#### Flow
1. Data stored in S3 is pulled by a Glue-spark job.
2. Glue performs filtering for only 2007 data and pushes it into Redshift using JDBC.

*Post load into Redshift* into a staging table, data is then moved to a target table after calculating the moving average.

#### Tech-debt / improvements
Since, this is a batch job, it will have its peak of processing and stale period thereafter until next execution. It totally depends on the cadence of arrival of the input file if batch job makes sense or conversion to a streaming job.  

### Tasks 2-4
2. Starting in year 2008 to 2009, mock up a Kafka Stream of the data coming into the system. Calculate the MA50 for the incoming Risk Score as well. Then store the data in the data store.  
3. Starting year 2009 onwards, you get a new requirement to also calculate the MA100 of the Risk Score and store it, but the data schema must also to be trackable. Make a new column on the data store that has a new column called MA100, update the version of this data store, and do version controlling so that the original features can easily be accessed.  
4. The team found out that they did not actually want the MA50, but the EMA50 for the purpose of this calculation (want to edit the feature without changing the name). In this case, they do not want a new column for the EMA50, but replace the existing MA50 with new logic. Edit the logic for the feature creation pipeline, and version control the new version of the feature store.  

#### Services
1. **MSK** as kafka cluster
2. **EC2** as client 
3. **Glue catalog**
4. **Redshift** as datastore
#### Flow
1. Producer residing in EC2, streams the data in the source file to the MSK topic (inflow). (For sake of simplicity I considered copying the data file into EC2 instance itself instead of pulling from S3 everytime).  
2. Consumer also residing in EC2, polls for data from the topic and calculates the MA(tasks 2, 3, 4) and EMA(task 4) and spits out a data structure with streamed values appended with calculated values.  
3. Consumer now queries the Glue catalog to get the latest schema for the Redshift target table.  
4. Catalog returns the schema alongwith the version.
5. Consumer moulds the data struct created in step 2 into the schema obtained in step 4 to create a JSON datapoint with schema and payload and sends the datapoint to the reproducer.  
6. Reproducer sends the datapoint to the target topic.  
7. Data is streamed into redshift to create the table in case it doesn't exist or evolve table schema in case schema change (limited to column addition) is seen and populate the table.

#### Tech-debt / improvements
1. Since the load of message consumption and processing happens on EC2, it may be worthwhile to think about scaling resources with ECS/EKS.
2. Data contracts can be agreed on by using schema registry in Glue.  
3. Sending out JSON(with schema and payload) to the connector could be held ill performant due to bloated datapoint. This can be solved by utilising implicit schema supply using Avro SerDe.

## About the codebase

### Directory- task1
1. glue_script.py  
- script contains the data pull and filter logic using spark sql from s3, also flushes the data to redshift staging table.
2. RiskScoreMA50.sql  
- script contains the table ddl and of target table and the insert logic to populate the moving average from the staged data received above.

### Directory- task2-4
1. config.ini  
- contains configurations for artifacts created and used within the project
2. produce.py  
- contains logic to read data from gzipped csv file, filter as per year(s) from input arguments and produce them to kafka inflow topic. 
2. consume.py  
- contains logic to poll data from the inflow topic, calculate ma50 and ma100 and place data into datapoint template. 
3. get_catalog_table.py
- contains logic to get latest version of table schema from glue catalog and add other metadata to the final datapoint template before values are filled into the template.  
5. reproduce.py  
- contains logic to put data into the outflow topic to send to redshift via connector.

### Directory- utilities
1. deleteme.json  
- contains config to reset the offset of any topic to nascent state. Usage-  
```
./kafka-delete-records.sh --bootstrap-server **masked** --offset-json-file deleteme.json
```  
2. kafka_catalog_table.py  
- contains reusable logic to create and update schema versions in glue catalog tables.  
3. redshift-sink-connector.json  
- config to create the JDBC connector for Redshift.  
4. redshift_sink-custom-plugin.json  
- config to create the custom plugin to be used while creating the JDBC connector.

## Redshift data snapshots

### Task1 
|amount_requested | application_date |            loan_title            | risk_score | debt_to_income_ratio | zip_code | state | employment_length | policy_code | riskscorema50 | _modified_ts|
|------------------|------------------|----------------------------------|------------|----------------------|----------|-------|-------------------|-------------|---------------|-------------|
| 1000.0           | 2007-05-26       | Wedding Covered but No Honeymoon |        693 | 10%                  | 481xx    | NM    | 4 years           | 0.0         |           693 |   1669446008|
| 1500.0           | 2007-05-27       | mdrigo                           |        509 | 9.43%                | 209xx    | MD    | < 1 year          | 0.0         |           635 |   1669446008|
| 3900.0           | 2007-05-27       | For Justin.                      |        700 | 10%                  | 469xx    | IN    | 2 years           | 0.0         |           660 |   1669446008|
| 10000.0          | 2007-05-27       | NOTIFYi Inc                      |        693 | 10%                  | 210xx    | MD    | < 1 year          | 0.0         |           673 |   1669446008|  



### Task2
| amount_requested | application_date |                loan_title                | risk_score | debt_to_income_ratio | zip_code | state | employment_length | policy_code |  ma50  |    _modified_ts     | _version |
|------------------|------------------|------------------------------------------|------------|----------------------|----------|-------|-------------------|-------------|--------|---------------------|----------|
| 9000.0           | 2009-01-01       | I.T. Consulting Operating Capital        | 517        | 0.51%                | 015xx    | MA    | 3 years           | 0.0         | 415.58 | 1669462778866773500 | 0        |
| 12000.0          | 2009-01-01       |                                          | 646        | 10.22%               | 028xx    | RI    | 1 year            | 0.0         | 413.48 | 1669462778879462996 | 0        |
| 15000.0          | 2009-01-01       | Pay Off credit Cards/Better credit score | 556        | 7.07%                | 324xx    | FL    | 10+ years         | 0.0         |  424.6 | 1669462778906298025 | 0        |
| 1500.0           | 2009-01-01       |                                          | 719        | 0%                   | 163xx    | PA    | < 1 year          | 0.0         |  436.8 | 1669462778946058581 | 0        |  


### Task3 
|amount_requested | application_date |     loan_title     | risk_score | debt_to_income_ratio | zip_code | state | employment_length | policy_code |  ma50  |    _modified_ts     | _version | ma100 |
|------------------|------------------|--------------------|------------|----------------------|----------|-------|-------------------|-------------|--------|---------------------|----------|------|
| 2000.0           | 2011-01-01       | major_purchase     | 0          | -1%                  | 871xx    | NM    | < 1 year          | 0.0         | 405.24 | 1669464279106588457 | 1        |     0|
| 1000.0           | 2011-01-01       | renewable_energy   | 611        | 5.2%                 | 368xx    | AL    | < 1 year          | 0.0         | 402.44 | 1669464279116304677 | 1        | 305.5|
| 25000.0          | 2011-01-01       | debt_consolidation | 502        | 20.52%               | 705xx    | LA    | < 1 year          | 0.0         | 412.48 | 1669464279124274697 | 1        |   371|
| 10000.0          | 2011-01-01       | wedding            | 0          | 0%                   | 985xx    | WA    | < 1 year          | 0.0         |    426 | 1669464279139737110 | 1        | 357.8|

### Task4
| amount_requested | application_date |     loan_title     | risk_score | debt_to_income_ratio | zip_code | state | employment_length | policy_code |  ma50  |    _modified_ts     | _version |  ma100|
|------------------|------------------|--------------------|------------|----------------------|----------|-------|-------------------|-------------|--------|---------------------|----------|--------|
| 10000.0          | 2012-01-01       | debt_consolidation | 0          | 0%                   | 729xx    | AR    | < 1 year          | 0.0         | 405.24 | 1669468473194040216 | 2        |       0|
| 35000.0          | 2012-01-01       | debt_consolidation | 614        | 18%                  | 554xx    | MN    | < 1 year          | 0.0         |  402.5 | 1669468473200286560 | 2        |     307|
| 2500.0           | 2012-01-01       | other              | 579        | 53.92%               | 287xx    | NC    | < 1 year          | 0.0         | 414.08 | 1669468473207720711 | 2        | 397.667|
| 10000.0          | 2012-01-01       | debt_consolidation | 0          | -1%                  | 729xx    | AR    | < 1 year          | 0.0         | 427.12 | 1669468473221650942 | 2        |     369|