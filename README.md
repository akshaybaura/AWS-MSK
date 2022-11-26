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
| amount_requested | application_date |         loan_title          | risk_score | debt_to_income_ratio | zip_code | state | employment_length | policy_code | riskscorema50 | _modified_ts | _version | ma100 |
|------------------|------------------|-----------------------------|------------|----------------------|----------|-------|-------------------|-------------|---------------|--------------|----------|-------|
| 1000.0           | 2007-05-26       | Consolidating Debt          |        703 | 10%                  | 010xx    | MA    | < 1 year          | 0.0         |           698 |   1669446008 |          |       |       
| 6000.0           | 2007-05-27       | waksman                     |        698 | 38.64%               | 017xx    | MA    | < 1 year          | 0.0         |           650 |   1669446008 |          |       |       
| 11000.0          | 2007-05-27       | Want to consolidate my debt |        715 | 10%                  | 212xx    | MD    | 1 year            | 0.0         |           669 |   1669446008 |          |       |           
| 15000.0          | 2007-05-27       | Trinfiniti                  |        645 | 0%                   | 105xx    | NY    | 3 years           | 0.0         |           669 |   1669446008 |          |       |

### Task2
| amount_requested | application_date |     loan_title     | risk_score | debt_to_income_ratio | zip_code | state | employment_length | policy_code |  riskscorema50   |    _modified_ts     | _version | ma100 |
|------------------|------------------|--------------------|------------|----------------------|----------|-------|-------------------|-------------|------------------|---------------------|----------|-------|
| 20000.0          | 2008-01-01       | debt_consolidation |          0 | 100%                 | 281xx    | NC    | < 1 year          | 0.0         |                0 | 1669480155006710127 | 0        |       | 
| 2000.0           | 2008-01-01       | virgin islands     |        511 | 4.93%                | 008xx    | FL    | 4 years           | 0.0         |            255.5 | 1669480155024659261 | 0        |       | 
| 20000.0          | 2008-01-01       | debt_consolidation |          0 | 100%                 | 920xx    | CA    | 10+ years         | 0.0         |  170.33332824707 | 1669480155032436488 | 0        |       | 
| 25000.0          | 2008-01-01       | Dr. Brimm          |        680 | 27.89%               | 337xx    | FL    | 10+ years         | 0.0         | 238.199996948242 | 1669480155049658721 | 0        |       | 

### Task3 
| amount_requested | application_date |     loan_title     | risk_score | debt_to_income_ratio | zip_code | state | employment_length | policy_code |  riskscorema50   |    _modified_ts     | _version |  ma100 | 
|------------------|------------------|--------------------|------------|----------------------|----------|-------|-------------------|-------------|------------------|---------------------|----------|--------|
| 10000.0          | 2010-01-01       | debt_consolidation |        715 | 30.91%               | 953xx    | CA    | < 1 year          | 0.0         |  602.52001953125 | 1669481509515933299 | 1        |     715|
| 5000.0           | 2010-01-01       | home_improvement   |          0 | 0%                   | 083xx    | NJ    | < 1 year          | 0.0         | 587.380004882812 | 1669481509526072325 | 1        |   357.5|
| 7000.0           | 2010-01-01       | debt_consolidation |        554 | 38%                  | 325xx    | FL    | 2 years           | 0.0         |  589.02001953125 | 1669481509541272846 | 1        |     492|
| 20000.0          | 2010-01-01       | other              |        679 | 8.21%                | 871xx    | NM    | 3 years           | 0.0         | 591.960021972656 | 1669481509557794425 | 1        | 556.667|


### Task4
| amount_requested | application_date |     loan_title     | risk_score | debt_to_income_ratio | zip_code | state | employment_length | policy_code |  riskscorema50   |    _modified_ts     | _version | ma100 |
|------------------|------------------|--------------------|------------|----------------------|----------|-------|-------------------|-------------|------------------|---------------------|----------|-------|
| 20000.0          | 2012-01-04       | major_purchase     |        611 | 8.12%                | 754xx    | TX    | < 1 year          | 0.0         | 595.310607910156 | 1669484067469575320 | 2        | 564.61|
| 5000.0           | 2012-01-08       | debt_consolidation |        602 | 10.91%               | 706xx    | LA    | < 1 year          | 0.0         | 615.887573242188 | 1669484083254425679 | 2        | 609.37|
| 20000.0          | 2012-01-05       | debt_consolidation |        623 | 5.1%                 | 119xx    | NY    | < 1 year          | 0.0         | 576.394775390625 | 1669484068788577594 | 2        | 567.79|
| 3500.0           | 2012-01-12       | other              |        669 | 29.66%               | 010xx    | MA    | 10+ years         | 0.0         | 620.951110839844 | 1669484110727337620 | 2        | 617.13|