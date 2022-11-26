import boto3

client = boto3.client('glue', region_name = 'ap-south-1')

choice = input('enter choice: ')
table_name = 'honest_table'

if choice == 'create':
    response = client.create_table(
        DatabaseName='honest_db',
        TableInput={
            'Name': table_name,
            'StorageDescriptor': {
                'Columns': [
                    {
                        'Name': 'amount_requested',
                        'Type': 'string',
                        'Comment': ''
                    },
                    {
                        'Name': 'application_date',
                        'Type': 'string',
                        'Comment': ''
                    },
                    {
                        'Name': 'loan_title',
                        'Type': 'string',
                        'Comment': ''
                    },
                    {
                        'Name': 'risk_score',
                        'Type': 'float',
                        'Comment': ''
                    },
                    {
                        'Name': 'debt_to_income_ratio',
                        'Type': 'string',
                        'Comment': ''
                    },
                    {
                        'Name': 'zip_code',
                        'Type': 'string',
                        'Comment': ''
                    },
                    {
                        'Name': 'state',
                        'Type': 'string',
                        'Comment': ''
                    },
                    {
                        'Name': 'employment_length',
                        'Type': 'string',
                        'Comment': ''
                    },
                    {
                        'Name': 'policy_code',
                        'Type': 'string',
                        'Comment': ''
                    },
                    {
                        'Name': 'riskscorema50',
                        'Type': 'float',
                        'Comment': ''
                    },
                    # {
                    #     'Name': 'ma100',
                    #     'Type': 'float',
                    #     'Comment': ''
                    # }
                ],
                'Location': table_name, 
                'InputFormat': 'org.apache.hadoop.mapred.TextInputFormat',
                'OutputFormat': 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat',
                'SerdeInfo': {
                    'SerializationLibrary': 'org.apache.hadoop.hive.serde2.OpenCSVSerde',
                    'Parameters': {
                        'separatorChar': ','
                    }
                },
                'Parameters': {
                    'topicName': table_name,
                    'typeOfData': 'kafka',
                    'connectionName': 'honest-msk'
                }
            },
            'TableType': 'EXTERNAL_TABLE',
            'Parameters': {
                'classification': 'csv',
                'connectionName': 'honest-msk'
            }
        }   
        )
    print('created')
elif choice=='update':
    response = client.update_table(
        DatabaseName='honest_db',
        TableInput={
            'Name': table_name,
            'StorageDescriptor': {
                'Columns': [
                    {
                        'Name': 'amount_requested',
                        'Type': 'string',
                        'Comment': ''
                    },
                    {
                        'Name': 'application_date',
                        'Type': 'string',
                        'Comment': ''
                    },
                    {
                        'Name': 'loan_title',
                        'Type': 'string',
                        'Comment': ''
                    },
                    {
                        'Name': 'risk_score',
                        'Type': 'float',
                        'Comment': ''
                    },
                    {
                        'Name': 'debt_to_income_ratio',
                        'Type': 'string',
                        'Comment': ''
                    },
                    {
                        'Name': 'zip_code',
                        'Type': 'string',
                        'Comment': ''
                    },
                    {
                        'Name': 'state',
                        'Type': 'string',
                        'Comment': ''
                    },
                    {
                        'Name': 'employment_length',
                        'Type': 'string',
                        'Comment': ''
                    },
                    {
                        'Name': 'policy_code',
                        'Type': 'string',
                        'Comment': ''
                    },
                    {
                        'Name': 'riskscorema50',
                        'Type': 'float',
                        'Comment': ''
                    },
                    {
                        'Name': 'ma100',
                        'Type': 'float',
                        'Comment': ''
                    }
                ],
                'Location': table_name, 
                'InputFormat': 'org.apache.hadoop.mapred.TextInputFormat',
                'OutputFormat': 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat',
                'SerdeInfo': {
                    'SerializationLibrary': 'org.apache.hadoop.hive.serde2.OpenCSVSerde',
                    'Parameters': {
                        'separatorChar': ','
                    }
                },
                'Parameters': {
                    'topicName': table_name,
                    'typeOfData': 'kafka',
                    'connectionName': 'honest-msk'
                }
            },
            'TableType': 'EXTERNAL_TABLE',
            'Parameters': {
                'classification': 'csv',
                'connectionName': 'honest-msk'
            }
            
        }
                )
    print('updated')
elif choice=='update2':
    response = client.update_table(
        DatabaseName='honest_db',
        TableInput={
            'Name': table_name,
            'StorageDescriptor': {
                'Columns': [
                    {
                        'Name': 'amount_requested',
                        'Type': 'string',
                        'Comment': ''
                    },
                    {
                        'Name': 'application_date',
                        'Type': 'string',
                        'Comment': ''
                    },
                    {
                        'Name': 'loan_title',
                        'Type': 'string',
                        'Comment': ''
                    },
                    {
                        'Name': 'risk_score',
                        'Type': 'float',
                        'Comment': ''
                    },
                    {
                        'Name': 'debt_to_income_ratio',
                        'Type': 'string',
                        'Comment': ''
                    },
                    {
                        'Name': 'zip_code',
                        'Type': 'string',
                        'Comment': ''
                    },
                    {
                        'Name': 'state',
                        'Type': 'string',
                        'Comment': ''
                    },
                    {
                        'Name': 'employment_length',
                        'Type': 'string',
                        'Comment': ''
                    },
                    {
                        'Name': 'policy_code',
                        'Type': 'string',
                        'Comment': ''
                    },
                    {
                        'Name': 'riskscorema50',
                        'Type': 'float',
                        'Comment': 'updated logic to ema'
                    },
                    {
                        'Name': 'ma100',
                        'Type': 'float',
                        'Comment': ''
                    }
                ],
                'Location': table_name, 
                'InputFormat': 'org.apache.hadoop.mapred.TextInputFormat',
                'OutputFormat': 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat',
                'SerdeInfo': {
                    'SerializationLibrary': 'org.apache.hadoop.hive.serde2.OpenCSVSerde',
                    'Parameters': {
                        'separatorChar': ','
                    }
                },
                'Parameters': {
                    'topicName': table_name,
                    'typeOfData': 'kafka',
                    'connectionName': 'honest-msk'
                }
            },
            'TableType': 'EXTERNAL_TABLE',
            'Parameters': {
                'classification': 'csv',
                'connectionName': 'honest-msk'
            }
            
        }
                )
else:
    print('invalid choice')