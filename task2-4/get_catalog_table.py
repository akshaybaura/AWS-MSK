import boto3
import json
client = boto3.client('glue', region_name='ap-south-1')

type_map = {'string': 'string', 'float': 'float', 'int': 'int32'}

def get_catalog(conf, type_map=type_map):
    try:
        response = client.get_table(
        DatabaseName=conf['glue_catalog']['database'],
        Name=conf['glue_catalog']['table']
        )
    except Exception as e1:
        print('Error: ',e1)
        raise e1
    else:
        columns = response['Table']['StorageDescriptor']['Columns']
        fields, payload = [], dict()
        for point in columns:
            f = {"type":type_map.get(point['Type'],'string'),"optional":True,"field":point['Name']}
            fields.append(f)
            payload.update({point['Name']:'random_value'})
        #appending meta info to schema
        fields.extend([{'type': 'int64', 'optional': True, 'field': '_modified_ts'},{'type': 'string', 'optional': True, 'field': '_version'}])
        #and to payload
        payload.update({'_modified_ts':'random_value', '_version':response['Table']['VersionId']})
        #finally
        final_datapoint = {"schema":{"type":"struct","fields":fields,"optional":False,"name":"honest_dp"},"payload":payload}
        return (final_datapoint)
        
# {'schema': {'type': 'struct', 'fields': [{'type': 'string', 'optional': True, 'field': 'amount_requested'}, {'type': 'string', 'optional': 
# True, 'field': 'application_date'}, {'type': 'string', 'optional': True, 'field': 'loan_title'}, {'type': 'float32', 'optional': True, 'field': 'risk_score'}, {'type': 'string', 'optional': True, 'field': 'debt_to_income_ratio'}, {'type': 'string', 'optional': True, 'field': 'zip_code'}, {'type': 'string', 'optional': True, 'field': 'state'}, {'type': 'string', 'optional': True, 'field': 'employment_length'}, {'type': 'string', 'optional': True, 'field': 'policy_code'}, {'type': 'float32', 'optional': True, 'field': 'ma50'}, {'type': 'string', 'optional': True, 'field': '_version'}], 'optional': False, 'name': 'honest_dp'}, 'payload': {'amount_requested': 'random_value', 'application_date': 'random_value', 'loan_title': 'random_value', 'risk_score': 'random_value', 'debt_to_income_ratio': 'random_value', 'zip_code': 'random_value', 'state': 'random_value', 'employment_length': 'random_value', 'policy_code': 'random_value', 'ma50': 'random_value', '_version': 
# '1'}
# print(get_catalog())


