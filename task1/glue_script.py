import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

args = getResolvedOptions(sys.argv, ['JOB_NAME', 's3_source_path', 'redshift_role', 'redshift_con'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

## create dynamic frame from s3 file
dyf = glueContext.create_dynamic_frame.from_options(connection_type="s3",
                                                        connection_options={"paths": [args["s3_source_path"]]},
                                                        format="csv",
                                                        format_options={
                                                            "withHeader": True,
                                                        },
)
df=dyf.toDF()
df.createOrReplaceTempView("df_view")

## filter to get only 2007 data
filtered = spark.sql("""select `amount requested` as amount_requested, `application date` as application_date, `loan title` as loan_title, cast(risk_score as int) as risk_score,
`debt-to-income ratio` as debt_to_income_ratio, `zip code` as zip_code, state as state, `employment length` as employment_length, `policy code` as policy_code
from df_view where split(`application date`,'-')[0] = '2007' """)

tgt_dyf = DynamicFrame.fromDF(filtered, glueContext, "tgt_dyf")

## copy into redshift
my_conn_options = {
    "dbtable": "honest_table_staging",
    "database": "dev",
    "aws_iam_role": args["redshift_role"]
    }

glueContext.write_dynamic_frame.from_jdbc_conf(
    frame = tgt_dyf, 
    catalog_connection = args["redshift_con"], 
    connection_options = my_conn_options,
    redshift_tmp_dir = args["TempDir"]) 

job = Job(glueContext)
job.init(args['JOB_NAME'], args)
job.commit()