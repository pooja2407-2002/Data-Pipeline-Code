from airflow import DAG
from airflow.providers.amazon.aws.operators.emr import EmrCreateJobFlowOperator, EmrAddStepsOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import pendulum

local_tz = pendulum.timezone("Asia/Kolkata")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': days_ago(0),
    'timezone': local_tz,
}

dag = DAG(
    'new_TRUMP_DAG',
    default_args=default_args,
    description='DAG to create an EMR cluster, run multiple Spark jobs, and keep the cluster alive',
    schedule_interval='51 13 * * *',  # Daily at 10 AM IST
    tags=['example'],
    catchup=False,
)

JOB_FLOW_OVERRIDES = {
    "Name": "Cluster-e2e",  # Cluster name
    "LogUri": "s3://aws-logs-103989150928-ap-south-1/elasticmapreduce",  # Log URI
    "ReleaseLabel": "emr-7.1.0",  # EMR release label
    "ServiceRole": "arn:aws:iam::103989150928:role/service-role/AmazonEMR-ServiceRole-20250412T172215",  # Service role ARN
    "Instances": {
        "InstanceGroups": [
            {
                "Name": "Primary",
                "Market": "ON_DEMAND",
                "InstanceRole": "MASTER",
                "InstanceType": "m5.xlarge",
                "InstanceCount": 1,
                "EbsConfiguration": {
                    "EbsBlockDeviceConfigs": [
                        {
                            "VolumeSpecification": {
                                "VolumeType": "gp2",
                                "SizeInGB": 32,
                            },
                            "VolumesPerInstance": 2,
                        }
                    ]
                }
            }
        ],
        "Ec2KeyName": "43batch",  # EC2 key name
        "Ec2SubnetId": "subnet-07efaf8353db812cd",  # EC2 subnet ID
        "EmrManagedMasterSecurityGroup": "sg-04d19191418d6e3e3",  # EMR managed master security group
        "EmrManagedSlaveSecurityGroup": "sg-0502ae4f759b3bc1a",  # EMR managed slave security group
        "KeepJobFlowAliveWhenNoSteps": True,  # Keep the cluster alive when no steps
        "TerminationProtected": False,
        "AdditionalMasterSecurityGroups": [],  # Additional master security groups (if any)
        "AdditionalSlaveSecurityGroups": [],  # Additional slave security groups (if any)
    },
    "Applications": [
        {"Name": "Hadoop"},
        {"Name": "Hive"},
        {"Name": "JupyterEnterpriseGateway"},
        {"Name": "Livy"},
        {"Name": "Spark"},
    ],
    "VisibleToAllUsers": True,
    "JobFlowRole": "AmazonEMR-InstanceProfile-20250412T172158",  # Job flow role ARN
    "Tags": [
        {
            "Key": "for-use-with-amazon-emr-managed-policies",
            "Value": "true"
        }
    ],
    "ScaleDownBehavior": "TERMINATE_AT_TASK_COMPLETION",
    "AutoTerminationPolicy": {
        "IdleTimeout": 60  # Auto termination policy
    }
}

SPARK_STEPS = [
    {
        'Name': 's3Job',
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': [
                'spark-submit',
                '--deploy-mode', 'client',
                '--master', 'local[*]',
                's3://z43n/pyfiles/s3j.py'
            ]
        }
    },
    {
        'Name': 'SnowJob',
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': [
                'spark-submit',
                '--deploy-mode', 'client',
                '--packages', 'net.snowflake:spark-snowflake_2.12:3.1.1',
                '--master', 'local[*]',
                's3://z43n/pyfiles/snow.py'
            ]
        }
    },
    {
        'Name': 'ApiJob',
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': [
                'spark-submit',
                '--deploy-mode', 'client',
                '--packages', 'net.snowflake:spark-snowflake_2.12:3.1.1',
                '--master', 'local[*]',
                's3://z43n/pyfiles/api.py'
            ]
        }
    },
    {
        'Name': 'MasterJob',
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': [
                'spark-submit',
                '--deploy-mode', 'client',
                '--packages', 'net.snowflake:spark-snowflake_2.12:3.1.1',
                '--master', 'local[*]',
                's3://z43n/pyfiles/master.py'
            ]
        }
    }
]

create_emr_cluster = EmrCreateJobFlowOperator(
    task_id='create_emr_cluster',
    job_flow_overrides=JOB_FLOW_OVERRIDES,
    aws_conn_id='aws_default',
    dag=dag,
)

add_spark_steps = EmrAddStepsOperator(
    task_id='add_spark_steps',
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
    aws_conn_id='aws_default',
    steps=SPARK_STEPS,
    dag=dag,
)

create_emr_cluster >> add_spark_steps
