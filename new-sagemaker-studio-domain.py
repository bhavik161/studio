
import sys
from pip._internal import main

main(['install', '-I', 'boto3', '--target', '/tmp/', '--no-cache-dir', '--disable-pip-version-check', '--upgrade'])
sys.path.insert(0,'/tmp/')

import json
import boto3
print(boto3.__version__)


def get_iam_execution_role():
    client = boto3.client('iam')
    
    response = client.get_role(
    RoleName='SandboxServiceRole'
                                )
    role = response['Role']['Arn']
    print(role)
    return role
    
def get_account_id():

    # Create an STS client
    sts_client = boto3.client('sts')

    # Get the caller identity
    caller_identity = sts_client.get_caller_identity()

    # Extract the account ID from the caller identity
    account_id = caller_identity['Account']
    return account_id

def create_bucket_for_canvas():
    account_id = get_account_id()
    bucket_name = f'sagemaker-canvas-us-east-1-{account_id}'
    s3 = boto3.client('s3')
    try:
        # Check if the bucket exists
        s3.head_bucket(Bucket=bucket_name)
        print(f"Bucket '{bucket_name}' already exists.")
        return(bucket_name)
    except Exception as e:
        # If the bucket doesn't exist, create it
        if e.response['Error']['Code'] == '404':
            s3.create_bucket(Bucket=bucket_name)
            print(f"Bucket '{bucket_name}' created successfully.")
            return(bucket_name)
    else:
        raise  # Raise other errors
        
def get_network_settings():
    ec2=boto3.client('ec2')
    vpcs = ec2.describe_vpcs()
    
    vpcid = vpcs['Vpcs'][0]['VpcId']
    
    print(vpcid)
    
    subnets = ec2.describe_subnets()
    lst_subnets=[]
    for subnet in subnets['Subnets']:
        #print(subnet)
        #print(subnet['SubnetId'])
        lst_subnets.append(subnet['SubnetId'])
    print(lst_subnets)
    
    sggroups = ec2.describe_security_groups()
    lst_sgs=[]
    for sg in sggroups['SecurityGroups']:
        if 'default' in sg['GroupName']:
            lst_sgs.append(sg['GroupId'])
    print(lst_sgs)
    
    return(vpcid, lst_subnets, lst_sgs)

def create_sm_domain():
    vpc,subnets,securitygroups = get_network_settings()
    bucket_name = create_bucket_for_canvas()
    smclient = boto3.client('sagemaker')
    response = smclient.create_domain(
        DomainName='new-studio-domain',
        AuthMode='IAM',
        DefaultUserSettings={
            'ExecutionRole': get_iam_execution_role(),
            'SecurityGroups': securitygroups,
            
            'CanvasAppSettings': {
                'TimeSeriesForecastingSettings': {
                    'Status': 'DISABLED',
                    #'AmazonForecastRoleArn': 'string'
                },
                'ModelRegisterSettings': {
                    'Status': 'ENABLED',
                    #'CrossAccountModelRegisterRoleArn': 'string'
                },
                'WorkspaceSettings': {
                    'S3ArtifactPath': f's3://{bucket_name}',
                    #'S3KmsKeyId': 'string'
                },
    
            },
    
            'DefaultLandingUri': 'studio::',
            'StudioWebPortal': 'ENABLED',
    
        },
        DomainSettings={
            'SecurityGroupIds': securitygroups,
    
        },
        SubnetIds=subnets,
        VpcId=vpc,
    
        DefaultSpaceSettings={
            'ExecutionRole': get_iam_execution_role(),
            'SecurityGroups':securitygroups,
        }
    )
    return response
    
def lambda_handler(event, context):
    # TODO implement
    #get_iam_execution_role()
    
    smclient = boto3.client('sagemaker')
    vpcid, lst_subnets,lst_sgs = get_network_settings()
    domainstatus = create_sm_domain()
    print(domainstatus)
    
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
