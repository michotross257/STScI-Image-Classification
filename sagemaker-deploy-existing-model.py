import argparse
import boto3
import sagemaker
from sagemaker.estimator import Estimator


parser = argparse.ArgumentParser(description='Deploy a trained model to SageMaker endpoint.')
parser.add_argument('region', type=str, metavar='',
                    help='Default region when creating new connections.')
parser.add_argument('train_job', type=str, metavar='',
                    help='Name of training job to attach to.')
parser.add_argument('-e', '--endpoint_name', type=str, default=None, metavar='',
                    help='Name of training job to attach to.')
args = parser.parse_args()


if __name__ == '__main__':
    boto3_sess = boto3.Session(region_name=args.region)
    sagemaker_sess = sagemaker.Session(boto3_sess)
    attached_estimator = Estimator.attach(args.train_job,
                                          sagemaker_session=sagemaker_sess)
    print("\nDeploying model...")
    _ = attached_estimator.deploy(initial_instance_count=1,
                                  instance_type='ml.t2.medium',
                                  endpoint_name=args.endpoint_name)
    print("\nModel is deployed.\n")
