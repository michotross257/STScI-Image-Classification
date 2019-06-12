import argparse
import boto3
import sagemaker
from sagemaker import get_execution_role
from sagemaker.amazon.amazon_estimator import get_image_uri


parser = argparse.ArgumentParser(description='Train and optionally deploy SageMaker model.')
parser.add_argument('region', type=str, metavar='',
                    help='Region of S3 bucket in which train & validation data is stored.')
parser.add_argument('bucket', type=str, metavar='',
                    help='Name of S3 bucket in which train & validation data is stored.')
parser.add_argument('numclasses', type=int, metavar='',
                    help='Number of unique labels/classes in the dataset.')
parser.add_argument('numsamples', type=int, metavar='',
                    help='Number of rows in the training set.')
parser.add_argument('arn', type=str, metavar='',
                    help='ARN of your SageMaker Execution Role used to give learning and hosting access to data.')
parser.add_argument('-e', '--epochs', type=int, default=10, metavar='',
                    help='Number of training epochs.')
parser.add_argument('-b', '--batchsize', type=int, default=1, metavar='',
                    help='Number of training samples to process before updating the model’s weights.')
parser.add_argument('-l', '--learning_rate', type=float, default=0.0001, metavar='',
                    help='Model learning rate.')
parser.add_argument('-d', '--deploy', action='store_true',
                    help='Whether to deploy the model after training.')
args = parser.parse_args()


if __name__ == "__main__":
    # folder in S3 bucket
    prefix = 'image-classification-transfer-learning'
    # configure boto3 session according to S3 region
    boto3_sess = boto3.Session(region_name=args.region)
    sess = sagemaker.Session(boto3_sess)
    # Amazon SageMaker image classification docker image
    training_image = get_image_uri(sess.boto_region_name, 'image-classification', repo_version="latest")
    s3_train = 's3://{}/{}/train/'.format(args.bucket, prefix)
    s3_validation = 's3://{}/{}/validation/'.format(args.bucket, prefix)
    s3_output_location = 's3://{}/{}/output'.format(args.bucket, prefix)

    model = sagemaker.estimator.Estimator(training_image,
                                          args.arn,
                                          train_instance_count=1,
                                          train_instance_type='ml.p2.xlarge',
                                          train_volume_size=50,
                                          train_max_run=360000,
                                          input_mode='File',
                                          output_path=s3_output_location,
                                          sagemaker_session=sess)

    model.set_hyperparameters(num_layers=34,
                              use_pretrained_model=1,
                              image_shape='3,256,256',
                              num_classes=args.numclasses,
                              num_training_samples=args.numsamples,
                              mini_batch_size=args.batchsize,
                              epochs=args.epochs,
                              learning_rate=args.learning_rate,
                              precision_dtype='float32')

    train_data = sagemaker.session.s3_input(s3_train, distribution='FullyReplicated',
                                            content_type='application/x-recordio',
                                            s3_data_type='S3Prefix')

    validation_data = sagemaker.session.s3_input(s3_validation, distribution='FullyReplicated',
                                                 content_type='application/x-recordio',
                                                 s3_data_type='S3Prefix')

    data_channels = {'train': train_data, 'validation': validation_data}
    model.fit(inputs=data_channels, logs=True)

    # --------------------------------------------------------------------------------------------
    # args.deploy = True
    #
    # If you do create the model endpoint (i.e. deploy the model), then you will be able to
    # invoke it for predictions. You will be paying for 1) the endpoint as long as it is running
    # and 2) the data in and out of the endpoint whenever it is invoked.
    # Check here for pricing: https://aws.amazon.com/sagemaker/pricing/
    # --------------------------------------------------------------------------------------------
    # args.deploy = False
    #
    # If you do not create the model endpoint (i.e. do not deploy the model), then you will not
    # be able to invoke it for predictions. You will not be paying for an endpoint.
    # --------------------------------------------------------------------------------------------
    if args.deploy:
        print("\nDeploying model...")
        deployed_model = model.deploy(initial_instance_count=1, instance_type='ml.t2.medium')
        print("\nModel is deployed.\n")
