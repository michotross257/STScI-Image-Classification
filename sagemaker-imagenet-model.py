import boto3
import sagemaker
from sagemaker import get_execution_role
from sagemaker.amazon.amazon_estimator import get_image_uri


# --------------------------- EDIT THESE VALUES ---------------------------
REGION               = '' # region of s3 bucket in which train & validation data is stored
BUCKET               = '' # name of s3 bucket in which train & validation data is stored
NUM_CLASSES          = 0  # the number of unique labels/classes in the dataset
NUM_TRAINING_SAMPLES = 0  # the number of rows in the training set
ROLE_ARN             = '' # the ARN of your SageMaker Execution Role used to give learning and hosting access to data
# --------------------------- EDIT THESE VALUES ---------------------------


if __name__ == "__main__":
    DELETE_ENDPOINT = False
    # folder in s3 bucket
    prefix = 'image-classification-transfer-learning'
    # configure boto3 session according to s3 region
    boto3_sess = boto3.Session(region_name=REGION)
    sess = sagemaker.Session(boto3_sess)
    # Amazon sagemaker image classification docker image
    training_image = get_image_uri(sess.boto_region_name, 'image-classification', repo_version="latest")
    s3_train = 's3://{}/{}/train/'.format(BUCKET, prefix)
    s3_validation = 's3://{}/{}/validation/'.format(BUCKET, prefix)
    s3_output_location = 's3://{}/{}/output'.format(BUCKET, prefix)

    model = sagemaker.estimator.Estimator(training_image,
                                          ROLE_ARN,
                                          train_instance_count=1,
                                          train_instance_type='ml.p2.xlarge',
                                          train_volume_size=50,
                                          train_max_run=360000,
                                          input_mode='File',
                                          output_path=s3_output_location,
                                          sagemaker_session=sess)

    model.set_hyperparameters(num_layers=34,
                              use_pretrained_model=1,
                              image_shape="3,256,256",
                              num_classes=NUM_CLASSES,
                              num_training_samples=NUM_TRAINING_SAMPLES,
                              mini_batch_size=1,
                              epochs=10,
                              learning_rate=0.0001,
                              precision_dtype='float32')

    train_data = sagemaker.session.s3_input(s3_train, distribution='FullyReplicated',
                                            content_type='application/x-recordio',
                                            s3_data_type='S3Prefix')

    validation_data = sagemaker.session.s3_input(s3_validation, distribution='FullyReplicated',
                                                 content_type='application/x-recordio',
                                                 s3_data_type='S3Prefix')

    data_channels = {'train': train_data, 'validation': validation_data}
    model.fit(inputs=data_channels, logs=True)
    deployed_model = model.deploy(initial_instance_count=1, instance_type='ml.t2.medium')

    # ---------------------------------------------------------------------------------------
    # DELETE_ENDPOINT = True
    # If you delete the model endpoint, then you will not be able to invoke it for predictions.
    # You stop paying for the endpoint as soon as you delete it.
    # ---------------------------------------------------------------------------------------
    # DELETE_ENDPOINT = False
    # If you do not delete the model endpoint, then you will be able to invoke it for predictions,
    # but you will be paying for 1) the endpoint as long as it is running and 2) the data in and
    # out of the endpoint whenever it is invoked.
    # Check here for pricing: https://aws.amazon.com/sagemaker/pricing/
    # ---------------------------------------------------------------------------------------
    if DELETE_ENDPOINT:
        deployed_model.delete_endpoint()
