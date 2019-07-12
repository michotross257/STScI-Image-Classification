import os
import json
import time
import boto3
import numpy as np
from astropy.io import fits
from PIL import Image
from PIL import FitsStubImagePlugin

# file size of image to be passed to SageMaker must be less than 5 MB
MAX_FILE_SIZE_FOR_SAGEMAKER = int(5e+6) # in bytes
# keys whose values are to be extracted from the header of the primary HDU of a FITS file
hdu_keys = ['FILENAME',
            'FILETYPE',
            'TELESCOP',
            'INSTRUME',
            'TARGNAME',
            'RA_TARG',
            'DEC_TARG',
            'PROPOSID',
            'PR_INV_L',
            'PR_INV_F',
            'GYROMODE',
            'DATE-OBS',
            'TIME-OBS',
            'EXPTIME',
            'EXPFLAG',
            'OBSTYPE',
            'OBSMODE',
            'DETECTOR',
            'FILTER',
            'APERTURE']

# environment variables
endpoint_name = os.environ.get('ENDPOINT_NAME')
destination_bucket_name = os.environ.get('DESTINATION_BUCKET')
classes = [cls.strip() for cls in os.environ.get('CLASSES').split(',')]

# acquire AWS service access
s3_client = boto3.client('s3')
runtime = boto3.client('runtime.sagemaker')

class FileSizeException(Exception):
    pass


def lambda_handler(event, context, call=None, callback=None):
    bucket = event['s3']['bucket']
    key = event['s3']['key']
    download_path = '/tmp/{}.fits'.format(event['image_id'])
    
    # download image and send off to SageMaker for classification
    s3_client.download_file(bucket, key, download_path, ExtraArgs={'RequestPayer': 'requester'})
    
    with fits.open(download_path) as downloaded_file:
        primary_hdu = downloaded_file[0]
        header_keys = list(primary_hdu.header.keys())
        metadata = {key: primary_hdu.header[key] for key in hdu_keys if key in header_keys}
        # if present, the first and fourth indexes of the FITS file are two pieces of one single image
        if len(downloaded_file) > 4 and downloaded_file[4].header['EXTNAME'] == 'SCI':
            # two portions of one single image
            data_1_of_2 = downloaded_file[1].data
            data_2_of_2 = downloaded_file[4].data
            height = data_1_of_2.shape[0] + data_2_of_2.shape[0]
            width = data_1_of_2.shape[1]
            # merge the two image Header Data Units (HDU) into one image
            temp = np.zeros((height, width))
            temp[0: int(height/2), :] = data_1_of_2
            temp[int(height/2): height, :] = data_2_of_2
            data = temp
        else:
            # get one of the image Header Data Units (HDU) from each FITS file
            data = downloaded_file[1].data
    
    # clean up temp download
    os.remove(download_path)
    
    # trim the extreme values
    top = np.percentile(data, 99)
    data[data > top] = top
    bottom = np.percentile(data, 1)
    data[data < bottom] = bottom
    
    # scale the data
    data = data - data.min()
    data = (data / data.max()) * 255.0
    data = np.flipud(data)
    data = np.uint8(data)
    
    image = Image.fromarray(data)
    download_path = download_path.replace('.fits','.jpg')
    image.save(download_path)
    file_size = os.path.getsize(download_path)
    msg = 'File size of image: {:.2f} MB\n'.format(file_size * 1e-6)
    msg += 'Size of an image to be classified by SageMaker endpoint must less than 5 MB.'
    if file_size >= MAX_FILE_SIZE_FOR_SAGEMAKER:
        # clean up temp download
        os.remove(download_path)
        raise FileSizeException(msg)
    s3_client.upload_file(download_path, destination_bucket_name, event['image_id'])

    with open(download_path, 'rb') as downloaded_image:
        content = downloaded_image.read()
    # clean up temp download
    os.remove(download_path)
    response = runtime.invoke_endpoint(EndpointName=endpoint_name,
                                       Body=content)
    result = json.loads(response['Body'].read().decode())

    item = {'probabilities': {}}
    # determine the predicted class based on the highest probability returned
    predicted_class = {'class': '', 'probability': 0.0}
    for result_index, probability in enumerate(result):
        item['probabilities'][classes[result_index]] = str(round(probability, 8))
        if probability > predicted_class['probability']:
            predicted_class['class'] = classes[result_index]
            predicted_class['probability'] = probability
        # if two or more classes have the same high probability, then create a concatenated string of the classes
        elif probability == predicted_class['probability']:
            predicted_class['class'] = predicted_class['class'] + ', ' + classes[result_index]
    item['predicted_class'] = predicted_class['class']
    event['metadata'] = metadata
    event['classification'] = item
    
    return event
