import os
import json
import time
import boto3
import numpy as np
from astropy.io import fits
from PIL import Image
from PIL import FitsStubImagePlugin
from google.cloud import automl_v1beta1 as automl


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
# --------------------------- AUTOML ---------------------------
project_id = os.environ.get('AUTOML_PROJECT_ID')
model_id = os.environ.get('AUTOML_MODEL_ID')
compute_region = os.environ.get('AUTOML_COMPUTE_REGION')
# the level of confidence the model must have to return the results for a class label
score_threshold = '0.0' # value from 0.0 to 1.0; default is 0.5
# --------------------------- AWS ---------------------------
destination_bucket_name = os.environ.get('DESTINATION_BUCKET')

# acquire AWS service access
s3_client = boto3.client('s3')

def get_prediction(content, project_id, model_id):
    '''Pass an image to automl model for classification.
       Function returns an automl_v1beta1.types.PredictResponse object.'''
    automl_client = automl.AutoMlClient()
    model_full_id = automl_client.model_path(project_id, compute_region, model_id)
    prediction_client = automl.PredictionServiceClient()
    payload = {'image': {'image_bytes': content}}
    params = {'score_threshold': score_threshold}
    response = prediction_client.predict(model_full_id, payload, params)

    return response


def lambda_handler(event, context, call=None, callback=None):
    bucket = event['s3']['bucket']
    key = event['s3']['key']
    download_path = '/tmp/{}.fits'.format(event['image_id'])
    
    # download image and send off to AutoML for classification
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
    s3_client.upload_file(download_path, destination_bucket_name, event['image_id'])

    with open(download_path, 'rb') as downloaded_image:
        content = downloaded_image.read()
    # clean up temp download
    os.remove(download_path)
    response = get_prediction(content, project_id, model_id)

    #========================================================================

    item = {'probabilities': {}}
    # determine the predicted class based on the highest probability returned
    predicted_class = {'class': '', 'probability': 0.0}
    for payload in response.payload:
        item['probabilities'][payload.display_name] = str(round(payload.classification.score, 8))
        if payload.classification.score > predicted_class['probability']:
            predicted_class['class'] = payload.display_name
            predicted_class['probability'] = payload.classification.score
        # if two or more classes have the same high probability, then create a concatenated string of the classes
        elif payload.classification.score == predicted_class['probability']:
            predicted_class['class'] = predicted_class['class'] + ', ' + payload.display_name
    item['predicted_class'] = predicted_class['class']
    event['metadata'] = metadata
    event['classification'] = item
    
    return event
