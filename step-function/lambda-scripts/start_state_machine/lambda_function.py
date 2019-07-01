import os
import json
from uuid import uuid4
import boto3


VALID_EXTENSIONS = ['fits']
VALID_SUBGROUPS = ['flc', 'flt']
state_machine_arn = os.environ.get('STATE_MACHINE_ARN')
client = boto3.client('stepfunctions')

def get_error_msg(name, input_element, valid_elements):
    msg = 'Invalid {}\n\t{} found: {}'.format(name, name.title(), input_element)
    msg += '\n\t{} must be one of the following: '.format(name.title())
    for index, valid_element in enumerate(valid_elements):
        if index == len(valid_elements)-1:
            msg += valid_element
        else:
            msg += valid_element + ', '
    return msg


def lambda_handler(event, context):
    '''Only start the state machine if the file is a valid subgroup and extension.'''
    event = json.loads(event['Records'][0]['Sns']['Message'])['Records'][0]
    bucket = event['s3']['bucket']['name']
    key = event['s3']['object']['key']
    
    # EXAMPLE 1
    # =======================================================
    # key      : hst/public/idpy/idpya7i2q/idpya7i2q_spt.fits
    # image_id : idpya7i2q_spt
    # subgroup : spt
    # extension: fits
    
    # EXAMPLE 2
    # ============================================================
    # key      : hst/public/idpy/idpya7i0q/idpya7i0q_flt_thumb.jpg
    # image_id : idpya7i0q_flt_thumb
    # subgroup : flt_thumb
    # extension: jpg
    
    image_id, extension = key.split('.')
    image_id = image_id.split('/')[-1]
    index = 0
    for character in image_id:
        if character == '_':
            break
        else:
            index += 1
    subgroup = image_id[index+1:]

    elements = {'extension': {'input': extension,
                              'valid': VALID_EXTENSIONS},
                'subgroup' : {'input': subgroup,
                              'valid': VALID_SUBGROUPS}}

    problems = []
    for element in elements:
        input_element = elements[element]['input']
        valid_elements = elements[element]['valid']
        if input_element not in valid_elements:
            msg = get_error_msg(element, input_element, valid_elements)
            problems.append(msg)

    if len(problems):
        print('File "{}" not processed.\nReason(s):'.format(key))
        for count, problem in enumerate(problems):
            print('{}) '.format(count+1) + problem)
        raise SystemExit
        
    state_machine_input = {
        'image_id': image_id,
        'subgroup': subgroup,
        's3': {
            'key': key,
            'bucket': bucket
        }
    }
    
    response = client.start_execution(
        stateMachineArn=state_machine_arn,
        name=str(uuid4()),
        input=json.dumps(state_machine_input)
    )
    print("State machine started.")
