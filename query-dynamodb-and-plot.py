import boto3
import argparse
import pandas as pd
import astropy
import astropy.coordinates as coord
import astropy.units as units
import matplotlib.pyplot as plt
from matplotlib.colors import to_rgba


parser = argparse.ArgumentParser(description='Query DynamoDB table for all entries and then plot the entries.')
parser.add_argument('profile', type=str, help='Name of the AWS profile to use.')
parser.add_argument('table', type=str, help='Name of the DynamoDB to query.')
args = parser.parse_args()

sess = boto3.Session(profile_name=args.profile)
db_client = sess.client('dynamodb')

kwargs = {
    'TableName': args.table,
    'ProjectionExpression': "#img_id,#cls,#tm,#stars,#deep,#cluster,#nebula,DEC_TARG,RA_TARG",
    'Select':'SPECIFIC_ATTRIBUTES',
    'ExpressionAttributeNames':{"#img_id": "IMAGE ID",
                                "#cls": "PREDICTED CLASS",
                                "#tm": "TIME ADDED TO TABLE",
                                "#stars": "PROBABILITY OF STARS",
                                "#deep": "PROBABILITY OF DEEP",
                                "#cluster": "PROBABILITY OF CLUSTER",
                                "#nebula": "PROBABILITY OF NEBULA"}
}

response = db_client.scan(**kwargs)
responses = []
# query table for all entries
while 'LastEvaluatedKey' in response.keys():
    key = response['LastEvaluatedKey']
    responses.extend([item for item in response['Items']])
    temp_kwargs = kwargs.copy()
    temp_kwargs['ExclusiveStartKey'] = key
    response = db_client.scan(**temp_kwargs)
responses.extend([item for item in response['Items']])

# extract the values from each entry
type_conversion = {'N': float, 'S': str}
for index in range(len(responses)):
    vals = responses[index]
    temp = {}
    for key in vals:
        _type = list(vals[key].keys())[0]
        val = list(vals[key].values())[0]
        temp[key] = type_conversion[_type](val)
    responses[index] = temp


ordered_cols = ['IMAGE ID', 'PREDICTED CLASS',
                'PROBABILITY OF CLUSTER', 'PROBABILITY OF DEEP', 
                'PROBABILITY OF NEBULA', 'PROBABILITY OF STARS',
                'DEC_TARG', 'RA_TARG']

# stick the data into a DataFrame
df = pd.DataFrame(columns=list(responses[0].keys()))
for index, item in enumerate(responses):
    df.loc[index, :] = item
df = df[ordered_cols]

colors = {'CLUSTER': 'red',
          'DEEP'   : 'blue',
          'NEBULA' : 'green',
          'STARS'  : 'yellow'}

# ==============================================
# code below taken from or inspired by:
# http://learn.astropy.org/plot-catalog.html
# Authors: Adrian M. Price-Whelan and Kelle Cruz
# ==============================================

# convert to angular coordinates
ra = coord.Angle(df['RA_TARG'] * units.degree)
ra = ra.wrap_at(180 * units.degree)
dec = coord.Angle(df['DEC_TARG'] * units.degree)

fig = plt.figure(figsize=(16, 14))
ax = fig.add_subplot(111, projection="mollweide")
max_len = max([len(k) for k in colors.keys()])
print('\nClass Count\n' + "="*16)
for cls in colors.keys():
    indexes = df[df['PREDICTED CLASS'] == cls].index
    print('{}'.format(cls).ljust(max_len) + ' -> {}'.format(len(indexes)))
    _ra = [ra.radian[index] for index in indexes]
    _dec = [dec.radian[index] for index in indexes]
    rgba_color = [to_rgba(c=colors[cls],
                          alpha=df.loc[index, 'PROBABILITY OF ' + cls]) for index in indexes]
    ax.scatter(_ra, _dec, color=rgba_color, label=cls)
print('-'*16 + "\n".ljust(max_len+5) + "{}\n".format(df.shape[0]))
ax.legend()
plt.show()
