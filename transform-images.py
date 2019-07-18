import os
from PIL import Image, ImageFile
import numpy as np

ImageFile.LOAD_TRUNCATED_IMAGES = True
np.random.seed(1234)
LOWER_BOUND = 8
UPPER_BOUND = 12
path = ''
classes = ['CLUSTER','NEBULA','STARS','DEEP']

# possible transformations
def original(image):
    '''No transformation, i.e. return the original image.'''
    return image
def flip(image):
    '''Flip the image.'''
    return image.rotate(90)
def crop(image, r_factor, g_factor):
    '''Crop the image.'''
    width, height = image.size
    left = width*r_factor
    right = left*g_factor
    top = height*r_factor
    bottom = top*g_factor
    return image.crop((left, top, right, bottom))
transformations = [original, flip, crop]

cnt = 1
msg_len = 0
num_images = sum([len(list(filter(lambda x: x != '.DS_Store',
                                  os.listdir(os.path.join(path, cls))))) for cls in classes])
for cls in classes:
    path_to_folder = os.path.join(path, cls)
    imgs = os.listdir(path_to_folder)
    imgs = list(filter(lambda x: x != '.DS_Store', imgs))
    for img_file in imgs:
        path_to_img = os.path.join(path_to_folder, img_file)
        img = Image.open(path_to_img)
        # randomly pick a transformation
        transformation = np.random.choice(transformations)
        if transformation.__name__ == 'crop':
            bound = np.random.uniform(LOWER_BOUND, UPPER_BOUND, size=1)[0]
            reduction_factor = 1/bound
            growth_factor = (1/reduction_factor)-2
            img = transformation(img, reduction_factor, growth_factor)
        else:
            img = transformation(img)
        new_path = os.path.join(path + '_UPDATED', cls)
        if not os.path.isdir(new_path):
            os.makedirs(new_path)
        img.save(os.path.join(new_path, img_file))
        msg = '\rImage {} of {} - "{}" - Transformation: {}'.format(cnt,
                                                                    num_images,
                                                                    img_file,
                                                                    transformation.__name__)
        buffer = msg_len - len(msg)
        buffer = buffer if buffer > 0 else 0
        print(msg, end=' '*buffer, flush=True)
        msg_len = len(msg)
        cnt += 1
