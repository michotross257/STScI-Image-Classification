import os
import argparse
from numpy.random import seed, shuffle
from shutil import copyfile

parser = argparse.ArgumentParser(description='Split directory of folders of images into train, validation, and test sets.')
parser.add_argument('-p', '--path', type=str, required=True, metavar='', help='Path to parent directory')
args = parser.parse_args()
PATH = args.path
CLASSES = os.listdir(PATH)
try:
    CLASSES.remove('.DS_Store')
except:
    pass

seed(0) # set numpy random seed
TRAIN_PROPORTION = 0.70
VALIDATION_PROPORTION = 0.10
TEST_PROPORTION = 0.20
train_path = os.path.join(PATH, "train")
validation_path = os.path.join(PATH, "validation")
test_path = os.path.join(PATH, "test")

for cls in CLASSES:
    images = os.listdir(os.path.join(PATH, cls))
    try:
        images.remove('.DS_Store')
    except:
        pass
    shuffle(images)
    stop = int(TRAIN_PROPORTION*len(images))
    train_images = images[0: stop]
    start = stop
    stop = start + int(VALIDATION_PROPORTION*len(images))
    validation_images = images[start: stop]
    test_images = images[stop:]
    msg = "Overlap found between training, validation, and test sets."
    assert(len(set(train_images) & set(validation_images) & set(test_images)) == 0), msg
    for item in [(train_path, train_images),
                 (validation_path, validation_images),
                 (test_path, test_images)]:
        destination_path = os.path.join(item[0], cls)
        if not os.path.isdir(destination_path):
            os.makedirs(destination_path)
        for img_path in item[1]:
            copyfile(os.path.join(PATH, cls, img_path), os.path.join(destination_path, img_path))
    print('{} images\n'.format(cls) + '-'*20)
    print('Number of Train Images: {}\nNumber of Validation Images: {}\nNumber of Test Images: {}\n'.format(len(train_images),
                                                                                                            len(validation_images),
                                                                                                            len(test_images)))
