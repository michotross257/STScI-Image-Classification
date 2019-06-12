import os
import argparse
from numpy.random import seed, shuffle
from shutil import copyfile


parser = argparse.ArgumentParser(description='Split directory of folders of images into train, validation, and test sets.')
parser.add_argument('path', type=str, metavar='', help='Path to parent directory')
parser.add_argument('-t', '--train', type=float, default=0.70, metavar='',
                    help='Proportion of data to allocate as training data. Float between 0.0 and 1.0.')
parser.add_argument('-v', '--validation', type=float, default=0.10, metavar='',
                    help='Proportion of data to allocate as validation data. Float between 0.0 and 1.0.')
parser.add_argument('-e', '--test', type=float, default=0.20, metavar='',
                    help='Proportion of data to allocate as test data. Float between 0.0 and 1.0.')
args = parser.parse_args()
msg = 'The sum of train, validation, and test proportions must equal 1.0'
assert(args.train + args.validation + args.test == 1.0), msg

seed(0) # set numpy random seed
train_path = os.path.join(args.path, "train")
validation_path = os.path.join(args.path, "validation")
test_path = os.path.join(args.path, "test")
classes = os.listdir(args.path)
classes = list(filter(lambda x: x not in ['.DS_Store'], classes))
extensions = ['.jpg', '.png', '.gif']


for cls in classes:
    # for each class, randomly shuffle the images, and then split into train, validation, and test sets
    images = os.listdir(os.path.join(args.path, cls))
    # make sure to grab only image files
    images = list(filter(lambda img_name: any([str(img_name).endswith(extension) for extension in extensions]), images))
    shuffle(images)
    stop = int(args.train*len(images))
    train_images = images[0: stop]
    start = stop
    stop = start + int(args.validation*len(images))
    validation_images = images[start: stop]
    test_images = images[stop:]
    # ensure no overlap between train, validation, and test sets
    msg = 'Overlap found between train, validation, and test sets.'
    assert(len(set(train_images) & set(validation_images) & set(test_images)) == 0), msg
    for item in [(train_path, train_images),
                 (validation_path, validation_images),
                 (test_path, test_images)]:
        destination_path = os.path.join(item[0], cls)
        # if class folder doesn't exist, then create it
        if not os.path.isdir(destination_path):
            os.makedirs(destination_path)
        # copy all of the images of that set (e.g. validation) for that class into the respective folder
        for img_path in item[1]:
            copyfile(os.path.join(args.path, cls, img_path), os.path.join(destination_path, img_path))
    print('{} images\n'.format(cls) + '-'*20)
    print('Number of Train Images: {}\nNumber of Validation Images: {}\nNumber of Test Images: {}\n'.format(len(train_images),
                                                                                                            len(validation_images),
                                                                                                            len(test_images)))
