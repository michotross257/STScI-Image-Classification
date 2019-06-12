import argparse
import csv
from google.cloud import storage

# NOTE: make sure to enable access to the required Google Cloud bucket
# otherwise you will get a permission error.

parser = argparse.ArgumentParser(description='Compile CSV file of Google Cloud images for Google Cloud AutoML.')
parser.add_argument('bucket', type=str, metavar='', help='Name of Google Cloud bucket.')
parser.add_argument('project_folder', type=str, metavar='', help='Name of Google Cloud project.')
parser.add_argument('save_path', type=str, metavar='', help='Path to CSV file (e.g. /Users/johndoe/Desktop/gcloud_data.csv).')
parser.add_argument('classes', type=str, nargs='*', metavar='', help='Classes/labels in dataset (e.g. dog cat bird).')
args = parser.parse_args()


if __name__ == "__main__":
    client = storage.Client()
    bucket = client.get_bucket(args.bucket)
    datasets = ["TRAIN", "VALIDATION", "TEST"]

    with open(args.save_path, "w") as file:
        csv_writer = csv.writer(file)
        for dataset in datasets:
            for cls in args.classes:
                image_path = "{}/{}/{}/".format(args.project_folder, dataset, cls)
                for image in bucket.list_blobs(prefix=image_path):
                    csv_writer.writerow([dataset, "gs://" + bucket.name + "/" + image.name, cls])
