import csv
from google.cloud import storage

# NOTE: make sure to enable access to the required Google Cloud bucket
# otherwise you will get a permission error.

# --------------------------- EDIT THESE VALUES ---------------------------
BUCKET_NAME    = '' # name of your gcloud bucket
PROJECT_FOLDER = '' # name of project
SAVE_PATH      = '' # path to CSV file. e.g. '/Users/johndoe/Desktop/gcloud_data.csv'
CLASSES        = [] # e.g. ['CLUSTER', 'DEEP', 'NEBULA', 'STARS']
# --------------------------- EDIT THESE VALUES ---------------------------

if __name__ == "__main__":
    client = storage.Client()
    bucket = client.get_bucket(BUCKET_NAME)
    datasets = ["TRAIN", "VALIDATION", "TEST"]

    with open(SAVE_PATH, "w") as file:
        csv_writer = csv.writer(file)
        for dataset in datasets:
            for cls in CLASSES:
                image_path = "{}/{}/{}/".format(PROJECT_FOLDER, dataset, cls)
                for image in bucket.list_blobs(prefix=image_path):
                    csv_writer.writerow([dataset, "gs://" + bucket.name + "/" + image.name, cls])
