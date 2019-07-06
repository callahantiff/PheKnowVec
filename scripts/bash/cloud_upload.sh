#!/usr/bin/env bash

# Ask the user for details to upload data to Google Storage
echo 'This program is designed to up load all files in a directory to a specified Google Cloud Storage Bucket. Please
  provide the prompted information below: '
echo
read -p 'Path to data directory: ' data_directory
read -sp 'Cloud Storage  Bucket (i.e. gs://[BUCKET_NAME]): ' bucket_name
echo

gsutil -m cp -r ${data_directory} ${bucket_name}
