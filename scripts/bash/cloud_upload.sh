#!/usr/bin/env bash

# running script requires:
# 1. Change permission: chmod u+ cloud_upload.sh
# 2. Run script using: ./scripts/bash/cloud_upload.sh
# example inputs:
#bucket_name: gs://sandbox-tc.appspot.com/PheKnowVec_1.0_09August2019/
#data_directory: temp_results/temp_results

# Ask the user for details to upload data to Google Storage
echo 'This program is designed to up load all files in a directory to a specified Google Cloud Storage Bucket. Please
  provide the prompted information below: '
echo
read -p 'Path to data directory: ' data_directory
read -sp 'Cloud Storage  Bucket (i.e. gs://[BUCKET_NAME]): ' bucket_name
echo

gsutil -m cp -r ${data_directory} ${bucket_name}