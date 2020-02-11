#!/bin/bash

echo "Autenticando en GCP"

PATH=$PATH:/root/google-cloud-sdk/bin	
export PATH
GCP_SERVICE_ACCOUNT_NAME=1084410096275-compute@developer.gserviceaccount.com

main_project_id=bc-te-dlake-dev-s7b3

export GOOGLE_APPLICATION_CREDENTIALS=bc-te-dlake-dev-s7b3-451c22a74cc1.json

gcloud auth activate-service-account \
${GCP_SERVICE_ACCOUNT_NAME} \
--key-file=bc-te-dlake-dev-s7b3-451c22a74cc1.json  --project=${main_project_id}

PATH=$PATH:/sbin
export PATH

echo "Iniciando proceso de HDF5"

python3 hdf5.py

echo "Fin Proceso"
