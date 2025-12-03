#Test Cluster
gcloud dataproc clusters create cluster-b086 \
    --enable-component-gateway \
    --region us-central1 \
    --single-node \
    --master-machine-type n4-standard-4 \
    --master-boot-disk-type hyperdisk-balanced \
    --master-boot-disk-size 100 \
    --image-version 2.2-debian12 \
    --optional-components JUPYTER \
    --scopes 'https://www.googleapis.com/auth/cloud-platform' \
    --project nownews-479616 
