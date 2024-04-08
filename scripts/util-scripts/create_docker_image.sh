#!/bin/bash

# Creates a Dockerfile and creates a GCP-available image
# Command example:
#   bash scripts/util-scripts/create_docker_image.sh -p <PROJECT>  -r <REGION> -m <IMAGE_NAME> -n <REPO_NAME> \
#        -t <TAG> -f <FLINK_VERSION> -P -B -M

set -e

# Optional parameters
region="us-central1"
flink_version="1.18.1"
repo_name="gmf-repo"
image_name="flink-image"
image_tag="latest"
project_id=
install_plugins=true
use_cloud_build=false
build_maven=true

while getopts ":f:m:n:p:r:t:BMP" opt; do 
  case $opt in
    # Strings
    f) flink_version=$OPTARG ;;
    m) image_name=$OPTARG ;;
    n) repo_name=$OPTARG ;;
    p) project_id=$OPTARG ;;
    r) region=$OPTARG ;;
    t) image_tag=$OPTARG ;;

    # Booleans
    B) use_cloud_build=true ;;
    M) build_maven=false ;;
    P) install_plugins=false ;; 

    \?)
       echo "Invalid option: -$OPTARG" >&2
       exit 1
       ;;
  esac
done

if [[ ${#project_id} -ge 1 ]]; then
  PROJECT=$project_id
else
  echo "Using default project to build image"
  PROJECT=$(gcloud config get-value project)
fi

SCRIPT_PATH=$(dirname "$0")

cd $SCRIPT_PATH
# Dockerfile needs to access compiled JAR
FILE="../../Dockerfile"
echo "Creating Dockerfile"
GCS_PLUGIN_URL=https://repo1.maven.org/maven2/org/apache/flink/flink-gs-fs-hadoop/$flink_version/flink-gs-fs-hadoop-$flink_version.jar
echo "FROM flink:$flink_version" > "$FILE"


if [[ $install_plugins == true ]]; then
  echo "Adding plugins to Dockerfile"
  echo "
  RUN mkdir /opt/flink/plugins/gs-fs-hadoop/
  RUN wget -q -O /opt/flink/plugins/gs-fs-hadoop/flink-gs-fs-hadoop-$flink_version.jar ${GCS_PLUGIN_URL}
  " >> "$FILE"
fi

if [[ $build_maven == true ]]; then
  echo "Building JAR"
  mvn -f ../../examples/GCStoGCS/ clean package
  echo "
  RUN mkdir /opt/flink/usrlib
  ADD examples/GCStoGCS/target/gmf-examples.jar /opt/flink/usrlib/gmf-examples.jar
  RUN chown -R flink:flink /opt/flink
  " >> "$FILE"
fi

IMAGE_FULL_NAME=$region-docker.pkg.dev/$PROJECT/$repo_name/$image_name

cd ../..
if [[ $use_cloud_build == true ]]; then
  echo "Building image using Cloud Build at $FILE"
  gcloud builds submit --region="$region" --tag "$IMAGE_FULL_NAME":"$image_tag"
else
  echo "Building image using Docker at $FILE"
  docker build . -t "$IMAGE_FULL_NAME":"$image_tag"
  echo "Pushing image using Docker"
  docker push "$IMAGE_FULL_NAME":"$image_tag"
fi