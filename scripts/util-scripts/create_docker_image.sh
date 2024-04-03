#!/bin/bash
set -e

# Optional
region="us-central1"
project_id=
install_plugins=true
flink_version="1.18.1"
repo_name="gmf-repo"
image_name="flink-image"
image_tag="latest"
use_cloud_build=false
build_maven=false

while getopts ":r:p:t:n:m:xb:" opt; do 
  case $opt in
    r) region=$OPTARG ;;
    b) use_cloud_build=true;;
    p) project_id=$OPTARG ;;
    t) image_tag=$OPTARG ;;
    n) repo_name=$OPTARG ;;
    m) image_name=$OPTARG ;;
    x) build_maven=true ;;

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

# Create Dockerfile
SCRIPT_PATH=$(dirname "$0")

cd $SCRIPT_PATH
FILE="../Dockerfile"
echo ">> Creating Dockerfile"
GCS_PLUGIN_URL=https://repo1.maven.org/maven2/org/apache/flink/flink-gs-fs-hadoop/$flink_version/flink-gs-fs-hadoop-$flink_version.jar
echo "FROM flink:$flink_version" > "$FILE"


if [[ $install_plugins==true ]]; then
  echo ">> Adding plugins to Dockerfile"
  echo "
  RUN mkdir /opt/flink/plugins/gs-fs-hadoop/
  RUN wget -q -O /opt/flink/plugins/gs-fs-hadoop/flink-gs-fs-hadoop-$flink_version.jar ${GCS_PLUGIN_URL}
  " >> "$FILE"
fi

if [[ $build_maven==true ]]; then
  echo ">> Building JAR"
  mvn -f ../flink/ clean package
  echo "
  RUN mkdir /opt/flink/usrlib
  ADD flink/target/gmf-examples.jar /opt/flink/usrlib/gmf-examples.jar
  RUN chown -R flink:flink /opt/flink
  " >> "$FILE"
fi

IMAGE_FULL_NAME=$region-docker.pkg.dev/$PROJECT/$repo_name/$image_name

cd ..
if [[ $use_cloud_build==true ]]; then
  echo ">> Building image using Cloud Build"
  gcloud builds submit --region="$region" --tag "$IMAGE_FULL_NAME":"$image_tag"
else
  echo ">> Building image using Docker at $FILE"
  docker build . -t "$IMAGE_FULL_NAME" --file "$FILE"
  echo ">> Pushing image using Docker"
  docker push "$IMAGE_FULL_NAME":"$image_tag"
fi