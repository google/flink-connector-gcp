This is a collection of scripts to create a Google Manage Flink deployment and start a Flink job

# Scripts

The main script `run_flink_job` compiles `flink-examples-gcs`, uploads it to a GCS bucket,
creates a Flink Deployment and starts a job.

There are scripts for each in folder `util-scripts`.

# Parameters

```
    # Required
    -j GCS path to the JAR. Can be created with -J under that path.

    # Optional 
    -a Arguments for the Flink job
    -d Deployment name
    -r Region, defaults to `us-central1`
    -p Project ID
    -v VPC for the deployment
    -n Subnetwork for the deployment
    -s Max slots of the deployment, defaults to 10
    -t Service account to impersonate
    -e Entry class name for the Flink job
    -m Min parallelism for the Flink job, defaults to 1
    -M Max parallelsim for the Flink job, defaults to 10
    -k List of Managed Kafka Clusters, comma separated

    # Booleans
    -P Compiles `flink-examples-gcp` and uploads the JAR to the `jar_uri`
    -D Creates a new deployment
```

# Examples

## Basic End-to-End on demand

The simplest command only needs the name of the path to the JAR (`-j`):

```
bash run_flink_job.sh -j gs://my-bucket/my-jar.jar
```

## End-to-End on existing deployment

If you have an existing deployment and want to launch a Flink job there:

```
bash run_flink_job.sh -j gs://my-bucket/my-jar.jar -d my-deployment
```

## End-to-End with new deployment and new JAR

If you need to create a deployment and generate the JAR:

```
bash run_flink_job.sh -j gs://my-bucket/my-job.jar -J -d my-deployment -D
```

## Create an on demand job with custom class, max parallelism, Managed Kafka Clusters and arguments

Creates a GMF cluster with a non-default Service Account and builds a Docker image using Cloud Build

```
bash run_flink_job.sh -j gs://my-bucket/my-job.jar -e path.to.entry.Class -M 1 -a arg1=value1,arg2=value2 -k projects/my-project/locations/us-central1/clusters/my-cluster,projects/my-project/locations/us-central1/clusters/my-cluster2
```