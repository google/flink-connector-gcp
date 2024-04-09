This is a collection of scripts to create a Google Manage Flink cluster and start a Flink Deployment

# Scripts

The main script `run_flink` creates a GMF cluster, adds needed settings, builds a Docker image and
starts a Flink Deployment.

There are scripts for each in folder `util-scripts`.

# Parameters

```
    # Required
    -c cluster name

    # Optional
    -a Arguments for the Flink Deployment
    -e Entry class name for the Flink Deployment
    -d Name of the deployment
    -f Flink version, defaults to `1.18.1`
    -i Full image name, used if you don't want to build the image and pass your own
    -j Jar URI
    -m Docker image name
    -n Repository for docker image
    -p Project ID
    -q Parallelism for the Flink Deployment,  defaults to 1
    -r Region, defaults to `us-central1`
    -s Service Account used to authentificate the Flink Deployment
    -t Tag of the Docker image

     # Booleans
    -B Uses Cloud Build to build the image
    -C Skips cluster creation
    -D Skips Docker image creation
    -F Skips Flink Deployment
    -M Skips compiling the example pipeline
    -P Skips installing GCS Plugin
    -W Skips Workload Indentity Federation annotation
```

# Examples

## Basic End-to-end

The simplest command only needs the name of the cluster (`-c`), it will use the default Project and
Compute Engine Service Account (`service-<PROJECT NUMBER>)-compute@developer.gserviceaccount.com`).

```
bash run_flink.sh -c my-cluster
```

## End-to-End with existing cluster

If you have an existing cluster and want to launch a Flink Deployment using Cloud Build. This will install CERTs, `flink-kubernetes-operator` and annotate the Service Account with [Workloads Identity Federation](https://cloud.google.com/kubernetes-engine/docs/how-to/workload-identity). The script will identify the cluster already exists:

```
bash run_flink.sh -c my-existing-cluster
```

If the cluster already has all the needed settings, you can fully skip the creation:

```
bash run_flink.sh -c my-existing-cluster -C
```

## End-2-end using an existing image

If you have an existising Docker image and want to use it in a new cluster, you can pass it with parameter `-f`.
The image has to be available for GMF and the script will skip the creation of a new image:

```
bash run_flink.sh -c my-cluster -f us-central1-docker.pkg.dev/<PROJECT>/gmf-repo/flink-image:latest -e MyClass -a "--arg1=value1 --arg2 value2" -j "local:///opt/flink/usrlib/my-job.jar"
```


## Create cluster with Service Account, using Cloud Build without Flink Deployment

Creates a GMF cluster with a non-default Service Account and builds a Docker image using Cloud Build

```
bash run_flink.sh -c my-cluster -s <PROJECT_NUMBER>-compute@developer.gserviceaccount.com -B -F
```