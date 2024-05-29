### Building the Docker Image

1. **Navigate:** Open your terminal and go to the directory containing your Dockerfile.
2. **Build:** Run the following command, replacing `<your-docker-hub-username>` with your actual Docker Hub username (or leave it out if you're not using Docker Hub):

   ```bash
   docker build -t <your-gcr>/kafka-cli .
   ```

### Applying the Kubernetes Configuration

1. **Ensure Prerequisites:**
    * Make sure you have a Kubernetes cluster running.
    * Have the `kafka-cli.yaml` file in your current directory.
    * If connecting to GMK, the `gmk-sasl-plain-login` secret should be available in your cluster.
   
2. **Apply:** Run the following command to create the necessary Kubernetes resources:

   ```bash
   kubectl apply -f kafka-cli.yaml
   ```

### Running the Backlog Command (within Kubernetes)

2. **Execute Command:** 

   ```bash
   kubectl exec -it kafka-cli -- kafka-consumer-groups.sh --command-config /opt/kafka-config.properties --bootstrap-server bootstrap.ellading-gmk-prod.us-central1.managedkafka.managed-flink-shared-dev.cloud.goog:9092 --describe --group ellading-group3
   ```

**Important Note:** Remember to replace placeholders with your actual values.