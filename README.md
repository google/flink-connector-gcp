# Apache Flink GCP Connectors

## Examples

You can find multiple examples that use GCP connectors in `flink-examples-gcp`. These are:

DataStream API:
- Google Cloud Storage Load Generator
- Google Cloud Storage to Google Cloud Storage WordCount
- Google Cloud Storage to Google Cloud Storage WordCount (Batch)
- Apache Kafka for BigQuery Load Generator
- Apache Kafka for BigQuery to BigQuery
- Apache Kafka for BigQuery to Apache Kafka for BigQuery WordCount
- PubSub Load Generator
- PubSub to BigQuery WordCount

Table API:
- Google Cloud Storage to Google Cloud Storage
- Apache Kafka for BigQuery to Apache Kafka for BigQuery

Python:
-  Google Cloud Storage to Google Cloud Storage WordCount

## Building the Project

This project uses the Maven wrapper for building. You don't need to install Maven separately.

To build the project and package all examples with their dependencies into an uber JAR, follow these steps:

1. **First, clone the repository.**

    ```bash
    git clone https://github.com/google/flink-connector-gcp.git
    cd flink-connector-gcp
    ```

2. **Run the following command:**

   ```bash
   ./mvnw clean package
   ```

   This command will:

    - `clean`: Delete any previous build artifacts.
    - `package`: Compile the code, run tests, and package the project. In this case, it will create an uber JAR containing all examples and their dependencies.

3. **The uber JAR will be located at:**

   ```
   flink-examples-gcp/target/flink-examples-gcp-0.0.0-shaded.jar
   ```

You can now use this JAR to run the examples.

## Scripts

Contains scripts to create a BigQuery Engine for Apache Flink deployment and start a Flink job.

## Tools

Collections of handy tools for development.

These contain:
- Kafka CLI
- Maven checkstyle

## Source Code Headers

Every file containing source code must include copyright and license
information. This includes any JS/CSS files that you might be serving out to
browsers. (This is to help well-intentioned people avoid accidental copying that
doesn't comply with the license.)

Apache header:

    Copyright 2022 Google LLC

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
