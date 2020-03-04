---
page_type: sample
languages:
- java
products:
- azure
description: "Azure Cosmos DB is a globally distributed multi-model database. One of the supported APIs is the Cassandra API"
urlFragment: azure-cosmos-db-cassandra-java-retry-sample
---

# Handling rate limited requests in the Azure Cosmos DB API for Cassandra
Azure Cosmos DB is a globally distributed multi-model database. One of the supported APIs is the Cassandra API. This sample illustrates how to handle rate limited requests. These are also known as [429 errors](https://docs.microsoft.com/rest/api/cosmos-db/http-status-codes-for-cosmosdb), and are returned when the consumed throughput exceeds the number of [Request Units](https://docs.microsoft.com/azure/cosmos-db/request-units) that have been provisioned for the service. In this code sample, we implement the [Azure Cosmos DB extension for Cassandra Retry Policy](https://github.com/Azure/azure-cosmos-cassandra-extensions) (see pom.xml file). 

The retry policy handles errors such as OverLoadedError (which may occur due to rate limiting), and parses the exception message to use RetryAfterMs field provided from the server as the back-off duration for retries. If RetryAfterMs is not available, it defaults to an exponential growing back-off scheme. In this case the time between retries is increased by a growing back off time (default: 1000 ms) on each retry, unless maxRetryCount is -1, in which case it backs off with a fixed duration.  It is important to handle rate limiting in Azure Cosmos DB to prevent errors when [provisioned throughput](https://docs.microsoft.com/azure/cosmos-db/how-to-provision-container-throughput) has been exhausted. 

## Prerequisites
* Before you can run this sample, you must have the following prerequisites:
    * An active Azure Cassandra API account - If you don't have an account, refer to the [Create Cassandra API account](https://aka.ms/cassapijavaqs).
    * [Java Development Kit (JDK) 1.8+](http://www.oracle.com/technetwork/java/javase/downloads/jdk8-downloads-2133151.html)
        * On Ubuntu, run `apt-get install default-jdk` to install the JDK.
    * Be sure to set the JAVA_HOME environment variable to point to the folder where the JDK is installed.
    * [Download](http://maven.apache.org/download.cgi) and [install](http://maven.apache.org/install.html) a [Maven](http://maven.apache.org/) binary archive
        * On Ubuntu, you can run `apt-get install maven` to install Maven.
    * [Git](https://www.git-scm.com/)
        * On Ubuntu, you can run `sudo apt-get install git` to install Git.

## Running this sample
1. Clone this repository using `git clone git@github.com:Azure-Samples/azure-cosmos-db-cassandra-java-retry-sample.git cosmosdb`.

2. Change directories to the repo using `cd cosmosdb/java-examples`

3. Next, substitute the Cassandra host, username, password  `java-examples\src\test\resources\config.properties` with your Cosmos DB account's values from connectionstring panel of the portal.

    ```
    cassandra_host=<FILLME>
    cassandra_username=<FILLME>
    cassandra_password=<FILLME>
    ssl_keystore_file_path=<FILLME>
    ssl_keystore_password=<FILLME>
    ```
    If ssl_keystore_file_path is not given in config.properties, then by default <JAVA_HOME>/jre/lib/security/cacerts will be used. If ssl_keystore_password is not given in config.properties, then the default password 'changeit' will be used

5. Run `mvn clean install` from java-examples folder to build the project. This will generate cosmosdb-cassandra-examples.jar under target folder.
 
6. Run `java -cp target/cosmosdb-cassandra-examples.jar com.microsoft.azure.cosmosdb.cassandra.examples.UserProfile` in a terminal to start your java application. The output will include a number of "overloaded" (rate limited) requests, the insert duration times, average latency, the number of users present in the table after the load test, and the number of user inserts that were attempted. Users in table and records attempted should be identical since rate limits have been successfully handled and retried. Notice that although requests are all successful, you may see significant average latency due to requests being retried after rate limiting:

   ![Console output](./media/output.png)

    If you do not see overloaded errors, you can increase the number of threads in UserProfile.java in order to force rate limiting: 

    ```java
        public static final int NUMBER_OF_THREADS = 40;
    ```

7. In a real world scenario, you may wish to take steps to increase the provisioned throughput when the system is experiencing rate limiting. Note that you can do this programmatically in the Azure Cosmos DB API for Cassandra by executing [ALTER commends in CQL](https://docs.microsoft.com/azure/cosmos-db/cassandra-support#keyspace-and-table-options). In production, you should handle 429 errors in a similar fashion to this sample, and monitor the system, increasing throughput if 429 errors are being recorded by the system. You can monitor whether requests are exceeding provisioned capacity using Azure portal metrics:

   ![Console output](./media/metrics.png)

    You can try increasing the provisioned RUs in the Cassandra Keyspace or table to see how this will improve latencies. You can consult our article on [elastic scale](https://docs.microsoft.com/en-us/azure/cosmos-db/manage-scale-cassandra) to understand the different methods for provisioning throughput in Cassandra API. 

8. One alternative to increasing RU provisioning for mitigating rate limiting, which might be more useful in scenarios where there is a highly asymmetrical distribution of throughout between regions, is to load balance between regions on the client. The load balancing policy implemented in this sample provides the mechanism to do this by allowing you to select a read or write region:

    ```java
        CosmosLoadBalancingPolicy loadBalancingPolicy1 = CosmosLoadBalancingPolicy.builder().withGlobalEndpoint(CONTACT_POINTS[0]).build();
        CosmosLoadBalancingPolicy loadBalancingPolicy2 = CosmosLoadBalancingPolicy.builder().withWriteDC("West US").withReadDC("West US").build();
    ```
    To test this out, set the `loadBalanceRegions` variable to true:

    ```java
    Boolean loadBalanceRegions = true;
    ```
    Note: when running this, you should ensure that you have two regions, one in West US (or you can change the above to a region of your choice), and [multi-master writes configured](https://docs.microsoft.com/en-us/azure/cosmos-db/how-to-multi-master). When you run the test again with load balancing configured, you should see requests being written to different regions, with latencies reduced, without having to increase provisioned throughput (RUs):

    ![Console output](./media/loadbalancingoutput.png)

    Note: when writing data to Cassandra, you should ensure that you account for [query idempotence](https://docs.datastax.com/en/developer/java-driver/3.0/manual/idempotence/), and the relevant rules for [retries](https://docs.datastax.com/en/developer/java-driver/3.0/manual/retries/#retries-and-idempotence). You should perform sufficient load testing to ensure that the implementation meets your requirements.

## About the code
The code included in this sample is a load test to simulate a scenario where Cosmos DB will rate limit requests (return a 429 error) because there are too many requests for the [provisioned throughput](https://docs.microsoft.com/azure/cosmos-db/how-to-provision-container-throughput) in the service. In this sample, we create a Keyspace and table, and run a multi-threaded process that will insert users concurrently into the user table. To help generate random data for users, we use a java library called "javafaker", which is included in the build dependencies. The loadTest() will eventually exhaust the provisioned Keyspace RU allocation (default is 400RUs). 



## Review the code

You can review the following files: `src/test/java/com/microsoft/azure/cosmosdb/cassandra/util/CassandraUtils.java` and `src/test/java/com/microsoft/azure/cosmosdb/cassandra/repository/UserRepository.java` to understand how sessions and the retry and load balancing policies are being added. You should also review the main class file  `src/test/java/com/microsoft/azure/cosmosdb/cassandra/examples/UserProfile.java` where the load test is created and run. We first create the retry policy in UserProfile.java:

```java
    CosmosRetryPolicy retryPolicy = new CosmosRetryPolicy(MAX_RETRY_COUNT, FIXED_BACK_OFF_TIME, GROWING_BACK_OFF_TIME);
```
We then create two load balancing policies. One with a global endpoint, and one with a specified region:

```java
    CosmosLoadBalancingPolicy loadBalancingPolicy1 = CosmosLoadBalancingPolicy.builder().withGlobalEndpoint(CONTACT_POINTS[0]).build();
    CosmosLoadBalancingPolicy loadBalancingPolicy2 = CosmosLoadBalancingPolicy.builder().withWriteDC("West US").withReadDC("West US").build();
```

We can then pass the policies to `getSession()` (see `CassandraUtils.java` - in this case we create two sessions in order to toggle between running our load test while load balancing across regions, or a single region - but typically you will create 1 session):

```java
    CassandraUtils utils = new CassandraUtils();
    UserProfile u = new UserProfile();
    Session cassandraSessionWithRetry1 = utils.getSession(CONTACT_POINTS, PORT, u.retryPolicy, u.loadBalancingPolicy1);
    Session cassandraSessionWithRetry2 = utils.getSession(CONTACT_POINTS, PORT, u.retryPolicy, u.loadBalancingPolicy2);
```

Please note that timeout limits are set in `getSession()`:

```java
        SocketOptions options = new SocketOptions();
        options.setConnectTimeoutMillis(30000);
        options.setReadTimeoutMillis(30000);

```

You should also consult the [Azure Cosmos DB extension for Cassandra Retry Policy](https://github.com/Azure/azure-cosmos-cassandra-extensions) if you want to understand how retry policy is implemented.

## More information

- [Azure Cosmos DB](https://docs.microsoft.com/azure/cosmos-db/introduction)
- [Java driver Source](https://github.com/datastax/java-driver)
- [Java driver Documentation](https://docs.datastax.com/en/developer/java-driver/)
