package com.microsoft.azure.cosmosdb.cassandra.examples;

import java.io.IOException;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import com.microsoft.azure.cosmosdb.cassandra.repository.UserRepository;
import com.microsoft.azure.cosmosdb.cassandra.util.CassandraUtils;
import com.microsoft.azure.cosmosdb.cassandra.util.Configurations;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.github.javafaker.Faker;
import com.microsoft.azure.cosmos.cassandra.CosmosLoadBalancingPolicy;
import com.microsoft.azure.cosmos.cassandra.CosmosRetryPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Example class which will demonstrate handling rate limiting using retry
 * policy, and client side load balancing using Cosmos DB Extensions for Java
 */
public class UserProfile {
    private static final Logger LOGGER = LoggerFactory.getLogger(UserProfile.class);
    private static Random random = new Random();
    private static Configurations config = new Configurations();
    private static final int FIXED_BACK_OFF_TIME = 5000;
    private static final int GROWING_BACK_OFF_TIME = 1000;
    private static final int MAX_RETRY_COUNT = 20;
    public static final int NUMBER_OF_THREADS = 40;
    public static final int NUMBER_OF_WRITES_PER_THREAD = 5;
    CosmosRetryPolicy retryPolicy = CosmosRetryPolicy.builder().withFixedBackOffTimeInMillis(FIXED_BACK_OFF_TIME)
            .withGrowingBackOffTimeInMillis(GROWING_BACK_OFF_TIME).withMaxRetryCount(MAX_RETRY_COUNT).build();

    CosmosLoadBalancingPolicy loadBalancingPolicy1 = CosmosLoadBalancingPolicy.builder()
            .withGlobalEndpoint(CONTACT_POINTS[0]).build();
    CosmosLoadBalancingPolicy loadBalancingPolicy2 = CosmosLoadBalancingPolicy.builder().withWriteDC("West US")
            .withReadDC("West US").build();
    AtomicInteger recordcount = new AtomicInteger(0);
    AtomicInteger exceptioncount = new AtomicInteger(0);
    AtomicInteger ratelimitcount = new AtomicInteger(0);
    AtomicInteger totalRetries = new AtomicInteger(0);
    AtomicLong insertCount = new AtomicLong(0);
    AtomicLong totalLatency = new AtomicLong(0);
    AtomicLong averageLatency = new AtomicLong(0);
    Boolean loadBalanceRegions = false;

    private static int PORT;
    static {
        String value;
        try {
            value = config.getProperty("cassandra_port");
            PORT = Short.parseShort(value);
        } catch (IOException e) {
            e.printStackTrace();
        }
    };

    private static String[] CONTACT_POINTS;
    static {
        String value;
        try {
            value = config.getProperty("cassandra_host");
            CONTACT_POINTS = new String[] { value };
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] s) throws Exception {

        CassandraUtils utils = new CassandraUtils();
        UserProfile u = new UserProfile();
        Session cassandraSessionWithRetry1 = utils.getSession(CONTACT_POINTS, PORT, u.retryPolicy,
                u.loadBalancingPolicy1);
        Session cassandraSessionWithRetry2 = utils.getSession(CONTACT_POINTS, PORT, u.retryPolicy,
                u.loadBalancingPolicy2);
        UserRepository region1 = new UserRepository(cassandraSessionWithRetry1);
        UserRepository region2 = new UserRepository(cassandraSessionWithRetry2);
        try {
            // Create keyspace and table in cassandra database
            region1.deleteTable("DROP KEYSPACE IF EXISTS uprofile");
            Thread.sleep(5000);
            region1.createKeyspace(
                    "CREATE KEYSPACE uprofile WITH REPLICATION = { 'class' : 'NetworkTopologyStrategy', 'datacenter1' : 1 }");
            Thread.sleep(5000);
            region1.createTable(
                    "CREATE TABLE uprofile.user (user_id text PRIMARY KEY, user_name text, user_bcity text)");
            Thread.sleep(5000);
            LOGGER.info("inserting records....");
            // Insert rows into user table
            u.loadTest(region1, region2, u,
                    "INSERT INTO  uprofile.user (user_id, user_name , user_bcity) VALUES (?,?,?)", NUMBER_OF_THREADS,
                    NUMBER_OF_WRITES_PER_THREAD, 5);
        } catch (Exception e) {
            System.out.println("Exception " + e);
        } finally {
            region1.selectUserCount("SELECT COUNT(*) as coun FROM uprofile.user");
            utils.close();
            System.out.println("count of records attempted: " + u.recordcount);
        }
    }

    public static Random getRandom() {
        return random;
    }

    public void loadTest(UserRepository region1, UserRepository region2, UserProfile u, String preparedStatementString,
            int noOfThreads, int noOfWritesPerThread, int noOfRetries) throws InterruptedException {
        PreparedStatement preparedStatement1 = region1.prepareInsertStatement(preparedStatementString);
        PreparedStatement preparedStatement2 = region2.prepareInsertStatement(preparedStatementString);
        preparedStatement1.setIdempotent(true);
        preparedStatement2.setIdempotent(true);
        Faker faker = new Faker();
        ExecutorService es = Executors.newCachedThreadPool();

        for (int i = 1; i <= noOfThreads; i++) {
            Runnable task = () -> {
                ;
                Random rand = new Random();
                int n = rand.nextInt(2);
                for (int j = 1; j <= noOfWritesPerThread; j++) {
                    UUID guid = java.util.UUID.randomUUID();
                    try {
                        String name = faker.name().lastName();
                        String city = faker.address().city();
                        u.recordcount.incrementAndGet();
                        long startTime = System.currentTimeMillis();

                        // load balancing across regions
                        if (loadBalanceRegions == true) {
                            // approx 50% of the time we will go to region 1
                            if (n == 1) {
                                System.out.print("writing to region 1! \n");
                                region1.insertUser(preparedStatement1, guid.toString(), name, city);
                            }
                            // approx 50% of the time we will go to region 2
                            else {
                                System.out.print("writing to region 2! \n");
                                region2.insertUser(preparedStatement2, guid.toString(), name, city);
                            }
                        } else {
                            region1.insertUser(preparedStatement1, guid.toString(), name, city);
                        }
                        long endTime = System.currentTimeMillis();
                        long duration = (endTime - startTime);
                        System.out.print("insert duration time millis: " + duration + "\n");
                        totalLatency.getAndAdd(duration);
                        u.insertCount.incrementAndGet();
                    } catch (Exception e) {
                        u.exceptioncount.incrementAndGet();
                        System.out.println("Exception: " + e.getCause());
                    }
                }
            };
            es.execute(task);
        }
        es.shutdown();
        boolean finished = es.awaitTermination(5, TimeUnit.MINUTES);
        if (finished) {
            long latency = (totalLatency.get() / insertCount.get());
            System.out.print("Average Latency: " + latency + "\n");
            System.out.println("Finished executing all threads.");
        }

    }
}
