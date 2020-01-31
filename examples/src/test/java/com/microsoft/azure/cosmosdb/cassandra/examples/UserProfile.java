package com.microsoft.azure.cosmosdb.cassandra.examples;

import java.io.IOException;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.microsoft.azure.cosmosdb.cassandra.repository.UserRepository;
import com.microsoft.azure.cosmosdb.cassandra.util.CassandraUtils;
import com.microsoft.azure.cosmosdb.cassandra.util.Configurations;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.github.javafaker.Faker;
import com.microsoft.azure.cosmos.cassandra.CosmosRetryPolicy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static com.datastax.driver.core.ConsistencyLevel.QUORUM;

/**
 * Example class which will demonstrate following operations on Cassandra
 * Database on CosmosDB - Create Keyspace - Create Table - Insert Rows - Select
 * all data from a table - Select a row from a table
 */
public class UserProfile {
    private static final Logger LOGGER = LoggerFactory.getLogger(UserProfile.class);

    private static Random random = new Random();
    private static Configurations config = new Configurations();
    private static final ConsistencyLevel CONSISTENCY_LEVEL = QUORUM;
    private static final int FIXED_BACK_OFF_TIME = 5000;
    private static final int GROWING_BACK_OFF_TIME = 1000;
    private static final int MAX_RETRY_COUNT = 20;

    public static final int NUMBER_OF_THREADS = 30;
    public static final int NUMBER_OF_WRITES_PER_THREAD = 10;
    private static final int TIMEOUT = 10;
    CosmosRetryPolicy retryPolicy = new CosmosRetryPolicy(MAX_RETRY_COUNT, FIXED_BACK_OFF_TIME, GROWING_BACK_OFF_TIME);
    AtomicInteger recordcount = new AtomicInteger(0);
    AtomicInteger exceptioncount = new AtomicInteger(0);
    AtomicInteger ratelimitcount = new AtomicInteger(0);
    AtomicInteger totalRetries = new AtomicInteger(0);

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
        Session cassandraSessionWithRetry = utils.getSession(CONTACT_POINTS, PORT, u.retryPolicy);
        UserRepository repository = new UserRepository(cassandraSessionWithRetry);
        try {
            // Create keyspace and table in cassandra database
            repository.deleteTable("DROP KEYSPACE IF EXISTS uprofile");
            Thread.sleep(2000);
            repository.createKeyspace("CREATE KEYSPACE uprofile WITH REPLICATION = { 'class' : 'NetworkTopologyStrategy', 'datacenter1' : 1 }");
            Thread.sleep(5000);
            repository.createTable("CREATE TABLE uprofile.user (user_id text PRIMARY KEY, user_name text, user_bcity text)");
            Thread.sleep(5000);
            LOGGER.info("inserting records....");
            // Insert rows into user table
            u.loadTest(repository, u, "INSERT INTO  uprofile.user (user_id, user_name , user_bcity) VALUES (?,?,?)",
                    NUMBER_OF_THREADS, NUMBER_OF_WRITES_PER_THREAD, 5);
        } catch (Exception e) {
            System.out.println("Exception " + e);
        } finally {
            repository.selectUserCount("SELECT COUNT(*) as coun FROM uprofile.user");
            utils.close();
            System.out.println("count of records attempted: " + u.recordcount);
        }
    }

    public static Random getRandom() {
        return random;
    }

    public void loadTest(UserRepository repository, UserProfile u, String preparedStatementString, int noOfThreads,
            int noOfWritesPerThread, int noOfRetries) throws InterruptedException {
        PreparedStatement preparedStatement = repository.prepareInsertStatement(preparedStatementString);
        preparedStatement.setIdempotent(true);
        Faker faker = new Faker();
        ExecutorService es = Executors.newCachedThreadPool();
        for (int i = 1; i <= noOfThreads; i++) {
            Runnable task = () -> {
                ;
                for (int j = 1; j <= noOfWritesPerThread; j++) {
                    // System.out.println("request: "+j);
                    UUID guid = java.util.UUID.randomUUID();
                    try {
                        String name = faker.name().lastName();
                        String city = faker.address().city();
                        u.recordcount.incrementAndGet();
                        repository.insertUser(preparedStatement, guid.toString(), name, city);

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
            System.out.println("Finished executing all threads.");
        }

    }
}
