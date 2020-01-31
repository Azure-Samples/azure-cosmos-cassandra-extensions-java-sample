package com.microsoft.azure.cosmosdb.cassandra.repository;

import java.util.List;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;

import static com.datastax.driver.core.BatchStatement.Type.UNLOGGED;
import static com.datastax.driver.core.ConsistencyLevel.QUORUM;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class gives implementations of create, delete table on Cassandra database
 * Insert & select data from the table
 */
public class UserRepository {
    private static final Logger LOGGER = LoggerFactory.getLogger(UserRepository.class);
    private Session session;
    private static final ConsistencyLevel CONSISTENCY_LEVEL = QUORUM;

    public UserRepository(Session session) {
        this.session = session;
    }

    /**
     * Create keyspace uprofile in cassandra DB
     */
    public void createKeyspace(String queryString) {
        final String query = queryString;
        session.execute(query);
        LOGGER.info("Executed query: "+queryString);
    }

    /**
     * Create user table in cassandra DB
     */
    public void createTable(String queryString) {
        final String query = queryString;
        session.execute(query);
        LOGGER.info("Executed query: "+queryString);
    }

    /**
     * Select all rows from user table
     */
    public void selectAllUsers() {

        final String query = "SELECT * FROM uprofile.user";
        List<Row> rows = session.execute(query).all();

        for (Row row : rows) {
            LOGGER.info("Obtained row: {} | {} | {} ", row.getInt("user_id"), row.getString("user_name"), row.getString("user_bcity"));
        }
    }

    /**
     * Select a row from user table
     *
     * @param id user_id
     */
    public void selectUser(int id) {
        final String query = "SELECT * FROM uprofile.user where user_id = 3";
        Row row = session.execute(query).one();

        LOGGER.info("Obtained row: {} | {} | {} ", row.getInt("user_id"), row.getString("user_name"), row.getString("user_bcity"));
    }

    /**
     * Delete user table.
     */
    public void deleteTable(String queryString) {
        final String query = queryString;
        session.execute(query);
    }

    /**
     * Insert a row into user table
     *
     * @param id   user_id
     * @param name user_name
     * @param city user_bcity
     */
    public void insertUser(PreparedStatement statement, String id, String name, String city) {
        BoundStatement boundStatement = new BoundStatement(statement);
        session.execute(boundStatement.bind(id, name, city));
    }

    public void simpleInsertUser(Statement statement) {
        BatchStatement batch = new BatchStatement(UNLOGGED);
        batch.add(statement);
        batch.setConsistencyLevel(CONSISTENCY_LEVEL);
        session.execute(batch);
    }

    public void selectUserCount(String queryString) {
        final String query = queryString;
        Row row = session.execute(query).one();
        //LOGGER.info("count of users: " +row.getLong(0), row.getLong(0));
        System.out.println("count of users in table: " +row.getLong(0));
    } 



    /**
     * Create a PrepareStatement to insert a row to user table
     *
     * @return PreparedStatement
     */
    public PreparedStatement prepareInsertStatement(String queryString) {
        final String insertStatement = queryString;
        return session.prepare(insertStatement);
    }
}