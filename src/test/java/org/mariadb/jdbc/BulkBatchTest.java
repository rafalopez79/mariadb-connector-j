package org.mariadb.jdbc;

import org.junit.Assert;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class BulkBatchTest extends BaseTest {

    /**
     * Initialisation.
     *
     * @throws SQLException exception
     */
    @BeforeClass()
    public static void initClass() throws SQLException {
        createTable("BulkBatchTest", "id int NOT NULL AUTO_INCREMENT, execute_number TINYINT(1), incr int, PRIMARY KEY (id)");
        createTable("BulkBatchTest2", "id int NOT NULL AUTO_INCREMENT, execute_number TINYINT(1), incr int, PRIMARY KEY (id)");
    }

    @Test
    public void hugeBulkBatchTest() throws Throwable {
        //only for 10.2 servers, because will take too much time if not.
        Assume.assumeTrue(getProtocolFromConnection(sharedConnection).isSupportArrayBinding());

        Statement stmt = sharedConnection.createStatement();
        PreparedStatement preparedStatement0 = prepareBatch("BulkBatchTest", sharedConnection, 10000000, false);
        preparedStatement0.executeBatch();
        ResultSet generatedKeys0 = preparedStatement0.getGeneratedKeys();

        stmt.setFetchSize(1000);
        ResultSet realResultSet = stmt.executeQuery("SELECT * FROM BulkBatchTest");

        for (int i = 0; i < 10000000; i++) {
            Assert.assertTrue(realResultSet.next());
            Assert.assertTrue(generatedKeys0.next());
            Assert.assertEquals(realResultSet.getInt(1), generatedKeys0.getInt(1));
        }
        Assert.assertFalse(generatedKeys0.next());
        Assert.assertFalse(realResultSet.next());
    }

    @Test
    public void bulkBatchTest2Bulk() throws Throwable {
        //only for 10.2 servers, because will take too much time if not.
        Assume.assumeTrue(getProtocolFromConnection(sharedConnection).isSupportArrayBinding());

        try (Connection connection2 = setConnection("")) {
            ExecutorService exec = Executors.newFixedThreadPool(2);

            PreparedStatement preparedStatement0 = prepareBatch("BulkBatchTest2", sharedConnection, 100000, false);
            PreparedStatement preparedStatement1 = prepareBatch("BulkBatchTest2", connection2, 100000, true);

            //check blacklist shared
            exec.execute(new BulkInsertThread(preparedStatement0));
            exec.execute(new BulkInsertThread(preparedStatement1));

            //wait for thread endings
            exec.awaitTermination(1, TimeUnit.MINUTES);
            exec.shutdown();

            ResultSet generatedKeys0 = preparedStatement0.getGeneratedKeys();
            ResultSet generatedKeys1 = preparedStatement1.getGeneratedKeys();

            Statement stmt = sharedConnection.createStatement();
            stmt.setFetchSize(1000);
            ResultSet realResultSet = stmt.executeQuery("SELECT * FROM BulkBatchTest2");
            for (int i = 0; i < 200000; i++) {
                Assert.assertTrue(realResultSet.next());
                if (realResultSet.getInt(2) == 1) {
                    Assert.assertTrue(generatedKeys1.next());
                    Assert.assertEquals(realResultSet.getInt(1), generatedKeys1.getInt(1));
                } else {
                    Assert.assertTrue(generatedKeys0.next());
                    Assert.assertEquals(realResultSet.getInt(1), generatedKeys0.getInt(1));
                }
            }
            Assert.assertFalse(generatedKeys0.next());
            Assert.assertFalse(generatedKeys1.next());
            Assert.assertFalse(realResultSet.next());
        }
    }

    private PreparedStatement prepareBatch(String tableName, Connection connection, int numberOfRecord, boolean executeNumber) throws SQLException {
        PreparedStatement preparedStatement = connection.prepareStatement("INSERT INTO " + tableName
                + "(execute_number, incr) VALUES (" + (executeNumber ? "1" : "0") + ", ?)");
        for (int i = 0; i < numberOfRecord; i++) {
            preparedStatement.setInt(1, i);
            preparedStatement.addBatch();
        }
        return preparedStatement;
    }

    private static class BulkInsertThread implements Runnable {
        private final PreparedStatement stmt;

        public BulkInsertThread(PreparedStatement stmt) {
            this.stmt = stmt;
        }

        @Override
        public void run() {
            try {
                stmt.executeBatch();
            } catch (SQLException e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
        }
    }

}
