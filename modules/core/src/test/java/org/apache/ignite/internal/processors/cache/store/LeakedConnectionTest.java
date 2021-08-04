package org.apache.ignite.internal.processors.cache.store;

import com.mysql.cj.jdbc.MysqlDataSource;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.io.PrintWriter;
import java.io.Serializable;
import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.sql.Types;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.logging.Logger;
import javax.cache.configuration.Factory;
import javax.sql.DataSource;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheEntryProcessor;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.cache.store.jdbc.CacheAbstractJdbcStore;
import org.apache.ignite.cache.store.jdbc.CacheJdbcPojoStoreFactory;
import org.apache.ignite.cache.store.jdbc.JdbcType;
import org.apache.ignite.cache.store.jdbc.JdbcTypeField;
import org.apache.ignite.cache.store.jdbc.dialect.MySQLDialect;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.multicast.TcpDiscoveryMulticastIpFinder;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Several test cases to show flaws of JDBC connection open/close logic when we use external storage (e.g. write-behind).
 * We use data source and JDBC connection wrappers that would count every connection opening and closing, so in the end
 * of test we can compare whether connection opening number is equal to connection release number.
 * <br/><br/>
 *
 * Preparation:
 *
 * 1. Use following docker-compose config for bootstrapping mysql DB:
 * <pre>
 * version: '3.1'
 *
 * services:
 *
 *   db:
 *     image: mysql
 *     command: --default-authentication-plugin=mysql_native_password
 *     restart: always
 *     environment:
 *       MYSQL_DATABASE: 'playground'
 *       MYSQL_ROOT_PASSWORD: root_pass
 *       MYSQL_USER: user
 *       MYSQL_PASSWORD: user_pass
 *     ports:
 *       - '3306:3306'
 *     expose:
 *       - 3306
 *     volumes:
 *       - mysql-playground:/var/lib/mysql
 *
 *   adminer:
 *     image: adminer
 *     restart: always
 *     ports:
 *       - 8080:8080
 *
 * volumes:
 *   mysql-playground:
 * </pre>
 *
 * 2. Create required table via calling {@link Person#main(String[])}
 * <br/><br/>
 *
 *
 * @see OCLogger
 * @see DsWrapper#getConnection()
 * @see ConnectionWrapper#close()
 *
 */
@SuppressWarnings("JavadocReference")
public class LeakedConnectionTest {

    public static final String CACHE_NAME = "personCache";
    public static final String TABLE_NAME = "person";

    public static final String MYSQL_URL = "jdbc:mysql://localhost:3306/playground?useSSL=false";

    public static final boolean HIKARI_DS_ENABLED = true;
    public static final int HIKARI_POOL_SIZE = 5;
    public static final long HIKARI_CONNECTION_TIMEOUT = 30 * 1000L;

    // See OCLogger javadocs to know why these variables are static
    public static DataSource mysqlDataSource;
    public static OCLogger ocLogger;

    private Ignite ignite;
    private IgniteCache<Integer, Person> cache;

    @Before
    public void setUp() throws Exception {
        ocLogger = new OCLogger();
        mysqlDataSource = mysqlDataSource();

        resetTable();

        ignite = Ignition.start(getIgniteConfig());
        cache = ignite.getOrCreateCache(CACHE_NAME);
    }

    /**
     * This test is always green because we do only cache puts, so JDBC connection
     * obtained only when write-behind flushers perform data transmission to underlying DB
     */
    @Test
    public void testPopulateOnly() {
        iterateOverPersons(10, person -> {
            System.out.println("Populate data for person:" + person.getName());
            cache.put(person.getId(), person);
        });

        assertAndFinish();
    }

    /**
     * This test would be green if number of invoke operations would be equal to
     * number of populated persons, so invoke would be performed only over entries
     * that already present in cache.
     */
    @Test
    public void testPopulateAndInvoke() {
        iterateOverPersons(5, person -> {
            System.out.println("Populate data for person:" + person.getName());
            cache.put(person.getId(), person);
        });

        iterateOverPersons(10, this::invoke);

        assertAndFinish();
    }

    /**
     * This test would fail because when we reach tx.commit() call Ignite will acquire a connection
     * and store it in CacheStoreSession properties (see {@link CacheAbstractJdbcStore#load(Object)} ()}
     * and {@link CacheAbstractJdbcStore#connection()}).
     * <br/><br/>
     * <p>
     * When load is finished and we reach finally section inside {@link CacheAbstractJdbcStore#load(Object)}
     * connection would not be closed because of transaction presence
     * (snippet from {@link CacheAbstractJdbcStore#closeConnection(Connection)}):
     * <br/><br/>
     *
     * <pre>
     *  // Close connection right away if there is no transaction.
     *  if (ses.transaction() == null) // here we will got true
     *      U.closeQuiet(conn);
     * </pre>
     * <p>
     * I guess that we have this logic because connection release in case of transaction
     * should be performed in some other place. And most obvious place is
     * {@link GridCacheWriteBehindStore#sessionEnd(boolean)}, but this is noop implementation!
     * <br/><br/>
     * <p>
     * So if take an assumption that 'noop' is a typo, just because this method is deprecated and
     * during refactoring someone forgot to move release logic to some other place, we can make this test green
     * via adding a call to sessionEnd(commit) of underlying cache store in
     * {@link GridCacheWriteBehindStore#sessionEnd(boolean)}, so it would release connection
     * that was acquired and preserved in session properties in {@link CacheAbstractJdbcStore#connection()}
     */
    @Test
    public void testInvokeOnly() {
        iterateOverPersons(3, this::invoke);

        assertAndFinish();
    }

    public void iterateOverPersons(int count, Consumer<Person> operation) {
        for (int i = 0; i < count; i++) {
            Person person = Person.from(i);
            operation.accept(person);
        }
    }

    public void assertAndFinish() {
        // We close ignite before assertion to be sure that write-behind flushing is finished
        // and we can safely proceed to connection open/close counting
        ignite.close();
        ocLogger.assertNotLeaked();
    }

    public void initTable() {
        try (
            Connection connection = mysqlDataSource.getConnection();
            PreparedStatement statement = connection.prepareStatement("DELETE FROM " + TABLE_NAME);
        ) {
            statement.execute();
        }
        catch (SQLException se) {
            se.printStackTrace();
        }
    }

    public void resetTable() {
        try (
            Connection connection = mysqlDataSource.getConnection();
            PreparedStatement statement = connection.prepareStatement("DELETE FROM " + TABLE_NAME);
        ) {
            statement.execute();
        }
        catch (SQLException se) {
            se.printStackTrace();
        }
    }

    static final CacheEntryProcessor<Integer, Person, Boolean> ENTRY_PROCESSOR = (entry, personArg) -> {

        Person newPerson = ((Person)personArg[0]);

        if (entry.exists()) {
            System.out.println("Update deposit for " + newPerson.getName());
            Person existingPerson = entry.getValue();
            existingPerson.setDeposit(existingPerson.getDeposit() + newPerson.getDeposit());
            entry.setValue(existingPerson);
            return true;
        }
        else {
            System.out.println("Initiate account for " + newPerson.getName());
            entry.setValue(newPerson);
            return false;
        }

    };

    public void invoke(Person person) {
        try (Transaction tx = ignite.transactions().txStart(TransactionConcurrency.PESSIMISTIC, TransactionIsolation.REPEATABLE_READ)) {
            cache.invoke(person.getId(), ENTRY_PROCESSOR, person);
            tx.commit();
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    // Configs

    public IgniteConfiguration getIgniteConfig() {
        return new IgniteConfiguration()
            .setWorkDirectory("/tmp/ignite/work")
            .setPeerClassLoadingEnabled(true)
            .setDiscoverySpi(new TcpDiscoverySpi()
                .setIpFinder(new TcpDiscoveryMulticastIpFinder()
                    .setAddresses(Collections.singletonList("127.0.0.1:47500..47509"))))
            .setCacheConfiguration(cacheConfig());
    }

    public CacheConfiguration<Integer, Person> cacheConfig() {
        CacheConfiguration<Integer, Person> cacheConfig = new CacheConfiguration<Integer, Person>()
            .setName(CACHE_NAME)
            .setCacheMode(CacheMode.PARTITIONED)
            .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
            .setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC)
            .setReadThrough(true)
            .setWriteThrough(true)
            .setIndexedTypes(Integer.class, Person.class)

            .setWriteBehindEnabled(true)
            .setWriteBehindFlushThreadCount(1)
            .setWriteBehindBatchSize(100)
            .setWriteBehindFlushFrequency(30_000L);

        CacheJdbcPojoStoreFactory<Integer, Person> cacheStoreFactory = new CacheJdbcPojoStoreFactory<>()
            .setDataSourceFactory(LAMBDA_DS_FACTORY)
            .setDialect(new MySQLDialect())
            .setTypes(new JdbcType()
                .setCacheName(CACHE_NAME)
                .setDatabaseTable(TABLE_NAME)

                .setKeyType(Integer.class)
                .setKeyFields(
                    new JdbcTypeField(Types.INTEGER, "id", Integer.class, "id"))

                .setValueType(Person.class)
                .setValueFields(
                    new JdbcTypeField(Types.INTEGER, "deposit", Integer.class, "deposit"),
                    new JdbcTypeField(Types.VARCHAR, "name", String.class, "name")));

        cacheConfig.setCacheStoreFactory(cacheStoreFactory);

        return cacheConfig;
    }

    // Data Sources Configs

    public static DataSource mysqlDataSource() {
        MysqlDataSource ds = new MysqlDataSource();

        ds.setURL(MYSQL_URL);
        ds.setUser("user");
        ds.setPassword("user_pass");

        return ds;
    }

    public static DataSource hikariDataSource(DataSource mysqlDataSource) {
        HikariConfig config = new HikariConfig();

        config.setJdbcUrl(MYSQL_URL);
        config.setUsername("user");
        config.setPassword("user_pass");
        config.setDataSource(mysqlDataSource);
        config.setMaximumPoolSize(HIKARI_POOL_SIZE);
        config.setConnectionTimeout(HIKARI_CONNECTION_TIMEOUT);

        return new HikariDataSource(config);
    }

    public static DataSource wrappedDataSource() {
        DataSource delegate = HIKARI_DS_ENABLED ? hikariDataSource(mysqlDataSource) : mysqlDataSource;
        return new DsWrapper(delegate, ocLogger);
    }

    /**
     * We have to use factory that uses lambda with static methods and variables because otherwise
     * factory instance would be split to two independent copies after IgniteConfiguration serialization
     * during Ignition.start(cfg). E.g. ocLogger assertion and open/close logging methods would be performed with
     * different instances, so we will always get green tests. See {@link OCLogger#assertNotLeaked()}
     */
    public static Factory<DataSource> LAMBDA_DS_FACTORY = LeakedConnectionTest::wrappedDataSource;

    /**
     * Cache values class
     */
    public static class Person implements Serializable {

        public static void main(String[] args) throws IgniteException {
            DataSource ds = mysqlDataSource();

            String query = new StringBuilder()
                .append("CREATE TABLE " + TABLE_NAME)
                .append("(")
                .append("id INT UNSIGNED PRIMARY KEY, ")
                .append("deposit INT UNSIGNED NOT NULL, ")
                .append("name VARCHAR(100) NOT NULL")
                .append(");")
                .toString();

            try (
                Connection connection = ds.getConnection();
                PreparedStatement statement = connection.prepareStatement(query);
            ) {
                statement.execute();
            }
            catch (SQLException se) {
                se.printStackTrace();
            }
        }

        private static final long serialVersionUID = -7901737726237044821L;

        @QuerySqlField(index = true)
        private Integer id;

        @QuerySqlField(index = true, orderedGroups = @QuerySqlField.Group(name = "search", order = 0, descending = true))
        private int deposit;

        @QuerySqlField(index = true, orderedGroups = @QuerySqlField.Group(name = "search", order = 0, descending = true))
        private String name;

        public static Person from(long id) {
            return new Person(Math.toIntExact(id), 0, "Person_" + id);
        }

        public Person(int id, int deposit, String name) {
            this.id = id;
            this.deposit = deposit;
            this.name = name;
        }

        public Person() {
            // noop
        }

        public Integer getId() {
            return id;
        }

        public void setId(Integer id) {
            this.id = id;
        }

        public int getDeposit() {
            return deposit;
        }

        public void setDeposit(int deposit) {
            this.deposit = deposit;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }
    }

    // JDBC Wrappers

    static class OCLogger implements Serializable {

        private final AtomicLong openCounter = new AtomicLong();
        private final AtomicLong closeCounter = new AtomicLong();

        public void logOpen() {
            openCounter.incrementAndGet();
        }

        public void logClose() {
            closeCounter.incrementAndGet();
        }

        /**
         * If OCLogger instance would be serialized during Ignition.start(),
         * this assertion would be turned to <code>assertEquals(0,0)</code>
         * because Ignite instance would use copy of OCLogger instance for logging
         * connections opening and closing while test class would use original instance for assertion.
         * As was mentioned before this is al due to IgniteConfiguration serialization/marshalling
         * mechanics during startup
         */
        public void assertNotLeaked() {
            assertEquals(openCounter.get(), closeCounter.get());
        }

    }

    public static class ConnectionWrapper implements Connection {

        private final Connection delegate;
        private final OCLogger logger;

        public ConnectionWrapper(Connection delegate, OCLogger logger) {
            this.delegate = delegate;
            this.logger = logger;
        }

        @Override public void close() throws SQLException {
            logger.logClose();
            delegate.close();
        }

        @Override public Statement createStatement() throws SQLException {
            return delegate.createStatement();
        }

        @Override public PreparedStatement prepareStatement(String sql) throws SQLException {
            return delegate.prepareStatement(sql);
        }

        @Override public CallableStatement prepareCall(String sql) throws SQLException {
            return delegate.prepareCall(sql);
        }

        @Override public String nativeSQL(String sql) throws SQLException {
            return delegate.nativeSQL(sql);
        }

        @Override public void setAutoCommit(boolean autoCommit) throws SQLException {
            delegate.setAutoCommit(autoCommit);
        }

        @Override public boolean getAutoCommit() throws SQLException {
            return delegate.getAutoCommit();
        }

        @Override public void commit() throws SQLException {
            delegate.commit();
        }

        @Override public void rollback() throws SQLException {
            delegate.rollback();
        }

        @Override public boolean isClosed() throws SQLException {
            return delegate.isClosed();
        }

        @Override public DatabaseMetaData getMetaData() throws SQLException {
            return delegate.getMetaData();
        }

        @Override public void setReadOnly(boolean readOnly) throws SQLException {
            delegate.setReadOnly(readOnly);
        }

        @Override public boolean isReadOnly() throws SQLException {
            return delegate.isReadOnly();
        }

        @Override public void setCatalog(String catalog) throws SQLException {
            delegate.setCatalog(catalog);
        }

        @Override public String getCatalog() throws SQLException {
            return delegate.getCatalog();
        }

        @Override public void setTransactionIsolation(int level) throws SQLException {
            delegate.setTransactionIsolation(level);
        }

        @Override public int getTransactionIsolation() throws SQLException {
            return delegate.getTransactionIsolation();
        }

        @Override public SQLWarning getWarnings() throws SQLException {
            return delegate.getWarnings();
        }

        @Override public void clearWarnings() throws SQLException {
            delegate.clearWarnings();
        }

        @Override public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
            return delegate.createStatement(resultSetType, resultSetConcurrency);
        }

        @Override public PreparedStatement prepareStatement(String sql, int resultSetType,
            int resultSetConcurrency) throws SQLException {
            return delegate.prepareStatement(sql, resultSetType, resultSetConcurrency);
        }

        @Override public CallableStatement prepareCall(String sql, int resultSetType,
            int resultSetConcurrency) throws SQLException {
            return delegate.prepareCall(sql, resultSetType, resultSetConcurrency);
        }

        @Override public Map<String, Class<?>> getTypeMap() throws SQLException {
            return delegate.getTypeMap();
        }

        @Override public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
            delegate.setTypeMap(map);
        }

        @Override public void setHoldability(int holdability) throws SQLException {
            delegate.setHoldability(holdability);
        }

        @Override public int getHoldability() throws SQLException {
            return delegate.getHoldability();
        }

        @Override public Savepoint setSavepoint() throws SQLException {
            return delegate.setSavepoint();
        }

        @Override public Savepoint setSavepoint(String name) throws SQLException {
            return delegate.setSavepoint(name);
        }

        @Override public void rollback(Savepoint savepoint) throws SQLException {
            delegate.rollback(savepoint);
        }

        @Override public void releaseSavepoint(Savepoint savepoint) throws SQLException {
            delegate.releaseSavepoint(savepoint);
        }

        @Override public Statement createStatement(int resultSetType, int resultSetConcurrency,
            int resultSetHoldability) throws SQLException {
            return delegate.createStatement(resultSetType, resultSetConcurrency, resultSetHoldability);
        }

        @Override public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency,
            int resultSetHoldability) throws SQLException {
            return delegate.prepareStatement(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
        }

        @Override public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency,
            int resultSetHoldability) throws SQLException {
            return delegate.prepareCall(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
        }

        @Override public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
            return delegate.prepareStatement(sql, autoGeneratedKeys);
        }

        @Override public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
            return delegate.prepareStatement(sql, columnIndexes);
        }

        @Override public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
            return delegate.prepareStatement(sql, columnNames);
        }

        @Override public Clob createClob() throws SQLException {
            return delegate.createClob();
        }

        @Override public Blob createBlob() throws SQLException {
            return delegate.createBlob();
        }

        @Override public NClob createNClob() throws SQLException {
            return delegate.createNClob();
        }

        @Override public SQLXML createSQLXML() throws SQLException {
            return delegate.createSQLXML();
        }

        @Override public boolean isValid(int timeout) throws SQLException {
            return delegate.isValid(timeout);
        }

        @Override public void setClientInfo(String name, String value) throws SQLClientInfoException {
            delegate.setClientInfo(name, value);
        }

        @Override public void setClientInfo(Properties properties) throws SQLClientInfoException {
            delegate.setClientInfo(properties);
        }

        @Override public String getClientInfo(String name) throws SQLException {
            return delegate.getClientInfo(name);
        }

        @Override public Properties getClientInfo() throws SQLException {
            return delegate.getClientInfo();
        }

        @Override public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
            return delegate.createArrayOf(typeName, elements);
        }

        @Override public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
            return delegate.createStruct(typeName, attributes);
        }

        @Override public void setSchema(String schema) throws SQLException {
            delegate.setSchema(schema);
        }

        @Override public String getSchema() throws SQLException {
            return delegate.getSchema();
        }

        @Override public void abort(Executor executor) throws SQLException {
            delegate.abort(executor);
        }

        @Override public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
            delegate.setNetworkTimeout(executor, milliseconds);
        }

        @Override public int getNetworkTimeout() throws SQLException {
            return delegate.getNetworkTimeout();
        }

        @Override public <T> T unwrap(Class<T> iface) throws SQLException {
            return delegate.unwrap(iface);
        }

        @Override public boolean isWrapperFor(Class<?> iface) throws SQLException {
            return delegate.isWrapperFor(iface);
        }
    }

    public static class DsWrapper implements DataSource, Serializable {

        private final DataSource delegate;
        private final OCLogger logger;

        public DsWrapper(DataSource delegate, OCLogger logger) {
            this.delegate = delegate;
            this.logger = logger;
        }

        @Override public Connection getConnection() throws SQLException {
            logger.logOpen();
            return new ConnectionWrapper(delegate.getConnection(), logger);
        }

        @Override public Connection getConnection(String username, String password) throws SQLException {
            logger.logOpen();
            return new ConnectionWrapper(delegate.getConnection(username, password), logger);
        }

        @Override public PrintWriter getLogWriter() throws SQLException {
            return delegate.getLogWriter();
        }

        @Override public void setLogWriter(PrintWriter out) throws SQLException {
            delegate.setLogWriter(out);
        }

        @Override public void setLoginTimeout(int seconds) throws SQLException {
            delegate.setLoginTimeout(seconds);
        }

        @Override public int getLoginTimeout() throws SQLException {
            return delegate.getLoginTimeout();
        }

        @Override public Logger getParentLogger() throws SQLFeatureNotSupportedException {
            return delegate.getParentLogger();
        }

        @Override public <T> T unwrap(Class<T> iface) throws SQLException {
            return delegate.unwrap(iface);
        }

        @Override public boolean isWrapperFor(Class<?> iface) throws SQLException {
            return delegate.isWrapperFor(iface);
        }
    }

}
