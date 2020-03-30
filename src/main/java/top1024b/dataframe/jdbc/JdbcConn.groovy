package top1024b.dataframe.jdbc


import groovy.transform.CompileStatic

/**
 * jdbc connection information
 */
@CompileStatic
class JdbcConn implements Serializable {
    final static String DRIVER = "driver"
    final static String URL = "url"
    final static String USER = "user"
    final static String PASSWORD = "password"

    final static String DRIVER_MYSQL = "com.mysql.jdbc.Driver"
    final static String DRIVER_ORACLE = "oracle.jdbc.driver.OracleDriver"

    private String driver
    private String url
    private String user
    private String password

    JdbcConn() {}

    JdbcConn(String driver, String url, String user, String password) {
        this.driver = driver
        this.url = url
        this.user = user
        this.password = password
    }

    Map<String, Object> getMap() {
        [
                (JdbcConn.DRIVER)  : driver,
                (JdbcConn.URL)     : url,
                (JdbcConn.USER)    : user,
                (JdbcConn.PASSWORD): password
        ] as Map<String, Object>
    }

    String getDriver() {
        return driver
    }

    void setDriver(String driver) {
        this.driver = driver
    }

    String getUrl() {
        return url
    }

    void setUrl(String url) {
        this.url = url
    }

    String getUser() {
        return user
    }

    void setUser(String user) {
        this.user = user
    }

    String getPassword() {
        return password
    }

    void setPassword(String password) {
        this.password = password
    }
}

