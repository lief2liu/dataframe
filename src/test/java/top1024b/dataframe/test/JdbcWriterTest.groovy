package top1024b.dataframe.test

import com.zaxxer.hikari.HikariDataSource
import groovy.transform.CompileStatic
import org.springframework.boot.jdbc.DataSourceBuilder
import org.springframework.jdbc.core.JdbcTemplate
import top1024b.dataframe.common.DataFrame
import top1024b.dataframe.jdbc.JdbcConn
import top1024b.dataframe.jdbc.JdbcWriter

@CompileStatic
class JdbcWriterTest implements Serializable {
    static void main(String[] args) {
        List<Map<String, Object>> rows = new LinkedList<>()
        (1..100).each {
            Map<String, Object> row = ["name": "a$it".toString(), age: new Random().nextInt(100)] as Map<String, Object>
            rows.add(row)
        }
        DataFrame df = DataFrame.read(rows, null)

        String jdbcUrl = "jdbc:mysql://192.168.1.13:3306/test" +
                "?allowMultiQueries=true&autoReconnect=true&failOverReadOnly=false" +
                "&useSSL=false&zeroDateTimeBehavior=convertToNull&characterEncoding=utf8"

        new JdbcWriterTest().testConn(df, jdbcUrl)
        new JdbcWriterTest().testTemplate(df, jdbcUrl)
    }

    void testConn(DataFrame df, String jdbcUrl) {

        JdbcConn conn = new JdbcConn()
        conn.setDriver(JdbcConn.DRIVER_MYSQL)
        conn.setUrl(jdbcUrl)
        conn.setUser("root")
        conn.setPassword("123456")

        println "res:" + df.write(new JdbcWriter(conn, "person"))
    }

    void testTemplate(DataFrame df, String jdbcUrl) {
        HikariDataSource dataSource = DataSourceBuilder.create().type(HikariDataSource.class).build()
        dataSource.setDriverClassName(JdbcConn.DRIVER_MYSQL)
        dataSource.setJdbcUrl(jdbcUrl)
        dataSource.setUsername("root")
        dataSource.setPassword("123456")
        JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource)

        println "res:" + df.write(new JdbcWriter(jdbcTemplate, "person"))
    }
}