package top1024b.dataframe.test

import com.zaxxer.hikari.HikariDataSource
import groovy.transform.CompileStatic
import org.springframework.boot.jdbc.DataSourceBuilder
import org.springframework.jdbc.core.JdbcTemplate
import top1024b.dataframe.common.DataFrame
import top1024b.dataframe.common.Page
import top1024b.dataframe.jdbc.JdbcConn
import top1024b.dataframe.jdbc.JdbcReader

@CompileStatic
class JdbcReaderTest implements Serializable {
    static void main(String[] args) {
        String jdbcUrl = "jdbc:mysql://192.168.1.13:3306/test" +
                "?allowMultiQueries=true&autoReconnect=true&failOverReadOnly=false" +
                "&useSSL=false&zeroDateTimeBehavior=convertToNull&characterEncoding=utf8"

        new JdbcReaderTest().testConn(jdbcUrl)
        new JdbcReaderTest().testTemplate(jdbcUrl)
    }

    void testConn(String jdbcUrl) {

        JdbcConn conn = new JdbcConn()
        conn.setDriver(JdbcConn.DRIVER_MYSQL)
        conn.setUrl(jdbcUrl)
        conn.setUser("root")
        conn.setPassword("123456")

        DataFrame.read(new JdbcReader(conn, "select * from person limit 10"))?.show()
    }

    void testTemplate(String jdbcUrl) {
        HikariDataSource dataSource = DataSourceBuilder.create().type(HikariDataSource.class).build()
        dataSource.setDriverClassName(JdbcConn.DRIVER_MYSQL)
        dataSource.setJdbcUrl(jdbcUrl)
        dataSource.setUsername("root")
        dataSource.setPassword("123456")
        JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource)

        DataFrame.read(new JdbcReader(jdbcTemplate, "select * from person limit 10", null))?.show()


        Page page = new Page()
        page.setNo(2)
        page.setSize(20)
        DataFrame.read(new JdbcReader(jdbcTemplate, "select * from person", null, page))?.show()

        DataFrame.read(new JdbcReader(
                jdbcTemplate,
                "select * from person where id<? and id>?",
                [30, 25] as List<Object>
        ))?.show()
    }
}