package top1024b.dataframe.jdbc

import groovy.sql.GroovyRowResult
import groovy.sql.Sql
import groovy.transform.CompileStatic
import org.springframework.jdbc.core.ColumnMapRowMapper
import org.springframework.jdbc.core.JdbcTemplate
import top1024b.dataframe.common.DataFrame
import top1024b.dataframe.common.DataFrameReader
import top1024b.dataframe.common.Page

/**
 * implements DataFrameReader to write rows to jdbc
 */
@CompileStatic
class JdbcReader implements DataFrameReader, Serializable {
    private String sql
    private Map<String, Object> conn
    private JdbcTemplate jdbcTemplate
    private List<Object> values
    private Map<String, Integer> page

    /**
     * construct jdbcReader with jdbc connect information,
     * recommend when developing non-interactive project
     * @param conn
     * @param sql
     */
    JdbcReader(JdbcConn conn, String sql) {
        getByConn(conn.getMap(), sql)
    }

    JdbcReader(Map<String, Object> conn, String sql) {
        getByConn(conn, sql)
    }

    private void getByConn(Map<String, Object> conn, String sql) {
        if (!conn || !sql) {
            throw new RuntimeException("param can't be null")
        }
        this.sql = sql
        this.conn = conn
    }

    /**
     * construct jdbcReader with spring jdbcTemplate,
     * recommend when developing interactive project such as web project
     * @param conn
     * @param sql
     */
    JdbcReader(JdbcTemplate jdbcTemplate, String sql, List<Object> values, Page page) {
        getByTemplate(jdbcTemplate, sql, values, page.getMap())
    }

    JdbcReader(JdbcTemplate jdbcTemplate, String sql, List<Object> values, Map<String, Integer> page) {
        getByTemplate(jdbcTemplate, sql, values, page)
    }

    JdbcReader(JdbcTemplate jdbcTemplate, String sql, List<Object> values) {
        getByTemplate(jdbcTemplate, sql, values, null)
    }

    private void getByTemplate(JdbcTemplate jdbcTemplate, String sql, List<Object> values, Map<String, Integer> page) {
        if (!jdbcTemplate || !sql) {
            throw new RuntimeException("param can't be null")
        }
        this.sql = sql
        this.jdbcTemplate = jdbcTemplate
        this.values = values
        this.page = page
    }


    @Override
    DataFrame read() {
        if (conn) {
            return jdbc()
        }

        jdbcTemplate()
    }

    /**
     * pure groovy jdbc
     * @return
     */
    DataFrame jdbc() {
        Sql db
        try {
            db = Sql.newInstance(conn)
            List<GroovyRowResult> rows = db.rows(sql)

            return DataFrame.read(rows as List<Map<String, Object>>, null)
        } finally {
            db?.close()
        }
    }

    /**
     * spring jdbc template
     * @return
     */
    DataFrame jdbcTemplate() {
        if (page) {
            return jdbcTemplatePage()
        }

        List<Map<String, Object>> rows
        if (values) {
            rows = jdbcTemplate.query(sql, values as Object[], new ColumnMapRowMapper())
        } else {
            rows = jdbcTemplate.queryForList(sql)
        }

        DataFrame.read(rows, null)
    }

    /**
     * spring jdbc template with page
     * @return
     */
    DataFrame jdbcTemplatePage() {
        String listSql = """
            select * from ($sql) t ${limit(page)}
        """.toString().trim()

        String totalSql = """
            select count(1) from ($sql) t
        """.toString().trim()

//        TODO add page support for oracle
//        if(jdbcTemplate.getDataSource().getConnection().getMetaData().getDatabaseProductName() == JdbcConn.DRIVER_MYSQL){
//            totalSql =
//        }

        List<Map<String, Object>> rows
        int total
        if (values) {
            rows = jdbcTemplate.query(listSql, values as Object[], new ColumnMapRowMapper())
            total = jdbcTemplate.queryForObject(totalSql, values as Object[], Integer.class)
        } else {
            rows = jdbcTemplate.queryForList(listSql)
            total = jdbcTemplate.queryForObject(totalSql, Integer.class)
        }
        page.put(Page.TOTAL, total)

        DataFrame.read(rows, page as Map<String, Object>)
    }

    /**
     * generate limit sql by page
     * @param page
     * @return
     */
    private String limit(Map<String, Integer> page) {
        int pageNo = page.get(Page.NO)
        int limit = page.get(Page.SIZE)
        int offset = (pageNo - 1) * limit
        """limit $offset,$limit""".toString()
    }
}

