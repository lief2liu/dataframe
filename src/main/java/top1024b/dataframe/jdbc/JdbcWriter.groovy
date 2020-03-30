package top1024b.dataframe.jdbc

import com.google.common.collect.Lists
import groovy.sql.Sql
import groovy.transform.CompileStatic
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.jdbc.core.ArgumentPreparedStatementSetter
import org.springframework.jdbc.core.ColumnMapRowMapper
import org.springframework.jdbc.core.JdbcTemplate
import top1024b.dataframe.common.DataFrame
import top1024b.dataframe.common.DataFrameWriter

@CompileStatic
class JdbcWriter implements DataFrameWriter, Serializable {
    private Logger logger = LoggerFactory.getLogger(this.getClass())
    private Map<String, Object> conn
    private JdbcTemplate jdbcTemplate
    private String tb
    private int batchSize = 10000

    /**
     * specify the batch size
     */
    JdbcWriter BatchSize(int batchSize) {
        this.batchSize = batchSize
        return this
    }

    /**
     * construct jdbcWriter with jdbc connect information,
     * recommend when developing non-interactive project
     */
    JdbcWriter(JdbcConn conn, String tb) {
        getByConn(conn.getMap(), tb)
    }

    JdbcWriter(Map<String, Object> conn, String tb) {
        getByConn(conn, tb)
    }

    private void getByConn(Map<String, Object> conn, String tb) {
        if (!conn || !tb) {
            throw new RuntimeException("param can't be null")
        }
        this.tb = tb
        this.conn = conn
    }

    /**
     * construct jdbcWriter with spring jdbcTemplate,
     * recommend when developing interactive project such as web project
     */
    JdbcWriter(JdbcTemplate jdbcTemplate, String tb) {
        getByTemplate(jdbcTemplate, tb)
    }

    private void getByTemplate(JdbcTemplate jdbcTemplate, String tb) {
        if (!jdbcTemplate || !tb) {
            throw new RuntimeException("param can't be null")
        }
        this.tb = tb
        this.jdbcTemplate = jdbcTemplate
    }

    /*
     * if you want to execute a single sql without dataframe, using this method
     * in this case , tb is regarded the sql you want to execute
     */
    int write(List<Object> values) {
        if (conn) {
            return withDb { Sql db ->
                if (values) {
                    return db.executeUpdate(tb, values)
                }
                db.executeUpdate(tb)
            }
        }

        if (values) {
            return jdbcTemplate.update(tb, values as Object[], new ColumnMapRowMapper())
        }

        jdbcTemplate.update(tb)
    }

    /**
     * write the dataframe to jdbc
     */
    @Override
    int write(DataFrame df) {
        List<Map<String, Object>> rows = df.rows
        if (!rows) {
            throw new RuntimeException("dataframe is empty!")
        }

        switch (df.getSaveMode()) {
            case DataFrame.SaveMode.INSERT:
                return batchByline(rows, { String field, String why ->
                    "insert into $tb($field) values $why".toString()
                })
            case DataFrame.SaveMode.IGNORE:
                return batchByline(rows, { String field, String why ->
                    "insert ignore into $tb($field) values $why".toString()
                })
            case DataFrame.SaveMode.REPLACE:
                return batchByline(rows, { String field, String why ->
                    "replace into $tb($field) values $why".toString()
                })
            case DataFrame.SaveMode.UPSERT:
                return batchByline(rows, { String field, String why ->
                    String update = field.split(",").collect({ "$it=values($it)" }).join(",")
                    "insert into $tb($field) values $why on duplicate key update $update".toString()
                })
            default:
                throw new IllegalArgumentException("unsupported save mode ${df.getSaveMode()},save mode can only be : insert/ignore/upsert/replace")
        }
    }

    /**
     * write data into database with a compressed sql
     */
    private int batchByline(List<Map<String, Object>> rows, Closure<String> getSql) {

        if (conn) {
            return withDb { Sql db ->
                partitionUpdate(rows, getSql, { String sql, List<Object> values ->
                    db.executeUpdate(sql, values)
                })
            }
        }

        partitionUpdate(rows, getSql, { String sql, List<Object> values ->
            jdbcTemplate.update(sql, new ArgumentPreparedStatementSetter(values as Object[]))
        })
    }

    /**
     * write data into database with batch,you can specify the batchsize by method of batchSize(size)
     */
    int partitionUpdate(List<Map<String, Object>> rows, Closure<String> getSql, Closure<Integer> update) {
        Set<String> cols = rows.get(0).keySet()
        String field = cols.join(",")
        String why = getWhyStr(cols)

        Lists.partition(rows, batchSize).collect { List<Map<String, Object>> subRows ->
            StringBuffer sb = new StringBuffer(rows.collect { "($why)" }.join(","))
            String sql = getSql(field, sb.toString())
            List<Object> values = rows.collect { it.values().toList() }.inject { a, b ->
                a.addAll(b)
                a
            }

            update(sql, values)
        }.inject { int a, int b -> a + b }
    }

    int withDb(Closure<Integer> act) {
        Sql db
        try {
            db = Sql.newInstance(conn)
            return act(db)
        } finally {
            try {
                if (!db.getConnection().getAutoCommit()) {
                    db.commit()
                }
            } catch (Exception e) {
                logger.info(e.getMessage())
            }
            db?.close()
        }
    }

    private String getWhyStr(Set<String> cols) {
        String why = ""
        for (int i = 0; i < cols.size(); i++) {
            why += "?,"
        }
        why.substring(0, why.length() - 1)
    }
}
