package top1024b.dataframe.redis

import groovy.transform.CompileStatic
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import redis.clients.jedis.Jedis
import redis.clients.jedis.JedisPool
import redis.clients.jedis.Pipeline
import redis.clients.jedis.SortingParams
import top1024b.dataframe.common.DataFrame
import top1024b.dataframe.common.DataFrameReader
import top1024b.dataframe.common.Page

import java.util.regex.Matcher

/**
 * implements DataFrameReader to read rows from redis
 */
@CompileStatic
class RedisReader implements DataFrameReader, Serializable {
    private Logger logger = LoggerFactory.getLogger(this.getClass())
    private JedisPool pool
    private RedisConn conn
    private RedisSql redisSql
    private String key = RedisConn.KEY
    private Page page

    /**
     * is null insert into redis
     */
    RedisReader setPage(Page page) {
        this.page = page
        return this
    }

    /**
     * construct redisReader with redis connect information,
     * recommend when developing non-interactive project
     * @param conn
     * @param sql
     */
    RedisReader(RedisConn conn, String sql) {
        if (!conn || !sql) {
            throw new RuntimeException("param can't be null")
        }
        parseSql(sql)
        this.conn = conn
    }

    /**
     * construct redisReader with pool,
     * recommend when developing interactive project such as web project
     */
    RedisReader(JedisPool pool, String sql) {
        if (!pool || !sql) {
            throw new RuntimeException("param can't be null")
        }
        parseSql(sql)
        this.pool = pool
    }

    /**
     * parse sql
     */
    void parseSql(String sql) {
        Matcher matcher = sql.toLowerCase() =~ "^select\\s+(\\S+)\\s+from\\s+(\\S+)(\\s+order\\s+by\\s+(\\S+)(\\sdesc)?)?(\\s+limit\\s+(\\d+)(\\soffset\\s+(\\d+))?)?\$"
        if (matcher.find()) {
            RedisSql redisSql = new RedisSql()
            redisSql.setField(matcher.group(1))
            redisSql.setTb(matcher.group(2))
            redisSql.setOrder(matcher.group(4))
            if (matcher.group(5)) {
                redisSql.setAsc(false)
            }

            try {
                redisSql.setLimit(matcher.group(7) as Integer)
                redisSql.setOffset(matcher.group(9) as Integer)
            } catch (Exception e) {
                logger.info("limit or offset is null")
            }
            this.redisSql = redisSql

            return
        }

        throw new RuntimeException("sql syntax error,the right redis  sql is like this (select * from tb order by field desc limit 10 offset 20)")
    }

    @Override
    DataFrame read() {
        conn.withDb({ Pipeline pip, Jedis jedis ->
            String tb = redisSql.getTb()

            //get field
            SortingParams params = new SortingParams()
            String field = redisSql.getField()
            List<String> fieldList = []
            if (field == "*") {
                fieldList = jedis.smembers(tb + ":schema") as List
                fieldList.add(0, key)
            } else {
                fieldList = field.split(",") as List
            }

            fieldList.each {
                if (it != key) {
                    params.get("$tb:*->$it")
                } else {
                    params.get("#")
                }
            }

            //get order and desc
            String sort = redisSql.getOrder()
            if (sort != key) {
                params.by("$tb:*->$sort")
            }
            if (!redisSql.asc) {
                params.desc()
            }

            //get limit and offset
            if (page) {
                params.limit(page.getOffset(), page.getSize())
                page.setTotal(jedis.scard("$tb:$key"))
            } else {
                Integer limit = redisSql.getLimit()
                if (limit != null) {
                    params.limit(redisSql.getOffset(), limit)
                }
            }

            //query
            int fieldSize = fieldList.size()
            List<Map<String, Object>> rows = jedis.sort("$tb:$key".toString(), params).collate(fieldSize)
                    .collect { List list ->
                        Map<String, Object> map = [:] as Map<String, Object>
                        (0..fieldSize - 1).each { int index ->
                            map.put(fieldList[index], list[index] as Object)
                        }
                        map
                    }

            return DataFrame.read(rows, page?.getMap() as Map<String, Object>)
        }, pool) as DataFrame
    }
}

