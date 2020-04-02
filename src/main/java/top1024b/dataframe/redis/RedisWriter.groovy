package top1024b.dataframe.redis

import groovy.transform.CompileStatic
import redis.clients.jedis.Jedis
import redis.clients.jedis.JedisPool
import redis.clients.jedis.Pipeline
import top1024b.dataframe.common.DataFrame
import top1024b.dataframe.common.DataFrameWriter

@CompileStatic
class RedisWriter implements DataFrameWriter, Serializable {
    private JedisPool pool
    private RedisConn conn = new RedisConn()
    private String tb
    private String key = RedisConn.KEY
    private boolean nullInsert = true

    /**
     * is null insert into redis
     */
    RedisWriter nullInsert(boolean nullInsert) {
        this.nullInsert = nullInsert
        return this
    }

    /**
     * construct redisWriter with redis connect information,
     * recommend when developing non-interactive project
     */
    RedisWriter(RedisConn conn, String tb) {
        if (!conn || !tb) {
            throw new RuntimeException("param can't be null")
        }
        this.tb = tb
        this.conn = conn
    }

    /**
     * construct redisWriter with pool,
     * recommend when developing interactive project such as web project
     */
    RedisWriter(JedisPool pool, String tb) {
        if (!pool || !tb) {
            throw new RuntimeException("param can't be null")
        }
        this.pool = pool
        this.tb = tb
    }

    /**
     * write the dataframe to redis
     */
    @Override
    int write(DataFrame df) {
        List<Map<String, Object>> rows = df.rows
        if (!rows) {
            throw new RuntimeException("dataframe is empty!")
        }

        conn.withDb({ Pipeline pip, Jedis jedis ->
            switch (df.getSaveMode()) {
                case DataFrame.SaveMode.OVERRIDE:
                    Set<String> keys = jedis.keys(tb + ":*")
                    keys.each {
                        pip.del(it)
                    }
                case DataFrame.SaveMode.UPSERT:
                    rows.each { Map<String, Object> map ->
                        String value
                        try {
                            value = map.get(key) as String
                        } catch (Exception e) {
                            throw new RuntimeException("every row should have a '$key' field as primary key")
                        }
                        Map newMap = [:]
                        map.keySet().each {
                            if (it != key) {
                                def dt = map.get(it)
                                if (!nullInsert && dt == null) {
                                    return
                                }
                                newMap.put(it, map.get(it).toString())
                            }
                        }
                        pip.hset(tb + ":" + value, newMap as Map<String, String>)
                        pip.sadd(tb + ":" + key, value)
                    }
                    rows.get(0).keySet().each {
                        if (it == key) {
                            return
                        }
                        pip.sadd(tb + ":schema", it as String)
                    }
                    break
                default:
                    throw new IllegalArgumentException("unsupported save mode ${df.getSaveMode()},save mode can only be : upsert/override")
            }
        }, pool)

        df.size()
    }
}
