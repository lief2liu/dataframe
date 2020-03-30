package top1024b.dataframe.redis

import groovy.transform.CompileStatic
import redis.clients.jedis.Jedis
import redis.clients.jedis.JedisPool
import redis.clients.jedis.Pipeline

/**
 * Redis connection information
 */
@CompileStatic
class RedisConn implements Serializable {
    final static String HOST = "redis.host"
    final static String PORT = "redis.port"
    final static String DB = "redis.db"
    final static String PASSWORD = "redis.auth"
    final static String KEY = "_id"

    private String host
    private Integer port = 6379
    private Integer db = 1
    private String password

    RedisConn() {
    }

    RedisConn(String host, Integer port, Integer db, String password) {
        if (port) {
            this.port = port
        }
        if (db) {
            this.db = db
        }
        this.host = host
        this.password = password
    }

    Map<String, Object> getMap() {
        [
                (RedisConn.HOST)    : host,
                (RedisConn.PORT)    : port,
                (RedisConn.DB)      : db,
                (RedisConn.PASSWORD): password
        ] as Map<String, Object>
    }

    Object withDb(Closure fun, JedisPool pool) {
        Jedis jedis = null
        Pipeline pip = null
        try {
            if (pool != null) {
                jedis = pool.getResource()
            } else {
                jedis = new Jedis(host, port)
                if (password) {
                    jedis.auth(password)
                }
                if (db) {
                    jedis.select(db)
                }
            }

            pip = jedis.pipelined()

            fun(pip, jedis)
        } finally {
            pip?.close()
            jedis?.close()
        }
    }

    String getHost() {
        return host
    }

    void setHost(String host) {
        this.host = host
    }

    Integer getPort() {
        return port
    }

    void setPort(Integer port) {
        this.port = port
    }

    Integer getDb() {
        return db
    }

    void setDb(Integer db) {
        this.db = db
    }

    String getPassword() {
        return password
    }

    void setPassword(String password) {
        this.password = password
    }
}

