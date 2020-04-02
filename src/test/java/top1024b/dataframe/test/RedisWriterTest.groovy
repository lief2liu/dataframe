package top1024b.dataframe.test


import groovy.transform.CompileStatic
import redis.clients.jedis.JedisPool
import redis.clients.jedis.JedisPoolConfig
import top1024b.dataframe.common.DataFrame
import top1024b.dataframe.jdbc.JdbcConn
import top1024b.dataframe.jdbc.JdbcReader
import top1024b.dataframe.redis.RedisConn
import top1024b.dataframe.redis.RedisWriter

@CompileStatic
class RedisWriterTest implements Serializable {
    static void main(String[] args) {
        String jdbcUrl = Common.jdbcUrl

        JdbcConn conn = new JdbcConn()
        conn.setDriver(JdbcConn.DRIVER_MYSQL)
        conn.setUrl(jdbcUrl)
        conn.setUser("root")
        conn.setPassword("123456")
        DataFrame df = DataFrame.read(new JdbcReader(conn, "select id as _id,name,age from person limit 10 offset 20"))
        new RedisWriterTest().testConn(df)
//        new RedisWriterTest().testPool(df)
    }

    void testConn(DataFrame df) {
        RedisConn redis = new RedisConn()
        redis.setHost(Common.redisHost)
        redis.setPassword(Common.redisPassword)
        redis.setDb(3)

        df?.write(new RedisWriter(redis, "test:person"))
    }

    void testPool(DataFrame df) {
        JedisPoolConfig conf = new JedisPoolConfig()
        JedisPool pool = new JedisPool(conf, "192.168.1.13", 6379, 2000, "bigpass", 3)

        df.setSaveMode(DataFrame.SaveMode.OVERRIDE).setSaveMode(DataFrame.SaveMode.OVERRIDE)
//                ?.show()
                ?.write(new RedisWriter(pool, "test:person"))
    }
}