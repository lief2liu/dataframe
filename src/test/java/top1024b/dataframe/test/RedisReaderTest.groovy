package top1024b.dataframe.test


import groovy.transform.CompileStatic
import top1024b.dataframe.common.DataFrame
import top1024b.dataframe.common.Page
import top1024b.dataframe.redis.RedisConn
import top1024b.dataframe.redis.RedisReader

@CompileStatic
class RedisReaderTest implements Serializable {
    static void main(String[] args) {
        RedisConn redis = new RedisConn()
        redis.setHost(Common.redisHost)
        redis.setPassword(Common.redisPassword)
        redis.setDb(3)

        DataFrame df = DataFrame.read(
                new RedisReader(redis,"select * from test:person order by age desc limit 10 offset 4")
                .setPage(new Page(5,2))
        )
        df.show()
        println df.getExt()
    }
}