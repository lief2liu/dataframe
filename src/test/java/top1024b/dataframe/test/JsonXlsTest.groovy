package top1024b.dataframe.test


import groovy.transform.CompileStatic
import top1024b.dataframe.common.DataFrame
import top1024b.dataframe.common.Page
import top1024b.dataframe.json.JsonReader
import top1024b.dataframe.json.JsonWriter
import top1024b.dataframe.redis.RedisConn
import top1024b.dataframe.redis.RedisReader
import top1024b.dataframe.xls.XlsReader
import top1024b.dataframe.xls.XlsWriter

@CompileStatic
class JsonXlsTest implements Serializable {
    static void main(String[] args) {
        DataFrame df = DataFrame.read(new JsonReader(Common.json))
        df.show()

        StringBuffer buffer = new StringBuffer()
        df.write(new JsonWriter(buffer))
        println buffer.toString()

        df.write(new XlsWriter("D:\\test.xls"))

        DataFrame.read(new XlsReader("D:\\test.xls")).show()
    }
}