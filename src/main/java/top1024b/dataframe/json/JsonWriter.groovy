package top1024b.dataframe.json


import groovy.json.JsonOutput
import groovy.transform.CompileStatic
import top1024b.dataframe.common.DataFrame
import top1024b.dataframe.common.DataFrameWriter

@CompileStatic
class JsonWriter implements DataFrameWriter,Serializable{
    StringBuffer sb

    JsonWriter(StringBuffer sb) {
        this.sb = sb
    }

    @Override
    int write(DataFrame df) {
        sb.append(new JsonOutput().toJson(df.rows))

        df.size()
    }
}
