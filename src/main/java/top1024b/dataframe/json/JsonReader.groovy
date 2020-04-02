package top1024b.dataframe.json

import groovy.json.JsonSlurper
import groovy.transform.CompileStatic
import top1024b.dataframe.common.DataFrame
import top1024b.dataframe.common.DataFrameReader

@CompileStatic
class JsonReader implements DataFrameReader, Serializable {
    String json

    JsonReader(String json) {
        if (!json.startsWith("[")) {
            json = "[$json]"
        }
        this.json = json
    }

    @Override
    DataFrame read() {
        List<Map<String, Object>> rows = new JsonSlurper().parseText(json) as List
        if (rows && rows.size() > 0) {
            return DataFrame.read(rows, null)
        }

        null
    }
}
