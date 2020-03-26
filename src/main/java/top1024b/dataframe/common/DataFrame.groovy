package top1024b.dataframe.common

import groovy.transform.CompileStatic
import org.slf4j.Logger
import org.slf4j.LoggerFactory

/**
 *  DataFrame base class,a dataframe consists of a list and a map,
 *  the list stores rows of data while the map stores extend information
 */
@CompileStatic
class DataFrame implements Serializable {
    private Logger logger = LoggerFactory.getLogger(this.getClass())
    private List<Map<String, Object>> rows
    private Map<String, Object> ext
    private SaveMode saveMode = SaveMode.UPSERT

    /**
     * insert : if the row already exists , an exception will be thrown
     * ignore ：if the row already exists , the new row will be ignored
     * upsert : if the row already exists , the old row will be updated by the new row
     * replace：if the row already exists , delete the old row and insert the new row
     */
    enum SaveMode {
        INSERT, IGNORE, UPSERT, REPLACE
    }

    private DataFrame(List<Map<String, Object>> rows, Map<String, Object> ext) {
        this.rows = rows
        this.ext = ext
    }

    /**
     * a dataframe can be created by rows and ext
     * @param rows
     * @param ext
     * @return
     */
    static DataFrame read(List<Map<String, Object>> rows, Map<String, Object> ext) {
        if (!rows) {
            return null
        }

        new DataFrame(rows, ext)
    }

    /**
     * dataframe can also be created by DataFrameReader
     * @param reader every datasource has its own reader
     * @return
     */
    static DataFrame read(DataFrameReader reader) {
        reader.read()
    }

    /**
     * write the data(rows) of a dataframe to somewhere like mysql,elasticsearch,etc
     * @param writer every datasource has its own writer
     * @return
     */
    int write(DataFrameWriter writer) {
        writer.write(this)
    }

    int size() {
        rows.size()
    }

    List<Map<String, Object>> getRows() {
        rows as List<Map<String, Object>>
    }

    Map<String, Object> getExt() {
        ext
    }

    Map<String, Object> dt2map() {
        Map<String, Object> res = [rows: getRows()] as Map<String, Object>
        if (ext) {
            res.put("ext", ext)
        }

        res
    }

    DataFrame SaveMode(SaveMode saveMode) {
        this.saveMode = saveMode

        this
    }

    SaveMode getSaveMode() {
        return saveMode
    }

    /**
     * print the dataframe
     */
    void show() {
        show(15)
    }

    void show(int width) {
        Set<String> cols = rows.get(0).keySet()

        String delimiter = ""
        width.times {
            delimiter += "-"
        }
        String content = ""
        String rowDelimeter = ""
        cols.size().times {
            rowDelimeter += delimiter
        }
        rowDelimeter += "\r\n"
        content += rowDelimeter
        cols.each {
            if (it.size() > delimiter.size() - 1) {
                it = it.substring(0, delimiter.size() - 1)
            }
            content += it
            (delimiter.size() - it.size() - 1).times {
                content += " "
            }
            content += "|"
        }
        content += "\r\n"
        content += rowDelimeter

        rows.each {
            it.each { kv ->
                String value = kv.value
                if (!value) {
                    value = ""
                } else if (value.size() > delimiter.size() - 1) {
                    value = value.substring(0, delimiter.size() - 1)
                }
                content += value
                if (value.getBytes("GBK").size() != value.size()) {
                    (delimiter.size() - value.getBytes("GBK").size()).times {
                        content += " "
                    }
                } else {
                    (delimiter.size() - value.size() - 1).times {
                        content += " "
                    }
                }
                content += "|"
            }
            content += "\r\n"
        }

        logger.info("\r\n" + content)
    }
}
