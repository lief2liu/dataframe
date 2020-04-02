package top1024b.dataframe.xls


import groovy.transform.CompileStatic
import jxl.Workbook
import jxl.write.Label
import jxl.write.WritableSheet
import jxl.write.WritableWorkbook
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import top1024b.dataframe.common.DataFrame
import top1024b.dataframe.common.DataFrameWriter

@CompileStatic
class XlsWriter implements DataFrameWriter, Serializable {

    Logger logger = LoggerFactory.getLogger(this.getClass())
    String file

    XlsWriter(String file) {
        this.file = file
    }

    @Override
    int write(DataFrame df) {
        WritableWorkbook workbook
        try {
            File file = new File(file)
            workbook = Workbook.createWorkbook(file)
            WritableSheet sheet = workbook.createSheet("sheet", 0)
            df.getColumnNames().eachWithIndex { String key, int c ->
                sheet.addCell(new Label(c, 0, key as String))
            }
            df.getRows().eachWithIndex { Map<String, Object> map, int r ->
                map.keySet().eachWithIndex { String key, int c ->
                    sheet.addCell(new Label(c, r + 1, map.get(key) as String))
                }
            }
            workbook.write()
            return df.size()
        } catch (Exception e) {
            logger.error("", e)
        } finally {
            workbook?.close()
        }

        0
    }
}