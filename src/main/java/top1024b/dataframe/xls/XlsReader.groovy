package top1024b.dataframe.xls


import groovy.transform.CompileStatic
import javafx.scene.control.Cell
import jxl.Sheet
import jxl.Workbook
import jxl.write.Label
import jxl.write.WritableSheet
import jxl.write.WritableWorkbook
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import top1024b.dataframe.common.DataFrame
import top1024b.dataframe.common.DataFrameReader
import top1024b.dataframe.common.DataFrameWriter

@CompileStatic
class XlsReader implements DataFrameReader, Serializable {

    Logger logger = LoggerFactory.getLogger(this.getClass())
    String file

    XlsReader(String file) {
        this.file = file
    }


    @Override
    DataFrame read() {
        File file = new File(file)
        Workbook workbook = Workbook.getWorkbook(file)
        Sheet sheet = workbook.getSheet(0)
        int rowSize = sheet.getRows()
        int colSize = sheet.getColumns()
        List<String> fields = (0..colSize - 1).collect { int y ->
            sheet.getCell(y, 0).getContents()
        }


        List<Map<String, Object>> rows = (1..rowSize - 1).collect { int x ->
            Map<String, Object> row = [:] as Map<String, Object>
            (0..colSize - 1).each { int y ->
                String content = sheet.getCell(y, x).getContents()
                row.put(fields[y], content)
            }

            row
        }

        DataFrame.read(rows, null)
    }
}