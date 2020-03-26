package top1024b.dataframe.common

import groovy.transform.CompileStatic

/**
 * every datasource should implements this interface to read from
 */
@CompileStatic
interface DataFrameReader {
    DataFrame read()
}
