package top1024b.dataframe.common


import groovy.transform.CompileStatic

/**
 * every datasource should implements this interface to write to
 */
@CompileStatic
interface DataFrameWriter {
    int write(DataFrame df)
}
