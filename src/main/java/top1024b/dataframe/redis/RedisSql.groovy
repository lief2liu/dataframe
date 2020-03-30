package top1024b.dataframe.redis

import groovy.transform.CompileStatic

/**
 * Redis connection information
 */
@CompileStatic
class RedisSql implements Serializable {
    private String field
    private String tb
    private String order
    private boolean asc = true
    private Integer limit
    private Integer offset = 0

    String getField() {
        return field
    }

    void setField(String field) {
        this.field = field
    }

    String getTb() {
        return tb
    }

    void setTb(String tb) {
        this.tb = tb
    }

    String getOrder() {
        return order
    }

    void setOrder(String order) {
        this.order = order
    }

    boolean getAsc() {
        return asc
    }

    void setAsc(boolean asc) {
        this.asc = asc
    }

    Integer getLimit() {
        return limit
    }

    void setLimit(int limit) {
        this.limit = limit
    }

    Integer getOffset() {
        return offset
    }

    void setOffset(int offset) {
        this.offset = offset
    }
}

