package top1024b.dataframe.common


import groovy.transform.CompileStatic

/**
 * common page bean
 */
@CompileStatic
class Page implements Serializable {
    final static String SIZE = "size"
    final static String NO = "no"
    final static String TOTAL = "total"

    private int size = 10
    private int no = 1
    private long total

    Page() {}

    Page(int size, int no) {
        this.size = size
        this.no = no
    }

    Map<String, Integer> getMap() {
        [
                (Page.SIZE) : size,
                (Page.NO)   : no,
                (Page.TOTAL): total,
        ] as Map<String, Integer>
    }

    int getSize() {
        return size
    }

    void setSize(int size) {
        this.size = size
    }

    int getNo() {
        return no
    }

    void setNo(int no) {
        this.no = no
    }

    long getTotal() {
        return total
    }

    void setTotal(long total) {
        this.total = total
    }

    int getOffset() {
        (no - 1) * size
    }
}

