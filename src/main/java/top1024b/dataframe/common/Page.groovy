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

    private int size
    private int no
    private int total

    Page(){}

    Page(int size, int no) {
        this.size = size
        this.no = no
    }

    Map<String, Integer> getMap() {
        [
                (Page.SIZE): size,
                (Page.NO)  : no
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

    int getTotal() {
        return total
    }

    void setTotal(int total) {
        this.total = total
    }
}

