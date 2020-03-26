package top1024b.dataframe.jdbc


import groovy.transform.CompileStatic

/**
 * jdbc sql util
 */
@CompileStatic
class JdbcSql implements Serializable {
    final String WHERE_JOIN_AND = " and "
    final String WHERE_JOIN_OR = " or "

    /**
     *  get sql like where a=? and b=? by Map, and put the value to values
     */
    String whereAnd(Map<String, Object> condition, List<Object> values) {
        String and = andOr(condition, values, WHERE_JOIN_AND)
        if (!and) {
            return ""
        }

        return "where $and"
    }

    /**
     * get sql like a = ? and b= ? ,and put the value to values
     */
    String and(Map<String, Object> condition, List<Object> values) {
        andOr(condition, values, WHERE_JOIN_AND)
    }

    /**
     * get sql like a = ? or b= ? ,and put the value to values
     */
    String or(Map<String, Object> condition, List<Object> values) {
        andOr(condition, values, WHERE_JOIN_OR)
    }

    private String andOr(Map<String, Object> condition, List<Object> values, String type) {
        condition.keySet().each {
            if (condition.get(it) == null) {
                condition.remove(it)
            }
        }

        String field = condition.keySet().join(",")
        values.addAll(condition.values())

        if (!field) {
            return null
        }
        field.split(",").collect({ "$it=?" }).join(type)
    }
}

