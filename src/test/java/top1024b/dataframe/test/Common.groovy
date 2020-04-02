package top1024b.dataframe.test


import groovy.transform.CompileStatic

@CompileStatic
class Common implements Serializable {
    final static String jdbcUrl = """
        jdbc:mysql://192.168.1.1:3306/test?allowMultiQueries=true&autoReconnect=true&failOverReadOnly=false&useSSL=false&zeroDateTimeBehavior=convertToNull&characterEncoding=utf8
    """.toString().trim()

    final static String redisHost = "192.168.1.1"
    final static String redisPassword = "123456"

    final static String json = """
                [
                    {
                        "id": "320100",
                        "name": "南京市",
                        "fid": "320000",
                        "level_id": "2"
                    },
                    {
                        "id": "320200",
                        "name": "无锡市",
                        "fid": "320000",
                        "level_id": "2"
                    },
                    {
                        "id": "320300",
                        "name": "徐州市",
                        "fid": "320000",
                        "level_id": "2"
                    },
                    {
                        "id": "320400",
                        "name": "常州市",
                        "fid": "320000",
                        "level_id": "2"
                    },
                    {
                        "id": "320500",
                        "name": "苏州市",
                        "fid": "320000",
                        "level_id": "2"
                    },
                    {
                        "id": "320600",
                        "name": "南通市",
                        "fid": "320000",
                        "level_id": "2"
                    },
                    {
                        "id": "320700",
                        "name": "连云港市",
                        "fid": "320000",
                        "level_id": "2"
                    },
                    {
                        "id": "320800",
                        "name": "淮安市",
                        "fid": "320000",
                        "level_id": "2"
                    },
                    {
                        "id": "320900",
                        "name": "盐城市",
                        "fid": "320000",
                        "level_id": "2"
                    },
                    {
                        "id": "321000",
                        "name": "扬州市",
                        "fid": "320000",
                        "level_id": "2"
                    },
                    {
                        "id": "321100",
                        "name": "镇江市",
                        "fid": "320000",
                        "level_id": "2"
                    },
                    {
                        "id": "321200",
                        "name": "泰州市",
                        "fid": "320000",
                        "level_id": "2"
                    },
                    {
                        "id": "321300",
                        "name": "宿迁市",
                        "fid": "320000",
                        "level_id": "2"
                    }
                ]
        """.toString().trim()
}