# dataframe介绍

标签（空格分隔）： doc

---

> dataframe是一个基于java的快速etl框架,可以非常方便的对jdbc、mongodb、elasticsearch等数据源的数据进行读写，大大简化了单个数据源之内，以及数据源与数据源之间的数据传输，使用它读写数据类似使用spark的dataframe的读写


## 场景示例

假如我们在mysql有如下名为**person**的表，用来存储每个人的信息
| id | name | age|
| ------------ | ------------ |
|1	|小明|	30
|2	|小丽|	28
|3	|红红|	32
|4	|jack|	32
<sup>表1-1</sup>

我们需要统计每个年龄（age字段）有多少人，把结果存到mysql中的**age_count**表
我们使用了dataframe后只要以下三行代码即可搞定
```
    String sql = "select age,count(1) as cnt from person group by age";
    DataFrame df = DataFrame.read(new JdbcReader(mysqlInfo, sql));
    df.write(new JdbcWriter(mysqlInfo, "age_count"));
```
如果结果不是保存到mysql而是其他数据源，比如es，需要把最后一行的JdbcWriter改成EsWriter即可，其它数据源的代码可[点此参考实例代码][1] 


##  快速开始
1. 建表
 - 在mysql中建立一张像表1-1的数据表并插入数据
    ```
    CREATE TABLE `person`  (
      `id` int(11) NOT NULL AUTO_INCREMENT,
      `name` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
      `age` int(11) NULL DEFAULT NULL,
      PRIMARY KEY (`id`) USING BTREE,
      INDEX `person_name`(`name`) USING BTREE
    ) ENGINE = InnoDB CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic;
    
    INSERT INTO `person` VALUES (1, '小明', 30);
    INSERT INTO `person` VALUES (2, '小丽', 28);
    INSERT INTO `person` VALUES (3, '红红', 32);
    INSERT INTO `person` VALUES (4, 'jack', 32);
    ```
    

 - 在mysql中建立一张age_count结果表
    ```
    CREATE TABLE `age_count`  (
      `age` int(11) NOT NULL,
      `cnt` int(11) NULL DEFAULT NULL,
      PRIMARY KEY (`age`) USING BTREE
    ) ENGINE = InnoDB CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic;
    ```
2. 开始写代码
    源代码可[点此参考][2]

    - 引入dataframe的依赖
    maven可以在pom中加入以下依赖
    ```
    <dependency>
        <groupId>top.dataframe</groupId>
        <artifactId>dataframe-core</artifactId>
        <version>0.6.3</version>
    </dependency>
    ```
        不用maven可以直接点 [此下载jar包][3]
        
    - 初始化mysql连接信息
    ```
        String jdbcUrl = "jdbc:mysql://192.168.1.1:3306/test" +
                "?allowMultiQueries=true&autoReconnect=true&failOverReadOnly=false" +
                "&useSSL=false&zeroDateTimeBehavior=convertToNull&characterEncoding=utf8";

        Map<String, Object> mysqlInfo = new HashMap<String, Object>() {{
            put(JdbcInfoKey.DRIVER, JdbcDriver.MYSQL);
            put(JdbcInfoKey.USER, "root");
            put(JdbcInfoKey.PASSWORD, "123456");
            put(JdbcInfoKey.URL, jdbcUrl);
        }};
    ```
        上面的mysql的url、用户名、密码根据你的实际情况修改
        
    - 将mysql的数据读取到dataframe中并打印
    ```
        String sql = "select age,count(1) as cnt from person group by age";
        DataFrame df = DataFrame.read(new JdbcReader(mysqlInfo, sql));
        df.show();
    ```
        运行后会控制台会输出如下
        ```
        ------------------------------
        age           |cnt           |
        ------------------------------
        28            |1             |
        30            |1             |
        32            |2             |
        ```
    - 将dataframe写入mysql的age_count表
    ```
    df.write(new JdbcWriter(mysqlInfo, "age_count"));
    ```
        写入后在mysql中可查到结果表如下
        | age | cnt | 
        | ------------ | ------------ |
        |28	|1
        |30	|1
        |32	|2

## dataframe的写入模式

dataframe有以下四种写入模式
```
INSERT
IGNORE
UPSERT
REPLACE
```
以上面的写入mysql的age_count表为例解释一下这4中模式如下（*注意由于age是主键，所以数据重复的判断依据为age相同*）：

- INSERT为直接插入，遇到重复的数据会报错，假如在age_count表中存在一条age为10的记录，再用dataframe插入一条age为10的记录，程序会报错

- IGNORE为忽略插入，遇到重复的数据忽略，假如在age_count表中存在一条age为10的记录，再用dataframe插入一条age为10的记录，这条要插入的数据会被忽略而不改变表中的任何数据

- UPSERT为更新插入，遇到重复的数据更新，不存在的数据直接插入，假如在age_count表中存在一条age为10的记录，再用dataframe插入一条age为10的记录，表中的age为10的数据会被更新成dataframe中age为10的数据

- REPLACE为覆盖插入，遇到重复的数据先删除再插入，假如在age_count表中存在一条age为10的记录，再用dataframe插入一条age为10的记录，表中的age为10的数据会被删除，再插入dataframe中的这条age为10的记录

dataframe的默认写入模式为UPSERT，如果要使用其他写入模式（比如IGNORE），可以这样写
```
df.write(new JdbcWriter(mysqlInfo, "age_count", SaveMode.IGNORE));
```

## dataframe的写入效率
我们拿纯jdbc的批量写入（关闭自动提交）和dataframe做对比，目标表是我们之前说过的person表，参考表1-1
纯jdbc的写入代码如下
```
    static void writeByJdbc(int size) throws Exception {
        System.out.println("using jdbc ...");
        Class.forName(JdbcDriver.MYSQL);
        String jdbcUrl = "jdbc:mysql://192.168.1.1:3306/test" +
                "?allowMultiQueries=true&autoReconnect=true&failOverReadOnly=false" +
                "&useSSL=false&zeroDateTimeBehavior=convertToNull&characterEncoding=utf8";
        Connection conn = DriverManager.getConnection(jdbcUrl, "root", "123456");
        conn.setAutoCommit(false);
        PreparedStatement ps = null;
        String sql = "insert into person(name,age) values (?,?)";
        ps = conn.prepareStatement(sql);
        for (int i = 0; i < size; i++) {
            ps.setString(1, "name" + i);
            ps.setInt(2, i);
            ps.addBatch();
        }
        ps.executeBatch();
        conn.commit();
        ps.clearBatch();
        conn.close();
    }
```
dataframe的写入代码如下
```
     static void writeByDataFrame(int size) {
        System.out.println("using dataframe ...");
        String jdbcUrl = "jdbc:mysql://192.168.1.1:3306/test" +
                "?allowMultiQueries=true&autoReconnect=true&failOverReadOnly=false" +
                "&useSSL=false&zeroDateTimeBehavior=convertToNull&characterEncoding=utf8";

        Map<String, Object> mysqlInfo = new HashMap<String, Object>() {{
            put(JdbcInfoKey.DRIVER, JdbcDriver.MYSQL);
            put(JdbcInfoKey.USER, "root");
            put(JdbcInfoKey.PASSWORD, "123456");
            put(JdbcInfoKey.URL, jdbcUrl);
        }};

        List<Map<String,Object>> data = new LinkedList<>();
        for (int i = 0; i < size; i++) {
            Map<String,Object> row = new HashMap<String, Object>();
            row.put("name", "name" + i);
            row.put("age", i);
            data.add(row);
        }

        DataFrame df = DataFrame.read(new ListReader(data));

        df.write(new JdbcWriter(mysqlInfo, "person"));
    }
```

对比的代码如下
```
public static void main(String[] args) throws Exception {
        long start = System.currentTimeMillis();
        int size = 20000;

//        writeByJdbc(size);
//        writeByDataFrame(size);

        System.out.print("write cost ");
        System.out.print(System.currentTimeMillis() - start);
        System.out.println("ms");
    }
```
我们拿2万条数据做对比，大家可以直接[点此下载源码][4]，修改size进行比较，以下是我运行的一次对比截图：
![aaaa.png-18.5kB][5]

![bbbb.png-19.9kB][6]

经过我多次试验
jdbc批量写入的方式时间消耗在20秒左右
dataframe的方式时间消耗1.2秒左右
dataframe的方式比直接用jdbc批量写入的方式快了16倍左右


----------


<center style='color: #999'>[粤ICP备19124828号][7]</center>


  [1]: https://github.com/lief2liu/dataframe/tree/master/src/test/java/top/dataframe
  [2]:  https://github.com/lief2liu/dataframe/blob/master/src/test/java/top/dataframe/QuickStart.java
  [3]: https://repo1.maven.org/maven2/top/dataframe/dataframe-core/0.6.3/dataframe-core-0.6.3.jar
  [4]: https://github.com/lief2liu/dataframe/tree/master/src/test/java/top/dataframe/CompareJdbc.java
  [5]: http://static.zybuluo.com/lief/1s8olvmt7k2ai2pikug1odvl/aaaa.png
  [6]: http://static.zybuluo.com/lief/doeqg3bpy76izjjsnfvinr86/bbbb.png
  [7]: http://www.beian.miit.gov.cn