# agile-sql : 动态sql解析器
设计初衷是为JPA规范下持久层框架提供类似于MyBatis一样的动态sql处理能力，该组件在Alibaba Druid的SQL语法解析能力基础上，增加了根据调用参数动态
判断剔除语法段能力，工具中对sql的语法处理均以MySQL语法为基准，如果需要转换为其他数据库可以全局替换为druid中其他数据库语法解析。
----
[![](https://img.shields.io/badge/druid-LATEST-green)](https://img.shields.io/badge/druid-LATEST-green)
[![](https://img.shields.io/badge/build-maven-green)](https://img.shields.io/badge/build-maven-green)
## 它有什么作用

* **占位符解析**
提供`${}`动态占位，不对该参数进行注入监测
提供`{}`普通占位，对该参数进行注入监测

* **占位符默认值解析**
冒号分隔，当占位符key获取不到时，以defaultValue部分语句作为新语句进行替换
提供`${key:defaultValue}`
提供`{key:defaultValue}`
  
* **SQL注入分析**
提供生成hibernate风格参数占位，用于hibernate预编译，有效避免sql注入
当参数集中包含注入风险参数时，抛出sql解析异常，防范SQL注入

* **动态剔除语法段**
根据参数集与sql占位符的匹配，当参数为空时，动态剔除对应的语法段，如where条件，select字段等等，并确保最终生成语句可准确执行

* **生成count语句**
提供sql动态解析同时，根据查询语句生成有效的count查询语句，一般用于分页功能，提供总条数计数能力

* **in语句**
支持将集合参数分解成in语句条件，实现类似mybatis的循环语句

-------
## 快速入门
开始你的第一个项目是非常容易的。

#### 步骤 1: 下载包
您可以从[最新稳定版本]下载包(https://github.com/mydeathtrial/agile-sql/releases).
该包已上传至maven中央仓库，可在pom中直接声明引用

以版本agile-sql-2.0.0.jar为例。
#### 步骤 2: 添加maven依赖
```xml
<!--声明中央仓库-->
<repositories>
    <repository>
        <id>cent</id>
        <url>https://repo1.maven.org/maven2/</url>
    </repository>
</repositories>
<!--声明依赖-->
<dependency>
    <groupId>cloud.agileframework</groupId>
    <artifactId>agile-sql</artifactId>
    <version>2.0.0</version>
</dependency>
```
#### 步骤 3: 程序中调用SqlUtil（例）
参数集：parserSQL第二个参数作为参数集，程序将从参数集中取出sql占位符中声明的key值对应的参数，并将其替换到sql语句中，占位符使用大括号形式，例`{key}`，占位符可以是最终值的一部分，如
      模糊查询条件占位`columnA like '%{key}%'`。程序并除in条件语句外，不会为任何参数添加单引号处理，是否添加单引号可以于调用动态SQL处理前自行添加。
```java
public class YourClass {
    public void test() {
        //声明参数集，参数集也可以是pojo对象，并支持对象嵌套，多层参数时使用点分隔形式声明占位符
        Map<String, Object> param = Maps.newHashMap();;
        param.put("a", "aColumn");
        param.put("b", "b");
        param.put("c", 12);
        param.put("d", new String[]{"c1", "c2"});

        //举例使用嵌套对象
        Demo g = new Demo();
        g.setC(Lists.newArrayList("in1", "in2"));
        param.put("g", g);

        String sql = SqlUtil.parserSQL("select {a},bColumn from your_table " +
         "where c = {c} and d in {d} and e = {e} and f in {f} or g in {g}",param);
        logger.debug(sql);
    }

    private static class Demo {
        private List<Object> c;

        public List<Object> getC() {
            return c;
        }

        public void setC(List<Object> c) {
            this.c = c;
        }
    }

}
```
结果日志
```
SELECT aColumn, bColumn
FROM your_table
WHERE c = 12
	AND d IN ('c1', 'c2')
	OR g IN ('in1', 'in2')
```

#### 步骤 4: count语句
使用方法同SqlUtil.parserSQL相同，方法为SqlUtil.parserCountSQL，其返回结果为count查询语句
```java
public class Test{
    public void test(){
        String sql = SqlUtil.parserSQL("select {a},bColumn from your_table where c = {c} and d in {d} and e = {e} and f in {f} or g in {g}",param);
    }
}
```
结果日志
```
select count(1) from (SELECT aColumn, bColumn
FROM your_table
WHERE c = 12
	AND d IN ('c1', 'c2')
	OR g IN ('in1', 'in2')) _select_table
```