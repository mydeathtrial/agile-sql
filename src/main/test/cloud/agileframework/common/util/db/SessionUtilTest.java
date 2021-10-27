package cloud.agileframework.common.util.db;

import cloud.agileframework.common.util.generator.IDUtil;
import com.alibaba.druid.DbType;
import com.alibaba.druid.sql.builder.SQLBuilderFactory;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import junit.framework.TestCase;
import lombok.SneakyThrows;
import org.apache.calcite.adapter.elasticsearch.ElasticsearchRel;
import org.apache.calcite.adapter.elasticsearch.ElasticsearchSchemaFactory;
import org.apache.calcite.config.Lex;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.dialect.HiveSqlDialect;
import org.apache.calcite.sql.dialect.OracleSqlDialect;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SessionUtilTest extends TestCase {

    public void testUpdate() throws SQLException {
        Connection connection = DataBaseUtil.getConnection(
                "jdbc:h2:~/projectdb;AUTO_SERVER=TRUE;DB_CLOSE_DELAY=-1",
                "root",
                "123456");

        List<Map<String,Object>> list = Lists.newArrayList();
        int i =10000;
        while (i>0){
            HashMap<String, Object> map = Maps.newHashMap();
            map.put("id", IDUtil.generatorId());
            map.put("name", "test"+(i--));
            list.add(map);
        }
        SessionUtil.batchUpdate(connection,"insert into sys_api (sys_api_id,name) values ({id},{name})",list);
    }

    public void testGenerate() throws SqlParseException {
        String s = SQLBuilderFactory.createSelectSQLBuilder(DbType.mysql)
                .selectWithAlias("name", "apiName")
                .select("name", "id").from("sys_api")
                .groupBy("name")
                .whereAnd("id=2").orderBy().toString();
        System.out.println(s);

        // 解析配置 - mysql设置
        SqlParser.Config mysqlConfig = SqlParser.config().withLex(Lex.MYSQL);
        // 创建解析器
        SqlParser parser = SqlParser.create("", mysqlConfig);
        // Sql语句
        String sql = "select * from sys_menu ORDER BY MENU_PID";
        // 解析sql
        SqlNode sqlNode = parser.parseQuery(sql);
        // 还原某个方言的SQL
        System.out.println(sqlNode.toSqlString(HiveSqlDialect.DEFAULT));
        // sql validate（会先通过Catalog读取获取相应的metadata和namespace）
    }
}