package cloud.agileframework.common.util.db;

import cloud.agileframework.common.util.generator.IDUtil;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import junit.framework.TestCase;

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

}