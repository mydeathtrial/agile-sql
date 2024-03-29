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
                "jdbc:mysql://127.0.0.1:3306/am?serverTimezone=GMT%2B8&useSSL=false&useUnicode=true&characterEncoding=utf-8&zeroDateTimeBehavior=CONVERT_TO_NULL&autoReconnect=true&allowPublicKeyRetrieval=true",
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