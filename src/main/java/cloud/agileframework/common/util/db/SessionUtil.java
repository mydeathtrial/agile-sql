package cloud.agileframework.common.util.db;

import cloud.agileframework.common.util.clazz.ClassUtil;
import cloud.agileframework.common.util.clazz.TypeReference;
import cloud.agileframework.common.util.object.ObjectUtil;
import cloud.agileframework.sql.SqlUtil;
import com.alibaba.druid.DbType;
import com.alibaba.druid.util.JdbcUtils;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import lombok.extern.slf4j.Slf4j;

import java.sql.*;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * @author 佟盟
 * 日期 2021-06-08 13:05
 * 描述 TODO
 * @version 1.0
 * @since 1.0
 */
@Slf4j
public class SessionUtil {
    //--------------------------------查询-----------------------------------------------

    /**
     * 查询并且将结果转换为java对象
     *
     * @param connection 连接
     * @param sql        sql
     * @param clazz      结果要转换的类型
     * @param param      sql占位参数
     * @param <T>        泛型
     * @return 集合
     */
    public static <T> List<T> query(Connection connection, String sql, Class<T> clazz, Object param) {
        List<Map<String, Object>> temp = query(connection, sql, param);
        return toJavaObjectList(clazz, temp);
    }

    public static <T> List<T> query(Connection connection, String sql, Class<T> clazz) {
        List<Map<String, Object>> temp = query(connection, sql, Maps.newHashMap());
        return toJavaObjectList(clazz, temp);
    }

    /**
     * 查询
     *
     * @param connection 连接
     * @param sql        sql
     * @param param      sql占位参数
     * @return 数据
     */
    public static List<Map<String, Object>> query(Connection connection, String sql, Object param) {
        String newSql = parseSql(connection, sql, param);
        return execute(connection, newSql);
    }

    public static List<Map<String, Object>> query(Connection connection, String sql) {
        String newSql = parseSql(connection, sql, null);
        return execute(connection, newSql);
    }

//--------------------------------分页-----------------------------------------------

    public static <T> List<T> limit(Connection connection, String sql, Class<T> clazz, Object param, int offset, int count) {
        List<Map<String, Object>> list = execute(connection, parseLimitSql(connection, sql, param, offset, count));
        return toJavaObjectList(clazz, list);
    }

    public static <T> List<T> limit(Connection connection, String sql, Class<T> clazz, int offset, int count) {
        List<Map<String, Object>> list = execute(connection, parseLimitSql(connection, sql, null, offset, count));
        return toJavaObjectList(clazz, list);
    }

    public static List<Map<String, Object>> limit(Connection connection, String sql, Object param, int offset, int count) {
        return execute(connection, parseLimitSql(connection, sql, param, offset, count));
    }

    public static List<Map<String, Object>> limit(Connection connection, String sql, int offset, int count) {
        return execute(connection, parseLimitSql(connection, sql, null, offset, count));
    }

//--------------------------------统计-----------------------------------------------

    public static long count(Connection connection, String sql, Object param) {
        List<Map<String, Object>> list = execute(connection, parseCountSql(connection, sql, param));
        if (list.isEmpty()) {
            return 0;
        }
        return Long.parseLong(list.stream().flatMap(a -> a.values().stream()).findFirst().orElse("0").toString());
    }

    public static long count(Connection connection, String sql) {
        List<Map<String, Object>> list = execute(connection, parseCountSql(connection, sql, null));
        if (list.isEmpty()) {
            return 0;
        }
        return Long.parseLong(list.stream().flatMap(a -> a.values().stream()).findFirst().orElse("0").toString());
    }

    /**
     * List Map转换为List clazz
     *
     * @param clazz 转换后的List元素类型
     * @param data  转换前的数据
     * @param <T>   泛型
     * @return List clazz
     */
    public static <T> List<T> toJavaObjectList(Class<T> clazz, List<Map<String, Object>> data) {
        if (data.isEmpty()) {
            return Lists.newArrayList();
        }
        if (ClassUtil.isWrapOrPrimitive(clazz) || Date.class.isAssignableFrom(clazz)|| String.class==clazz) {
            return data.stream().flatMap(n -> n.values().stream()).map(a -> (T) ObjectUtil.to(a, new TypeReference<>(clazz))).collect(Collectors.toList());
        }
        return data.parallelStream().map(a -> (T) ObjectUtil.to(a, new TypeReference<>(clazz))).collect(Collectors.toList());
    }

    //--------------------------------三种sql语句------------------------------------------
    private static String parseSql(Connection connection, String sql, Object param) {
        String newSql = sql;
        try {
            DbType dbType = JdbcUtils.getDbTypeRaw(connection.getMetaData().getURL(), null);
            newSql = SqlUtil.parserSQLByType(dbType, sql, param);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return newSql;
    }

    private static String parseLimitSql(Connection connection, String sql, Object param, int offset, int count) {
        String newSql = sql;
        try {
            DbType dbType = JdbcUtils.getDbTypeRaw(connection.getMetaData().getURL(), null);
            newSql = SqlUtil.parserLimitSQLByType(dbType, sql, param, offset, count);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return newSql;
    }


    private static String parseCountSql(Connection connection, String sql, Object param) {
        String newSql = sql;
        try {
            DbType dbType = JdbcUtils.getDbTypeRaw(connection.getMetaData().getURL(), null);
            newSql = SqlUtil.parserCountSQLByType(dbType, sql, param);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return newSql;
    }

    //---------------------------------执行器-------------------------------------------
    public static List<Map<String, Object>> execute(Connection connection, String sql) {
        List<Map<String, Object>> list = Lists.newArrayList();
        try (
                Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery(sql)
        ) {
            List<String> columns = Lists.newArrayList();
            ResultSetMetaData metaData = resultSet.getMetaData();
            for (int i = 1; i <= metaData.getColumnCount(); i++) {
                columns.add(metaData.getColumnName(i));
            }

            // 展开结果集数据库
            while (resultSet.next()) {
                Map<String, Object> map = Maps.newHashMap();
                columns.forEach(column -> {
                    try {
                        map.put(column, resultSet.getString(column));
                    } catch (SQLException ignored) {
                    }
                });
                list.add(map);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        return list;
    }

    private static <T> T preparedParseSql(Connection connection, String sql, Object param, Function<PreparedStatement, T> function) {
        Map<String, Object> params = Maps.newHashMap();
        try (
                PreparedStatement statement = connection.prepareStatement(SqlUtil.parserSQLByType(JdbcUtils.getDbTypeRaw(connection.getMetaData().getURL(), null), sql, param, params));
        ) {
            for (Map.Entry<String, Object> e : params.entrySet()) {
                statement.setObject(Integer.parseInt(e.getKey()), e.getValue());
            }

            return function.apply(statement);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public static int update(Connection connection, String sql) {
        return update(connection, sql, Maps.newHashMap());
    }

    public static int update(Connection connection, String sql, Object param) {
        Integer count = preparedParseSql(connection, sql, param, a -> {
            if (a == null) {
                return 0;
            }
            try {
                return a.executeUpdate();
            } catch (SQLException throwable) {
                throwable.printStackTrace();
            }
            return 0;
        });
        return count == null ? 0 : count;
    }

    public static void batchUpdate(Connection connection, List<String> sql) {
        try (
                Statement statement = connection.createStatement();
        ) {
            for (String s : sql) {
                statement.addBatch(s);
            }
            statement.executeBatch();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 批量更新
     *
     * @param connection 连接
     * @param sql        sql
     * @param params     sql的占位参数集合，一个元素就是一条数据
     */
    public static void batchUpdate(Connection connection, String sql, List<Map<String, Object>> params) throws SQLException {

        Map<String, List<Map<String, Object>>> batches = Maps.newHashMap();
        Map<String, Object> temp = Maps.newHashMap();
        for (Map<String, Object> map : params) {
            String prepareSql = SqlUtil.parserSQLByType(JdbcUtils.getDbTypeRaw(connection.getMetaData().getURL(), null), sql, map, temp);

            List<Map<String, Object>> values = batches.get(prepareSql);
            if (values == null) {
                values = Lists.newArrayList();
            }
            values.add(Maps.newHashMap(temp));
            batches.put(prepareSql, values);
            temp.clear();
        }

        for (Map.Entry<String, List<Map<String, Object>>> entry : batches.entrySet()) {
            try (
                    PreparedStatement statement = connection.prepareStatement(entry.getKey());
            ) {
                for (Map<String, Object> param : entry.getValue()) {
                    for (Map.Entry<String, Object> e : param.entrySet()) {
                        statement.setObject(Integer.parseInt(e.getKey()), e.getValue());
                    }
                    statement.addBatch();
                }
                statement.executeBatch();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

}
