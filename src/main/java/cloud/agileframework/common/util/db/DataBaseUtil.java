package cloud.agileframework.common.util.db;


import cloud.agileframework.common.constant.Constant;
import cloud.agileframework.common.util.clazz.TypeReference;
import cloud.agileframework.common.util.object.ObjectUtil;
import cloud.agileframework.common.util.pattern.PatternUtil;
import cloud.agileframework.sql.SqlUtil;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import lombok.Data;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Stream;

/**
 * @author 佟盟 on 2018/9/7
 */
public class DataBaseUtil {
    private static final Logger logger = LoggerFactory.getLogger(DataBaseUtil.class);
    private static Connection conn;

    private static ResultSet getResultSet(PATTERN type, String url, String username, String password, String pattern) {

        DBInfo dbInfo = parseDBUrl(url);
        if (dbInfo == null) {
            throw new RuntimeException("无法识别的数据库类型");
        }
        try {
            conn = getConnection(url, username, password);
        } catch (Exception e) {
            e.printStackTrace();
        }

        pattern = pattern == null ? "%" : pattern;
        ResultSet rs = null;
        try {
            // 获取Meta信息对象
            DatabaseMetaData meta = conn.getMetaData();
            // 数据库的用户
            String schemaPattern = null;
            String catalog = null;
            switch (type) {
                case TABLE:
                    String[] types = {"TABLE", "VIEW"};

                    if (dbInfo.type == DB.ORACLE) {
                        schemaPattern = username;
                        if (null != schemaPattern) {
                            schemaPattern = schemaPattern.toUpperCase();
                        }
                        rs = meta.getTables(null, schemaPattern, pattern, types);
                    } else if (dbInfo.type == DB.MYSQL) {
                        schemaPattern = dbInfo.getName();
                        rs = meta.getTables(schemaPattern, schemaPattern, pattern, types);
                    } else if (dbInfo.type == DB.DB2) {
                        schemaPattern = dbInfo.getName();
                        rs = meta.getTables(null, schemaPattern, pattern, types);
                    } else {
                        rs = meta.getTables(null, null, pattern, types);
                    }
                    break;
                case COLUMN:
                    pattern = pattern.toUpperCase();
                    String columnNamePattern = null;
                    if (DB.ORACLE == dbInfo.type) {
                        schemaPattern = username;
                        if (null != schemaPattern) {
                            schemaPattern = schemaPattern.toUpperCase();
                        }
                    } else if (DB.MYSQL == dbInfo.type) {
                        catalog = dbInfo.getName();
                        schemaPattern = dbInfo.getName();
                    }

                    rs = meta.getColumns(catalog, schemaPattern, pattern, columnNamePattern);
                    break;
                case PRIMARY_KEY:
                    pattern = pattern.toUpperCase();
                    if (DB.ORACLE == dbInfo.type) {
                        schemaPattern = username;
                        if (null != schemaPattern) {
                            schemaPattern = schemaPattern.toUpperCase();
                        }
                    }

                    rs = meta.getPrimaryKeys(null, schemaPattern, pattern);
                    break;
                default:
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return rs;
    }

    /**
     * 列出数据库的所有表
     */
    public static List<Map<String, Object>> listTables(String url, String username, String password, String tableName) {
        if (tableName.contains(Constant.RegularAbout.COMMA)) {
            String[] tables = tableName.replaceAll("((?![%-])\\W)+", Constant.RegularAbout.COMMA).split(Constant.RegularAbout.COMMA);
            List<Map<String, Object>> list = new ArrayList<>();
            for (String table : tables) {
                list.addAll(getDBInfo(PATTERN.TABLE, url, username, password, table));
            }
            return list;
        }
        return getDBInfo(PATTERN.TABLE, url, username, password, tableName.trim());
    }

    /**
     * 列出表的所有字段
     */
    public static List<Map<String, Object>> listColumns(String url, String username, String password, String tableName) {
        List<Map<String, Object>> list = getDBInfo(PATTERN.COLUMN, url, username, password, tableName);
        List<Map<String, Object>> keyList = listPrimayKeys(url, username, password, tableName);
        for (Map<String, Object> keyColumn : keyList) {
            for (Map<String, Object> column : list) {
                boolean isPrimaryKey = false;
                if (keyColumn.get("COLUMN_NAME").toString().equals(column.get("COLUMN_NAME").toString())) {
                    isPrimaryKey = true;
                }
                column.put("IS_PRIMARY_KEY", isPrimaryKey);
            }
        }
        return list;
    }

    /**
     * 列出表的所有主键
     */
    public static List<Map<String, Object>> listPrimayKeys(String url, String username, String password, String tableName) {
        return getDBInfo(PATTERN.PRIMARY_KEY, url, username, password, tableName);
    }

    /**
     * 列出表的所有主键
     */
    public static List<Map<String, Object>> getDBInfo(PATTERN pattern, String url, String username, String password, String tableName) {
        List<Map<String, Object>> list = null;
        ResultSet rs = null;
        try {
            rs = getResultSet(pattern, url, username, password, tableName);
            list = parseResultSetToMapList(rs);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            close(rs);
            close(conn);
        }
        return list;
    }

    public static DBInfo parseDBUrl(String url) {
        DB db = Stream.of(DB.values())
                .filter(node -> PatternUtil.matches(node.parsingUrlRegx, url))
                .findFirst()
                .orElse(null);

        if (db == null) {
            return null;
        }
        Map<String, String> map = PatternUtil.getGroups(db.parsingUrlRegx, url.replace(" ", ""));
        if (map == null) {
            return null;
        }
        DBInfo dbInfo = ObjectUtil.to(map, new TypeReference<DBInfo>() {
        });
        dbInfo.setType(db);
        return dbInfo;
    }

    /**
     * 获取JDBC连接
     */
    public static Connection getConnection(String url, String username, String password) throws SQLException {
        Properties info = new Properties();
        info.put("user", username);
        info.put("password", password);
        // !!! Oracle 如果想要获取元数据 REMARKS 信息,需要加此参数
        info.put("remarksReporting", "true");
        // !!! MySQL 标志位, 获取TABLE元数据 REMARKS 信息
        info.put("useInformationSchema", "true");
        return DriverManager.getConnection(url, info);
    }

    /**
     * 将一个未处理的ResultSet解析为Map列表.
     */
    private static List<Map<String, Object>> parseResultSetToMapList(ResultSet rs) {
        List<Map<String, Object>> result = new ArrayList<>();
        if (null == rs) {
            return result;
        }
        try {
            while (rs.next()) {
                Map<String, Object> map = parseResultSetToMap(rs);
                result.add(map);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return result;
    }

    /**
     * 解析ResultSet的单条记录,不进行 ResultSet 的next移动处理
     */
    private static Map<String, Object> parseResultSetToMap(ResultSet rs) {
        if (null == rs) {
            return null;
        }
        final int length = 16;
        Map<String, Object> map = new HashMap<>(length);
        try {
            ResultSetMetaData meta = rs.getMetaData();
            int colNum = meta.getColumnCount();
            for (int i = 1; i <= colNum; i++) {
                // 列名
                String name = meta.getColumnLabel(i);
                Object value = rs.getObject(i);
                // 加入属性
                map.put(name, value);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return map;
    }

    public static void close(Connection conn) {
        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    public static void close(ResultSet rs) {
        if (rs != null) {
            try {
                rs.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    // ResultSetMetaData 使用示例
    public static void demoResultSetMetaData(ResultSetMetaData data) throws SQLException {
        for (int i = 1; i <= data.getColumnCount(); i++) {
            // 获得所有列的数目及实际列数
            int columnCount = data.getColumnCount();
            // 获得指定列的列名
            String columnName = data.getColumnName(i);
            // 获得指定列的数据类型
            int columnType = data.getColumnType(i);
            // 获得指定列的数据类型名
            String columnTypeName = data.getColumnTypeName(i);
            // 所在的Catalog名字
            String catalogName = data.getCatalogName(i);
            // 对应数据类型的类
            String columnClassName = data.getColumnClassName(i);
            // 在数据库中类型的最大字符个数
            int columnDisplaySize = data.getColumnDisplaySize(i);
            // 默认的列的标题
            String columnLabel = data.getColumnLabel(i);
            // 获得列的模式
            String schemaName = data.getSchemaName(i);
            // 某列类型的精确度(类型的长度)
            int precision = data.getPrecision(i);
            // 小数点后的位数
            int scale = data.getScale(i);
            // 获取某列对应的表名
            String tableName = data.getTableName(i);
            // 是否自动递增
            boolean isAutoInctement = data.isAutoIncrement(i);
            // 在数据库中是否为货币型
            boolean isCurrency = data.isCurrency(i);
            // 是否为空
            int isNullable = data.isNullable(i);
            // 是否为只读
            boolean isReadOnly = data.isReadOnly(i);
            // 能否出现在where中
            boolean isSearchable = data.isSearchable(i);
            logger.info(columnCount + "");
            logger.info("获得列" + i + "的字段名称:" + columnName);
            logger.info("获得列" + i + "的类型,返回SqlType中的编号:" + columnType);
            logger.info("获得列" + i + "的数据类型名:" + columnTypeName);
            logger.info("获得列" + i + "所在的Catalog名字:" + catalogName);
            logger.info("获得列" + i + "对应数据类型的类:" + columnClassName);
            logger.info("获得列" + i + "在数据库中类型的最大字符个数:" + columnDisplaySize);
            logger.info("获得列" + i + "的默认的列的标题:" + columnLabel);
            logger.info("获得列" + i + "的模式:" + schemaName);
            logger.info("获得列" + i + "类型的精确度(类型的长度):" + precision);
            logger.info("获得列" + i + "小数点后的位数:" + scale);
            logger.info("获得列" + i + "对应的表名:" + tableName);
            logger.info("获得列" + i + "是否自动递增:" + isAutoInctement);
            logger.info("获得列" + i + "在数据库中是否为货币型:" + isCurrency);
            logger.info("获得列" + i + "是否为空:" + isNullable);
            logger.info("获得列" + i + "是否为只读:" + isReadOnly);
            logger.info("获得列" + i + "能否出现在where中:" + isSearchable);
        }
    }

    /**
     * 数据库信息
     */
    @Data
    public static class DBInfo {
        private DB type;
        private String ip;
        private String port;
        private String name;
    }

    /**
     * 数据库类型,枚举
     */
    public enum DB {
        /**
         * 数据库类型
         */
        ORACLE(Constant.RegularAbout.ORACLE),
        MYSQL(Constant.RegularAbout.MYSQL),
        SQL_SERVER(Constant.RegularAbout.SQL_SERVER),
        SQL_SERVER2005(Constant.RegularAbout.SQL_SERVER2005),
        DB2(Constant.RegularAbout.DB2),
        INFORMIX(Constant.RegularAbout.INFORMIX),
        SYBASE(Constant.RegularAbout.SYBASE),
        OTHER(null);

        private final String parsingUrlRegx;

        DB(String parsingUrlRegx) {
            this.parsingUrlRegx = parsingUrlRegx;
        }

    }

    /**
     * 匹配类型
     */
    public enum PATTERN {
        /**
         * 表
         */
        TABLE,
        /**
         * 字段
         */
        COLUMN,
        /**
         * 主键
         */
        PRIMARY_KEY
    }

    public static <T> List<T> query(Connection connection, String sql, Class<T> clazz, Object param) {
        List<T> list = Lists.newArrayList();
        try (
                Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery(SqlUtil.parserSQL(sql, param));
        ) {
            if (clazz == String.class) {
                // 展开结果集数据库
                while (resultSet.next()) {
                    list.add((T) resultSet.getString(Constant.NumberAbout.ONE));
                }
            } else {
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
                    list.add(ObjectUtil.to(map, new TypeReference<>(clazz)));
                }
            }

        } catch (Exception ignored) {
        }
        return list;
    }

}
