package cloud.agileframework.sql;

import cloud.agileframework.common.util.template.VelocityUtil;
import com.alibaba.druid.DbType;
import com.alibaba.druid.sql.PagerUtils;
import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLObject;
import com.alibaba.druid.sql.ast.SQLOrderBy;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.expr.SQLBetweenExpr;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOpExpr;
import com.alibaba.druid.sql.ast.expr.SQLBooleanExpr;
import com.alibaba.druid.sql.ast.expr.SQLCharExpr;
import com.alibaba.druid.sql.ast.expr.SQLDateTimeExpr;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.expr.SQLInListExpr;
import com.alibaba.druid.sql.ast.expr.SQLInSubQueryExpr;
import com.alibaba.druid.sql.ast.expr.SQLIntegerExpr;
import com.alibaba.druid.sql.ast.expr.SQLMethodInvokeExpr;
import com.alibaba.druid.sql.ast.expr.SQLNullExpr;
import com.alibaba.druid.sql.ast.expr.SQLPropertyExpr;
import com.alibaba.druid.sql.ast.statement.SQLDeleteStatement;
import com.alibaba.druid.sql.ast.statement.SQLExprTableSource;
import com.alibaba.druid.sql.ast.statement.SQLInsertStatement;
import com.alibaba.druid.sql.ast.statement.SQLJoinTableSource;
import com.alibaba.druid.sql.ast.statement.SQLReplaceStatement;
import com.alibaba.druid.sql.ast.statement.SQLSelect;
import com.alibaba.druid.sql.ast.statement.SQLSelectGroupByClause;
import com.alibaba.druid.sql.ast.statement.SQLSelectOrderByItem;
import com.alibaba.druid.sql.ast.statement.SQLSelectQuery;
import com.alibaba.druid.sql.ast.statement.SQLSelectQueryBlock;
import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.druid.sql.ast.statement.SQLSubqueryTableSource;
import com.alibaba.druid.sql.ast.statement.SQLTableSource;
import com.alibaba.druid.sql.ast.statement.SQLUnionQuery;
import com.alibaba.druid.sql.ast.statement.SQLUnionQueryTableSource;
import com.alibaba.druid.sql.ast.statement.SQLUpdateStatement;
import com.alibaba.druid.sql.parser.SQLParserUtils;
import com.alibaba.druid.sql.parser.SQLStatementParser;
import com.alibaba.druid.sql.visitor.SchemaStatVisitor;
import com.alibaba.druid.util.JdbcConstants;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

/**
 * 描述：
 * <p>创建时间：2018/12/6<br>
 *
 * @author 佟盟
 * @version 1.0
 * @since 1.0
 */
public class SqlUtil {
    /**
     * 常量表达式正则
     */
    public static final String CONSTANT_CONDITION_REGEX = "((OR|AND|LIKE)[\\s]+1[\\s]*=[\\s]*1)|(1[\\s]*=[\\s]*1[\\s]+(OR|AND|LIKE))|(^1[\\s]*=[\\s]*1)";
    /**
     * 常量表达式
     */
    public static final String CONSTANT_CONDITION = "1 = 1";
    public static final ThreadLocal<DbType> DB_TYPE_THREAD_LOCAL = new ThreadLocal<>();
    private static final ThreadLocal<Map<String, Object>> QUERY_PARAM_THREAD_LOCAL = new ThreadLocal<>();
    private static Logger log = LoggerFactory.getLogger(SqlUtil.class);

    public static String parserCountSQLByType(DbType dbType, String sql, Object parameters, Map<String, Object> query) {
        return parserSQLByType(dbType, sql, parameters, query, (a, b) -> PagerUtils.count(b, a));
    }

    public static String parserCountSQLByType(DbType dbType, String sql, Object parameters) {
        return parserSQLByType(dbType, sql, parameters, null, (a, b) -> PagerUtils.count(b, a));
    }

    public static String parserCountSQLByType(DbType dbType, String sql) {
        return parserSQLByType(dbType, sql, null, null, (a, b) -> PagerUtils.count(b, a));
    }

    /**
     * 根据给定参数动态生成完成参数占位的查询条数sql语句
     *
     * @param sql        原sql模板
     * @param parameters map格式的sql语句中的参数集合，使用{paramName}方式占位
     * @return 生成的sql结果
     */
    public static String parserCountSQL(String sql, Object parameters, Map<String, Object> query) {
        return parserSQLByType(null, sql, parameters, query, (a, b) -> PagerUtils.count(b, a));
    }

    public static String parserCountSQL(String sql) {
        return parserSQLByType(null, sql, null, null, (a, b) -> PagerUtils.count(b, a));
    }

    public static String parserCountSQL(String sql, Object parameters) {
        return parserSQLByType(null, sql, parameters, null, (a, b) -> PagerUtils.count(b, a));
    }

    public static String parserSQL(String sql, Object parameters) {
        return parserSQLByType(null, sql, parameters, null);
    }

    public static String parserSQL(String sql, Object parameters, Map<String, Object> query) {
        return parserSQLByType(null, sql, parameters, query);
    }

    /**
     * 根据给定参数动态生成完成参数占位的sql语句
     *
     * @param sql 原sql
     * @return 生成的sql结果
     */
    private static String parserSQL(String sql) {
        return parserSQLByType(null, sql, null, null);
    }


    public static String parserSQLByType(DbType dbType, String sql, Object parameters) {
        return parserSQLByType(dbType, sql, parameters, null);
    }

    public static String parserLimitSQLByType(DbType dbType, String sql, Object parameters, int offset, int count) {
        return parserSQLByType(dbType, sql, parameters, null, (a, b) -> PagerUtils.limit(b, a, offset, count));
    }

    public static String parserLimitSQLByType(DbType dbType, String sql, int offset, int count, Map<String, Object> query) {
        return parserSQLByType(dbType, sql, null, query, (a, b) -> PagerUtils.limit(b, a, offset, count));
    }

    public static String parserLimitSQLByType(DbType dbType, String sql, int offset, int count) {
        return parserSQLByType(dbType, sql, null, null, (a, b) -> PagerUtils.limit(b, a, offset, count));
    }

    public static String parserLimitSQLByType(DbType dbType, String sql, Object parameters, int offset, int count, Map<String, Object> query) {
        return parserSQLByType(dbType, sql, parameters, query, (a, b) -> PagerUtils.limit(b, a, offset, count));
    }

    public static String parserSQLByType(DbType dbType, String sql, Object parameters, Map<String, Object> query) {
        return parserSQLByType(dbType, sql, parameters, query, (a, b) -> b);
    }

    public static String parserSQLByType(DbType dbType, String sqlSource, Object parameters, Map<String, Object> query, BiFunction<DbType, String, String> machining) {
        dbType = dbType == null ? DbType.mysql : dbType;
        setQueryParamThreadLocal(query);
        String sql = sqlSource;

        try {
            sql = VelocityUtil.parse(sqlSource, parameters);
        } catch (Exception e) {
            e.printStackTrace();
        }
        try {

            sql = Param.parsingSqlString(sql, Param.parsingParam(parameters));

            Param.parsingPlaceHolder(sql);

            sql = sql.replace("< @", "<  @");
            sql = parserSQLByType(dbType, sql);
            //额外加工，如分页与统计之类
            sql = machining.apply(dbType, sql);
            Map<String, Object> queryParams = QUERY_PARAM_THREAD_LOCAL.get();
            if (queryParams != null) {
                Iterator<Map.Entry<String, Object>> it = queryParams.entrySet().iterator();
                while (it.hasNext()) {
                    Map.Entry<String, Object> e = it.next();
                    String k = e.getKey();
                    Object v = e.getValue();
                    if (v instanceof WhereIn) {
                        sql = sql.replace(k, ((WhereIn) v).sql());
                        it.remove();
                    }
                }

                if (query != null) {
                    String finalSql = sql;
                    List<String> paramSortedList = queryParams.keySet().stream().sorted(Comparator.comparingInt(finalSql::indexOf)).collect(Collectors.toList());
                    int i = 1;
                    Map<String, Object> resolvedQueryParams = Maps.newHashMap();
                    for (String param : paramSortedList) {
                        while (sql.contains(param)) {
                            sql = StringUtils.replaceOnce(sql, param, "?");
                            Object v = queryParams.get(param);
                            resolvedQueryParams.put(String.valueOf(i++), v);
                        }
                    }
                    queryParams.clear();
                    queryParams.putAll(resolvedQueryParams);
                } else {
                    for (String param : queryParams.keySet()) {
                        Object value = queryParams.get(param);
                        if (value instanceof Number) {
                            sql = sql.replace(param, SQLUtils.toSQLString(new SQLIntegerExpr((Number) value), dbType));
                        } else if (value instanceof Boolean) {
                            sql = sql.replace(param, SQLUtils.toSQLString(new SQLBooleanExpr((Boolean) value), dbType));
                        } else if (value instanceof Date) {
                            sql = sql.replace(param, SQLUtils.toSQLString(new SQLDateTimeExpr((Date) value, TimeZone.getDefault())));
                        } else if (value == null) {
                            sql = sql.replace(param, SQLUtils.toSQLString(new SQLNullExpr()));
                        } else {
                            sql = sql.replace(param, SQLUtils.toSQLString(new SQLCharExpr(value.toString()), dbType));
                        }
                    }
                }
            }
        } catch (Exception e) {
            log.error("agile-sql parse exception", e);
            throw e;
        } finally {
            QUERY_PARAM_THREAD_LOCAL.remove();
        }

        return sql;
    }

    private static String parserSQLByType(DbType dbType, String sql) {
        if (dbType == null) {
            dbType = DB_TYPE_THREAD_LOCAL.get();
            dbType = dbType == null ? JdbcConstants.MYSQL : dbType;
        }
        DB_TYPE_THREAD_LOCAL.set(dbType);

        // 新建 MySQL Parser
        SQLStatementParser parser = SQLParserUtils.createSQLStatementParser(sql, dbType);

        // 使用Parser解析生成AST，这里SQLStatement就是AST
        SQLStatement statement = parser.parseStatement();

        // 使用visitor来访问AST
        SchemaStatVisitor visitor = SQLUtils.createSchemaStatVisitor(dbType);
        statement.accept(visitor);

        parsingPart(statement);

        DB_TYPE_THREAD_LOCAL.remove();
        return SQLUtils.toSQLString(statement, dbType, new SQLUtils.FormatOption(true, false));
    }

    private static void parserInsert(SQLInsertStatement statement) {
        parsingTableSource(statement.getTableSource());

        Param.parsingSQLInsertStatement(statement);
    }

    private static void parserDelete(SQLDeleteStatement statement) {
        parsingTableSource(statement.getTableSource());
        parsingTableSource(statement.getFrom());

        parsingWhere(statement.getWhere());
    }

    private static void parserUpdate(SQLUpdateStatement statement) {
        Param.parsingSQLUpdateStatement(statement);

        parsingTableSource(statement.getTableSource());
        parsingTableSource(statement.getFrom());

        parsingWhere(statement.getWhere());

        SQLOrderBy order = statement.getOrderBy();
        Param.parsingSQLOrderBy(order);
    }

    /**
     * 处理查询语句
     *
     * @param statement 查询statement
     */
    private static void parserSelect(SQLSelectStatement statement) {
        SQLSelect sqlSelect = statement.getSelect();
        parserSQLSelect(sqlSelect);
    }

    private static void parserSQLSelect(SQLSelect sqlSelect) {
        parserQuery(sqlSelect.getQuery());
    }

    private static void parserQuery(SQLSelectQuery query) {
        if (query instanceof SQLSelectQueryBlock) {
            SQLSelectQueryBlock sqlSelectQueryBlock = ((SQLSelectQueryBlock) query);
            Param.parsingSQLSelectItem(sqlSelectQueryBlock);

            SQLTableSource from = sqlSelectQueryBlock.getFrom();
            parsingTableSource(from);

            parsingWhere(sqlSelectQueryBlock.getWhere());

            SQLSelectGroupByClause groupBy = sqlSelectQueryBlock.getGroupBy();
            if (groupBy != null) {
                Param.parsingSQLSelectGroupByClause(groupBy);
            }

            SQLOrderBy order = sqlSelectQueryBlock.getOrderBy();
            if (order != null) {
                Param.parsingSQLOrderBy(order);
            }
        } else if (query instanceof SQLUnionQuery) {
            parserQuery(((SQLUnionQuery) query).getLeft());
            parserQuery(((SQLUnionQuery) query).getRight());
        }
    }

    /**
     * 处理where条件
     *
     * @param where where的表达式
     * @param <T>   泛型
     */
    private static <T extends SQLExpr> void parsingWhere(T where) {
        if (where == null) {
            return;
        }
        parserSQLObject(where);

        SQLObject parent = where.getParent();

        SQLExpr newWhere;
        if (parent instanceof SQLSelectQueryBlock) {
            newWhere = ((SQLSelectQueryBlock) parent).getWhere();
            SQLExpr newParseWhere = parsingWhereConstant(newWhere);
            ((SQLSelectQueryBlock) parent).setWhere(newParseWhere);
        } else if (parent instanceof SQLUpdateStatement) {
            newWhere = ((SQLUpdateStatement) parent).getWhere();
            SQLExpr newParseWhere = parsingWhereConstant(newWhere);
            ((SQLUpdateStatement) parent).setWhere(newParseWhere);
        } else {
            newWhere = ((SQLDeleteStatement) parent).getWhere();
            SQLExpr newParseWhere = parsingWhereConstant(newWhere);
            ((SQLDeleteStatement) parent).setWhere(newParseWhere);
        }
    }

    public static SQLExpr parsingWhereConstant(SQLExpr sqlExpr) {
        String where = SQLUtils.toSQLString(sqlExpr, DB_TYPE_THREAD_LOCAL.get());
        where = where.replaceAll(CONSTANT_CONDITION_REGEX, "").trim();
        final int minSize = 3;
        if (where.trim().length() < minSize || CONSTANT_CONDITION.equals(where)) {
            return null;
        }
        sqlExpr = SQLUtils.toSQLExpr(where, DB_TYPE_THREAD_LOCAL.get());
        if (where.contains(CONSTANT_CONDITION)) {
            return parsingWhereConstant(sqlExpr);
        } else {
            return sqlExpr;
        }
    }

    /**
     * 处理查询的from部分
     *
     * @param from from部分
     */
    private static void parsingTableSource(SQLTableSource from) {
        if (from instanceof SQLSubqueryTableSource) {
            SQLSelect childSelect = ((SQLSubqueryTableSource) from).getSelect();
            parserSQLSelect(childSelect);
        } else if (from instanceof SQLJoinTableSource) {
            SQLTableSource left = ((SQLJoinTableSource) from).getLeft();
            parsingTableSource(left);

            SQLTableSource right = ((SQLJoinTableSource) from).getRight();
            parsingTableSource(right);

            SQLExpr condition = ((SQLJoinTableSource) from).getCondition();
            if (condition != null) {
                parserSQLObject(condition);
                SQLExpr newCondition = parsingWhereConstant(condition);
                ((SQLJoinTableSource) from).setCondition(newCondition);
            }
        } else if (from instanceof SQLUnionQueryTableSource) {
            parserQuery(((SQLUnionQueryTableSource) from).getUnion());
        }
    }

    /**
     * 替换类语句replace into
     *
     * @param replace 替换语句
     */
    private static void parsingSQLReplaceStatement(SQLReplaceStatement replace) {
        for (SQLInsertStatement.ValuesClause valuesClause : replace.getValuesList()) {
            List<SQLExpr> values = valuesClause.getValues();

            List<Integer> indexs = Lists.newArrayList();
            for (SQLExpr expr : values) {
                if (Param.unprocessed(expr)) {
                    indexs.add(values.indexOf(expr));
                }
            }
            for (Integer i : indexs) {
                values.add(i, new SQLIdentifierExpr(null));
            }
        }
    }

    /**
     * sql分段，比如把where条件按照表达式拆分成段
     *
     * @param sqlObject sql druid对象
     */
    private static List<SQLObject> getMuchPart(SQLObject sqlObject) {
        List<SQLObject> result = new LinkedList<>();

        if (sqlObject == null) {
            return result;
        }
        List<SQLObject> children = ((SQLExpr) sqlObject).getChildren();
        if (children != null && !children.isEmpty()) {
            for (SQLObject child : children) {
                if (child instanceof SQLExpr) {
                    List<SQLObject> grandson = ((SQLExpr) child).getChildren();
                    if (grandson == null || grandson.isEmpty()) {
                        result.add(sqlObject);
                        break;
                    } else {
                        result.addAll(getMuchPart(child));
                    }
                }
            }
        } else {
            return getMuchPart(sqlObject.getParent());
        }
        return result;
    }

    /**
     * 处理sqlObject直接转转换占位符
     *
     * @param sqlObject sql druid对象
     */
    public static void parserSQLObject(SQLExpr sqlObject) {
        if (sqlObject == null) {
            return;
        }
        List<SQLObject> sqlPartInfo = getMuchPart(sqlObject);
        for (SQLObject part : sqlPartInfo) {
            parsingPart(part);
        }
    }

    private static void parsingPart(SQLObject part) {
        if (part instanceof SQLInListExpr) {
            Param.parsingSQLInListExpr((SQLInListExpr) part);
        } else if (part instanceof SQLBinaryOpExpr) {
            Param.parsingSQLBinaryOpExpr((SQLBinaryOpExpr) part);
        } else if (part instanceof SQLMethodInvokeExpr) {
            Param.parsingMethodInvoke((SQLMethodInvokeExpr) part);
        } else if (part instanceof SQLBetweenExpr) {
            Param.parsingSQLBetweenExpr((SQLBetweenExpr) part);
        } else if (part instanceof SQLOrderBy) {
            Param.parsingSQLOrderBy((SQLOrderBy) part);
        } else if (part instanceof SQLSelectGroupByClause) {
            Param.parsingSQLSelectGroupByClause((SQLSelectGroupByClause) part);
        } else if (part instanceof SQLSelectQueryBlock) {
            Param.parsingSQLSelectItem((SQLSelectQueryBlock) part);
        } else if (part instanceof SQLUpdateStatement) {
            parserUpdate((SQLUpdateStatement) part);
        } else if (part instanceof SQLInsertStatement) {
            parserInsert((SQLInsertStatement) part);
        } else if (part instanceof SQLDeleteStatement) {
            parserDelete((SQLDeleteStatement) part);
        } else if (part instanceof SQLSelectStatement) {
            parserSelect((SQLSelectStatement) part);
        } else if (part instanceof SQLInSubQueryExpr) {
            parsingInSubQuery((SQLInSubQueryExpr) part);
        } else if (part instanceof SQLPropertyExpr) {
            parsingPart(part.getParent());
        } else if (part instanceof SQLSelect) {
            parserSQLSelect((SQLSelect) part);
        } else if (part instanceof SQLSelectQuery) {
            parserQuery((SQLSelectQuery) part);
        } else if (part instanceof SQLTableSource) {
            parsingTableSource((SQLTableSource) part);
        } else if (part instanceof SQLReplaceStatement) {
            parsingSQLReplaceStatement((SQLReplaceStatement) part);
        }
    }

    /**
     * 处理where info in （select）类型条件
     *
     * @param c in的druid表达式
     */
    private static void parsingInSubQuery(SQLInSubQueryExpr c) {
        SQLSelect sqlSelect = c.getSubQuery();
        SQLStatementParser sqlStatementParser = SQLParserUtils.createSQLStatementParser(parserSQL(sqlSelect.toString()), DB_TYPE_THREAD_LOCAL.get());
        sqlSelect.setQuery(((SQLSelectStatement) sqlStatementParser.parseStatement()).getSelect().getQueryBlock());
    }

    /**
     * 查询语句获取排序字段集合
     *
     * @return 排序集合
     */
    public static List<SQLSelectOrderByItem> getSort(String sql) {
        List<SQLSelectOrderByItem> sorts = new ArrayList<>();
        // 新建 MySQL Parser
        SQLStatementParser parser = SQLParserUtils.createSQLStatementParser(sql, DB_TYPE_THREAD_LOCAL.get());
        // 使用Parser解析生成AST，这里SQLStatement就是AST
        SQLStatement statement = parser.parseStatement();
        SQLSelectQueryBlock sqlSelectQueryBlock = ((SQLSelectStatement) statement).getSelect().getQueryBlock();

        if (sqlSelectQueryBlock == null) {
            return sorts;
        }

        SQLOrderBy orderBy = sqlSelectQueryBlock.getOrderBy();
        if (orderBy != null) {
            return orderBy.getItems();

        }
        return sorts;
    }

    /**
     * 提取操作的sql语句中包含的目标表表名
     *
     * @param sql sql语句
     * @return 表名字
     */
    public static String getTableName(String sql) {

        // 新建 Parser
        SQLStatementParser parser = SQLParserUtils.createSQLStatementParser(sql, DB_TYPE_THREAD_LOCAL.get());

        // 使用Parser解析生成AST，这里SQLStatement就是AST
        SQLStatement statement = parser.parseStatement();

        // 使用visitor来访问AST
        SchemaStatVisitor visitor = new SchemaStatVisitor(DB_TYPE_THREAD_LOCAL.get());
        statement.accept(visitor);


        String tableName = null;
        if (statement instanceof SQLUpdateStatement) {
            tableName = extractUpdateTableName((SQLUpdateStatement) statement);
        } else if (statement instanceof SQLDeleteStatement) {
            tableName = extractDeleteTableName((SQLDeleteStatement) statement);
        } else if (statement instanceof SQLInsertStatement) {
            tableName = extractInsertTableName((SQLInsertStatement) statement);
        }

        return tableName;
    }

    private static String extractUpdateTableName(SQLUpdateStatement statement) {
        String tableName = parseSQLTableSource(statement.getFrom());
        if (tableName == null) {
            tableName = parseSQLTableSource(statement.getTableSource());
        }
        return tableName;
    }

    private static String extractDeleteTableName(SQLDeleteStatement statement) {
        String tableName = parseSQLTableSource(statement.getFrom());
        if (tableName == null) {
            tableName = parseSQLTableSource(statement.getTableSource());
        }
        return tableName;
    }

    private static String extractInsertTableName(SQLInsertStatement statement) {
        String tableName = statement.getTableName() == null ? null : statement.getTableName().getSimpleName();
        if (tableName == null) {
            tableName = parseSQLTableSource(statement.getTableSource());
        }
        return tableName;
    }

    private static String getTableName(SQLUpdateStatement statement) {
        String tableName = parseSQLTableSource(statement.getFrom());
        if (tableName == null) {
            tableName = parseSQLTableSource(statement.getTableSource());
        }
        return tableName;
    }

    private static String parseSQLTableSource(SQLTableSource sqlTableSource) {
        if (sqlTableSource instanceof SQLJoinTableSource) {
            return parseSQLTableSource(((SQLJoinTableSource) sqlTableSource).getLeft());
        } else if (sqlTableSource instanceof SQLExprTableSource) {
            return ((SQLExprTableSource) sqlTableSource).getName().getSimpleName();
        } else {
            return null;
        }
    }

    public static void setQueryParamThreadLocal(String key, Object value) {
        Map<String, Object> map = QUERY_PARAM_THREAD_LOCAL.get();
        if (map == null) {
            map = Maps.newConcurrentMap();
            setQueryParamThreadLocal(map);
        }
        map.put(key, value);
    }

    public static void setQueryParamThreadLocal(Map<String, Object> params) {
        if (params == null) {
            return;
        }
        QUERY_PARAM_THREAD_LOCAL.set(params);
    }

    public static void removeQueryParam(String key) {
        Map<String, Object> map = QUERY_PARAM_THREAD_LOCAL.get();
        if (map != null) {
            map.remove(key);
        }
    }
}
