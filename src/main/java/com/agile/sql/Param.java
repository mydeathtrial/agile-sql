package com.agile.sql;

import com.agile.common.util.json.JSONUtil;
import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLObject;
import com.alibaba.druid.sql.ast.SQLOrderBy;
import com.alibaba.druid.sql.ast.expr.SQLBetweenExpr;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOpExpr;
import com.alibaba.druid.sql.ast.expr.SQLInListExpr;
import com.alibaba.druid.sql.ast.expr.SQLMethodInvokeExpr;
import com.alibaba.druid.sql.ast.expr.SQLQueryExpr;
import com.alibaba.druid.sql.ast.statement.SQLInsertStatement;
import com.alibaba.druid.sql.ast.statement.SQLSelectGroupByClause;
import com.alibaba.druid.sql.ast.statement.SQLSelectItem;
import com.alibaba.druid.sql.ast.statement.SQLSelectOrderByItem;
import com.alibaba.druid.sql.ast.statement.SQLSelectQueryBlock;
import com.alibaba.druid.sql.ast.statement.SQLUpdateSetItem;
import com.alibaba.druid.sql.ast.statement.SQLUpdateStatement;
import com.alibaba.druid.sql.parser.ParserException;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.alibaba.druid.sql.ast.expr.SQLBinaryOperator.Equality;

/**
 * @author 佟盟
 * 日期 2019/12/21 13:19
 * 描述 TODO
 * @version 1.0
 * @since 1.0
 */
public class Param {
    public static final String PREFIX = "'";
    public static final String REPLACEMENT = "''";
    public static final String FORMAT = "%s";
    public static final String FORMAT1 = "'%s'";
    public static final String DELIMITER = ",";
    public static final String REGEX = "[\\,]+(?=[^\\)]*(\\(|$))";
    public static final String SQL_ILLEGAL = "\\b(and|exec|insert|select|drop|grant|alter|delete|update|count|chr|mid|master|truncate|char|declare|or)\\s";
    public static final int INITIAL_CAPACITY = 16;
    private static ThreadLocal<Map<String, Object>> threadLocal = new ThreadLocal<>();
    private static final String PARAM_START = "@_START_";
    private static final String PARAM_END = "_END_";
    private static final String NOT_FOUND_PARAM = "@NOT_FOUND_PARAM_";
    private static final String REPLACE_NULL_CONDITION = " 1=1 ";
    private final String placeHolder;

    public String getPlaceHolder() {
        return placeHolder;
    }

    public Param(String key, Object value) {
        if (isIllegal(value.toString())) {
            throw new ParserException();
        }
        this.placeHolder = PARAM_START + key + PARAM_END;
        threadLocal.get().put(placeHolder, value);
    }

    /**
     * 判断是否非法
     *
     * @return 是否
     */
    private static boolean isIllegal(String sql) {
        Matcher matcher = Pattern.compile(SQL_ILLEGAL).matcher(sql.toLowerCase());
        return matcher.find();

    }

    /**
     * 处理参数集合
     *
     * @param params 参数集合
     */
    public static Object parsingParam(Object params) {
        threadLocal.remove();
        threadLocal.set(new HashMap<>(INITIAL_CAPACITY));
        if (params == null) {
            return null;
        }

        return JSONUtil.toMapOrList(params);
    }

    /**
     * 判空，包括空白字符串
     *
     * @param obj 判断对象
     * @return true是空的 false不是空的
     */
    public static boolean isEmpty(Object obj) {
        if (obj == null) {
            return true;
        }

        if (obj instanceof Optional) {
            return !((Optional) obj).isPresent();
        }
        if (obj instanceof CharSequence) {
            int strLen = ((CharSequence) obj).length();
            if (strLen == 0) {
                return true;
            }
            for (int i = 0; i < strLen; i++) {
                if (!Character.isWhitespace(((CharSequence) obj).charAt(i))) {
                    return false;
                }
            }
            return true;
        }
        if (obj.getClass().isArray()) {
            return Array.getLength(obj) == 0;
        }
        if (obj instanceof Collection) {
            return ((Collection) obj).isEmpty();
        }
        if (obj instanceof Map) {
            return ((Map) obj).isEmpty();
        }

        // else
        return false;
    }


    /**
     * 处理参数占位
     *
     * @param sql    未处理的sql语句
     * @param params 参数集合
     * @return 处理过的sql
     */
    public static String parsingSqlString(String sql, Object params) {
        return parsingPlaceholder("{", "}", ":", sql, params, NOT_FOUND_PARAM);
    }

    private static String parsingPlaceholder(String openToken, String closeToken, String equalToken, String text, Object args, String replaceNull) {
        if (args == null) {
            if (replaceNull != null) {
                args = new HashMap(0);
            } else {
                return text;
            }
        }

        if (text == null || text.isEmpty()) {
            return "";
        }
        char[] src = text.toCharArray();
        int offset = 0;
        // search open token
        int start = text.indexOf(openToken, offset);
        if (start == -1) {
            return text;
        }
        final StringBuilder builder = new StringBuilder();
        StringBuilder expression = null;
        while (start > -1) {
            if (start > 0 && src[start - 1] == '\\') {
                // this open token is escaped. remove the backslash and continue.
                builder.append(src, offset, start - offset - 1).append(openToken);
                offset = start + openToken.length();
            } else {
                // found open token. let's search close token.
                if (expression == null) {
                    expression = new StringBuilder();
                } else {
                    expression.setLength(0);
                }
                builder.append(src, offset, start - offset);
                offset = start + openToken.length();
                int end = text.indexOf(closeToken, offset);
                while (end > -1) {
                    if (end > offset && src[end - 1] == '\\') {
                        // this close token is escaped. remove the backslash and continue.
                        expression.append(src, offset, end - offset - 1).append(closeToken);
                        offset = end + closeToken.length();
                        end = text.indexOf(closeToken, offset);
                    } else {
                        expression.append(src, offset, end - offset);
                        offset = end + closeToken.length();
                        break;
                    }
                }
                if (end == -1) {
                    // close token was not found.
                    builder.append(src, start, src.length - start);
                    offset = src.length;
                } else {
                    String key = expression.toString();
                    String[] keyObj = key.split(equalToken);
                    Object o;
                    String value;
                    String replaceKey = null;
                    //判断是否有配置了默认值(:-)
                    if (keyObj.length > 0) {
                        //配置了默认值,使用key获取当前环境变量中是否已经配置
                        o = JSONUtil.pathGet(keyObj[0], args);
                        if (o != null) {
                            replaceKey = keyObj[0];
                        }
                    } else {
                        o = JSONUtil.pathGet(key, args);
                        if (o != null) {
                            replaceKey = key;
                        }
                    }

                    if (o == null || isEmpty(o) || replaceKey == null) {
                        if (key.contains(equalToken)) {
                            //获取不到使用默认值
                            value = keyObj[1].trim();
                        } else if (replaceNull != null) {
                            //获取不到环境变量时,返回原表达式
                            value = replaceNull;
                        } else {
                            value = openToken + key + closeToken;
                        }
                    } else {
                        Param param = new Param(replaceKey, o);
                        value = param.getPlaceHolder();
                    }
                    builder.append(value);
                    offset = end + closeToken.length();
                }
            }
            start = text.indexOf(openToken, offset);
        }
        if (offset < src.length) {
            builder.append(src, offset, src.length - offset);
        }
        return builder.toString();
    }

    private static String parsingPlaceHolder(String sql, Function<String, String> function) {
        String finalSql = sql;
        Set<Map.Entry<String, Object>> set = threadLocal.get()
                .entrySet()
                .stream()
                .filter(param -> finalSql.contains(param.getKey()))
                .collect(Collectors.toSet());

        for (Map.Entry<String, Object> param : set) {
            Object value = param.getValue();
            String replaceValue;
            if (Collection.class.isAssignableFrom(value.getClass()) || value.getClass().isArray()) {
                Stream<Object> stream;
                if (value.getClass().isArray()) {
                    ArrayList<Object> collection = new ArrayList<>();
                    int length = Array.getLength(value);
                    for (int i = 0; i < length; i++) {
                        Object v = Array.get(value, i);
                        collection.add(v);
                    }
                    stream = collection.stream();
                } else {
                    stream = ((Collection<Object>) value).stream();
                }
                replaceValue = stream.map(n -> function.apply(n.toString())).collect(Collectors.joining(DELIMITER));
            } else {
                if (isIllegal(value.toString())) {
                    throw new ParserException();
                }
                replaceValue = function.apply(value.toString());

            }
            sql = sql.replace(param.getKey(), replaceValue);
        }

        return sql;
    }

    public static void parsingSQLSelectItem(SQLSelectQueryBlock sqlSelectQueryBlock) {
        if (sqlSelectQueryBlock == null) {
            return;
        }
        List<SQLSelectItem> sqlSelectItems = sqlSelectQueryBlock.getSelectList();
        sqlSelectItems.removeIf(Param::unprocessed);

        String sql = sqlSelectItems.stream().map(SQLUtils::toSQLString).collect(Collectors.joining(DELIMITER));
        sql = parsingPlaceHolder(sql, value -> value);

        SQLExpr sqlExpr = SQLUtils.toSQLExpr("select " + sql + " from dual");
        if (sqlExpr instanceof SQLQueryExpr) {
            List<SQLSelectItem> list = ((SQLQueryExpr) sqlExpr).getSubQuery().getQueryBlock().getSelectList();
            sqlSelectItems.clear();
            sqlSelectItems.addAll(list);
        }
    }

    public static void parsingSQLUpdateStatement(SQLUpdateStatement sqlUpdateStatement) {
        if (sqlUpdateStatement == null) {
            return;
        }
        List<SQLUpdateSetItem> items = sqlUpdateStatement.getItems();
        items.removeIf(Param::unprocessed);

        if (items.size() == 0) {
            return;
        }
        items.forEach(node -> {
            SQLExpr column = node.getColumn();
            String newColumn = parsingPlaceHolder(SQLUtils.toSQLString(column), value -> value);
            node.setColumn(SQLUtils.toSQLExpr(newColumn));
        });
        items.forEach(node -> {
            String value = SQLUtils.toSQLString(node.getValue());
            if (!value.startsWith(PREFIX)) {
                value = PREFIX + value;
            }
            if (!value.endsWith(PREFIX)) {
                value = value + PREFIX;
            }
            String newValue = parsingPlaceHolder(value, v -> String.format(FORMAT, v).replace(PREFIX, REPLACEMENT));
            node.setValue(SQLUtils.toSQLExpr(newValue));
        });
    }

    public static void parsingSQLInsertStatement(SQLInsertStatement statement) {
        if (statement == null) {
            return;
        }
        List<SQLExpr> columns = statement.getColumns();
        List<SQLExpr> values = statement.getValues().getValues();

        if (columns.size() != values.size()) {
            throw new ParserException("插入的字段数量与值数量不一致");
        }

        for (int i = 0; i < columns.size(); i++) {
            SQLExpr column = columns.get(i);
            SQLExpr value = values.get(i);
            if (unprocessed(column) || unprocessed(value)) {
                columns.remove(column);
                values.remove(value);
                continue;
            }
            String newColumn = parsingPlaceHolder(SQLUtils.toSQLString(column), v -> v);
            columns.remove(column);
            columns.add(i, SQLUtils.toSQLExpr(newColumn));

            String newValue = parsingPlaceHolder(SQLUtils.toSQLString(value), v -> v);
            values.remove(value);
            values.add(i, SQLUtils.toSQLExpr(newValue));
        }
    }

    public static void parsingSQLInListExpr(SQLInListExpr sqlExpr) {
        if (sqlExpr == null) {
            return;
        }
        List<SQLExpr> targetList = sqlExpr.getTargetList();
        targetList.removeIf(Param::unprocessed);

        if (targetList.size() == 0) {
            SQLUtils.replaceInParent(sqlExpr, SQLUtils.toSQLExpr(REPLACE_NULL_CONDITION));
            return;
        }
        String sql = parsingPlaceHolder(SQLUtils.toSQLString(sqlExpr), value -> String.format(FORMAT1, value));
        SQLExpr newSQLInListExpr = SQLUtils.toSQLExpr(sql);

        if (newSQLInListExpr instanceof SQLInListExpr) {
            sqlExpr.setTargetList(((SQLInListExpr) newSQLInListExpr).getTargetList());
            sqlExpr.setNot(((SQLInListExpr) newSQLInListExpr).isNot());
            sqlExpr.setExpr(sqlExpr.getExpr());
        }
    }

    public static void parsingSQLBinaryOpExpr(SQLBinaryOpExpr sqlExpr) {
        if (sqlExpr == null) {
            return;
        }
        if (unprocessed(sqlExpr)) {
            SQLUtils.replaceInParent(sqlExpr, SQLUtils.toSQLExpr(REPLACE_NULL_CONDITION));
        } else {
            String right = SQLUtils.toSQLString(sqlExpr.getRight());
//            if (!right.startsWith(PREFIX)) {
//                right = PREFIX + right;
//            }
//            if (!right.endsWith(PREFIX)) {
//                right = right + PREFIX;
//            }
            String rightSql = parsingPlaceHolder(right, value -> String.format(FORMAT, value).replace(PREFIX, REPLACEMENT));
            sqlExpr.setRight(SQLUtils.toSQLExpr(rightSql));
            String sql = parsingPlaceHolder(SQLUtils.toSQLString(sqlExpr), value -> String.format(FORMAT, value));

            SQLUtils.replaceInParent(sqlExpr, SQLUtils.toSQLExpr(sql));
        }
    }

    public static void parsingSQLOrderBy(SQLOrderBy sqlExpr) {
        if (sqlExpr == null) {
            return;
        }
        List<SQLSelectOrderByItem> orders = sqlExpr.getItems();
        orders.removeIf(Param::unprocessed);

        if (orders.size() == 0) {
            return;
        }
        String sql = orders.stream().map(SQLUtils::toSQLString).collect(Collectors.joining(DELIMITER));
        sql = parsingPlaceHolder(sql, value -> value);

        SQLExpr querySQL = SQLUtils.toSQLExpr("select * from dual order by " + sql);
        if (querySQL instanceof SQLQueryExpr) {
            List<SQLSelectOrderByItem> list = ((SQLQueryExpr) querySQL).getSubQuery().getQueryBlock().getOrderBy().getItems();
            orders.clear();
            orders.addAll(list);
        }
    }

    public static void parsingSQLSelectGroupByClause(SQLSelectGroupByClause sqlExpr) {
        if (sqlExpr == null) {
            return;
        }
        List<SQLExpr> items = sqlExpr.getItems();
        items.removeIf(Param::unprocessed);


        String sql = items.stream().map(SQLUtils::toSQLString).collect(Collectors.joining(DELIMITER));
        sql = parsingPlaceHolder(sql, value -> value);

        List<SQLExpr> s = Stream.of(sql.split(REGEX))
                .map(SQLUtils::toSQLExpr)
                .collect(Collectors.toList());

        items.clear();
        items.addAll(s);
    }


    /**
     * 语法段是否存在“参数不存在”占位符
     *
     * @param sql sql语法段
     * @return 是否
     */
    public static boolean unprocessed(SQLObject sql) {
        return SQLUtils.toSQLString(sql).contains(NOT_FOUND_PARAM);
    }

    public static void parsingSQLBetweenExpr(SQLBetweenExpr part) {
        if (unprocessed(part)) {
            SQLUtils.replaceInParent(part, SQLUtils.toSQLExpr(REPLACE_NULL_CONDITION));
            return;
        }

        String sql = parsingPlaceHolder(SQLUtils.toSQLString(part), value -> value);

        SQLExpr newSql = SQLUtils.toSQLExpr(sql);
        if (newSql instanceof SQLBetweenExpr) {
            part.setNot(((SQLBetweenExpr) newSql).isNot());
            part.setBeginExpr(((SQLBetweenExpr) newSql).getBeginExpr());
            part.setEndExpr(((SQLBetweenExpr) newSql).getEndExpr());
            part.setTestExpr(((SQLBetweenExpr) newSql).getTestExpr());
        }
    }

    public static void parsingMethodInvoke(SQLMethodInvokeExpr methodInvokeExpr) {
        if (Param.unprocessed(methodInvokeExpr)) {
            SQLObject parent = methodInvokeExpr.getParent();
            if (parent instanceof SQLBinaryOpExpr) {
                ((SQLBinaryOpExpr) parent).setRight(SQLUtils.toSQLExpr("1"));
                ((SQLBinaryOpExpr) parent).setLeft(SQLUtils.toSQLExpr("1"));
                ((SQLBinaryOpExpr) parent).setOperator(Equality);
            } else if (parent instanceof SQLInListExpr) {
                ((SQLInListExpr) parent).getTargetList().remove(methodInvokeExpr);
            } else if (parent instanceof SQLUpdateSetItem) {
                SQLObject updateStatement = parent.getParent();
                if (updateStatement instanceof SQLUpdateStatement) {
                    ((SQLUpdateStatement) updateStatement).getItems().remove(parent);
                }
            } else if (parent instanceof SQLSelectItem) {
                SQLObject selectQuery = parent.getParent();
                if (selectQuery instanceof SQLSelectQueryBlock) {
                    ((SQLSelectQueryBlock) selectQuery).getSelectList().remove(parent);
                }
            }
        } else {
            String sql = parsingPlaceHolder(SQLUtils.toSQLString(methodInvokeExpr), value -> value);
            SQLExpr newSql = SQLUtils.toSQLExpr(sql);
            SQLUtils.replaceInParent(methodInvokeExpr, newSql);
        }
    }
}
