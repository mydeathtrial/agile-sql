package cloud.agileframework.sql;

import cloud.agileframework.common.constant.Constant;
import cloud.agileframework.common.util.json.JSONUtil;
import cloud.agileframework.common.util.object.ObjectUtil;
import cloud.agileframework.common.util.string.StringUtil;
import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLObject;
import com.alibaba.druid.sql.ast.SQLOrderBy;
import com.alibaba.druid.sql.ast.expr.SQLBetweenExpr;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOpExpr;
import com.alibaba.druid.sql.ast.expr.SQLInListExpr;
import com.alibaba.druid.sql.ast.expr.SQLMethodInvokeExpr;
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
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.Iterator;

import static com.alibaba.druid.sql.ast.expr.SQLBinaryOperator.Equality;

/**
 * @author 佟盟
 * 日期 2019/12/21 13:19
 * 描述 TODO
 * @version 1.0
 * @since 1.0
 */
public class Param {

    public static final String SQL_ILLEGAL = "\\b(sp_|xp_|execute|like|create|group|order|by|having|where|from|union|and|exec|insert|select|drop|grant|alter|delete|update|count|chr|mid|master|truncate|char|declare|or|ifnull|0[xX][\\da-fA-F]+)\\b|([*;+'%])";
    public static final int INITIAL_CAPACITY = 16;
    private static final ThreadLocal<Map<String, Object>> THREAD_LOCAL = new ThreadLocal<>();
    public static final String PARAM_START = "@_START_";
    public static final String PARAM_INDEX = "_INDEX";
    public static final String PARAM_END = "_END_";
    public static final String PARAM_SPLIT = "_SPLIT_";
    public static final String NOT_FOUND_PARAM = "@NOT_FOUND_PARAM_";
    public static final String REPLACE_NULL_CONDITION = " 1=1 ";
    private final String placeHolder;

    public String getPlaceHolder() {
        return placeHolder;
    }

    public Param(String key, Object value) {

        this.placeHolder = PARAM_START + key.replace(Constant.RegularAbout.SPOT,PARAM_SPLIT) + PARAM_END;
        THREAD_LOCAL.get().put(placeHolder, value);
    }

    /**
     * 判断是否非法
     *
     * @return 是否
     */
    public static void isIllegal(String sql) {
        Matcher matcher = Pattern.compile(SQL_ILLEGAL).matcher(sql.toLowerCase());
        if (matcher.find()) {
            throw new ParserException("SQL 注入风险");
        }
    }

    /**
     * 处理参数集合
     *
     * @param params 参数集合
     */
    public static Object parsingParam(Object params) {
        THREAD_LOCAL.remove();
        THREAD_LOCAL.set(new HashMap<>(INITIAL_CAPACITY));
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
            return !((Optional<?>) obj).isPresent();
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
            Object set = ((Collection<?>) obj).stream().filter(node -> !StringUtil.isEmpty(String.valueOf(node))).collect(Collectors.toSet());
            return ((Collection<?>) set).isEmpty();
        }
        if (obj instanceof Map) {
            return ((Map<?, ?>) obj).isEmpty();
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
        sql = StringUtil.parsingPlaceholder("${", "}", ":", sql, params, NOT_FOUND_PARAM);
        return parsingPlaceholder("#{", "}", ":", sql, params, NOT_FOUND_PARAM);
    }

    private static String parsingPlaceholder(String openToken, String closeToken, String equalToken, String text, Object args, String replaceNull) {
        if (args == null) {
            if (replaceNull != null) {
                args = new HashMap<String, Object>(0);
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
                    //判断是否有配置了默认值(:)
                    if (keyObj.length > 0) {
                        //配置了默认值,使用key获取当前环境变量中是否已经配置
                        o = JSONUtil.pathGet(keyObj[0].trim(), args);
                        if (o != null) {
                            replaceKey = keyObj[0].trim();
                        }
                    } else {
                        o = JSONUtil.pathGet(key.trim(), args);
                        if (o != null) {
                            replaceKey = key.trim();
                        }
                    }

                    if (o == null || isEmpty(o)) {
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

    public static void parsingPlaceHolder(String sql) {
        Set<Map.Entry<String, Object>> set = THREAD_LOCAL.get()
                .entrySet()
                .stream()
                .filter(param -> sql.contains(param.getKey()))
                .collect(Collectors.toSet());

        for (Map.Entry<String, Object> param : set) {
            SqlUtil.setQueryParamThreadLocal(param.getKey(), param.getValue());
        }
    }

    private static void parsingPlaceHolder(String sql, Consumer<Map.Entry<String, Object>> consumer) {
        Set<Map.Entry<String, Object>> set = THREAD_LOCAL.get()
                .entrySet()
                .stream()
                .filter(param -> sql.contains(param.getKey()))
                .collect(Collectors.toSet());

        for (Map.Entry<String, Object> param : set) {
            if (consumer != null) {
                consumer.accept(param);
            }
        }
    }

    public static void parsingSQLSelectItem(SQLSelectQueryBlock sqlSelectQueryBlock) {
        if (sqlSelectQueryBlock == null) {
            return;
        }
        List<SQLSelectItem> sqlSelectItems = sqlSelectQueryBlock.getSelectList();
        sqlSelectItems.removeIf(Param::unprocessed);
    }

    public static void parsingSQLUpdateStatement(SQLUpdateStatement sqlUpdateStatement) {
        if (sqlUpdateStatement == null) {
            return;
        }
        List<SQLUpdateSetItem> items = sqlUpdateStatement.getItems();
        items.removeIf(Param::unprocessed);
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

        Iterator<SQLExpr> it = columns.iterator();
        while (it.hasNext()){
            SQLExpr column = it.next();
            SQLExpr valueSQLExpr = values.get(columns.indexOf(column));
            if (unprocessed(column) || unprocessed(valueSQLExpr)) {
                it.remove();
                values.remove(valueSQLExpr);
            }
        }
    }

    public static void parsingSQLInListExpr(SQLInListExpr sqlExpr) {
        if (sqlExpr == null) {
            return;
        }
        List<SQLExpr> targetList = sqlExpr.getTargetList();
        targetList.removeIf(Param::unprocessed);

        if (targetList.isEmpty()) {
            SQLUtils.replaceInParent(sqlExpr, SQLUtils.toSQLExpr(REPLACE_NULL_CONDITION, SqlUtil.DB_TYPE_THREAD_LOCAL.get()));
            return;
        }

        parsingPlaceHolder(SQLUtils.toSQLString(sqlExpr, SqlUtil.DB_TYPE_THREAD_LOCAL.get()), value -> {
            String key = value.getKey();

            Object vs = value.getValue();
            if (Collection.class.isAssignableFrom(vs.getClass()) || vs.getClass().isArray()) {
                List<Object> list;
                if (vs.getClass().isArray()) {
                    ArrayList<Object> collection = new ArrayList<>();
                    int length = Array.getLength(vs);
                    for (int i = 0; i < length; i++) {
                        Object v = Array.get(vs, i);
                        collection.add(v);
                    }
                    list = collection;
                } else {
                    list = new ArrayList<>((Collection<Object>) vs);
                }

                for (int i = 0; i < list.size(); i++) {
                    String ck = key + i + PARAM_INDEX;
                    Object cv = list.get(i);
                    SqlUtil.setQueryParamThreadLocal(ck, cv);
                }
                SqlUtil.setQueryParamThreadLocal(key, new WhereIn(key, list));
            }
        });
    }

    public static void parsingSQLBinaryOpExpr(SQLBinaryOpExpr sqlExpr) {
        if (sqlExpr == null) {
            return;
        }
        if (unprocessed(sqlExpr)) {
            SQLUtils.replaceInParent(sqlExpr, SQLUtils.toSQLExpr(REPLACE_NULL_CONDITION, SqlUtil.DB_TYPE_THREAD_LOCAL.get()));
        }
    }

    public static void parsingSQLOrderBy(SQLOrderBy sqlExpr) {
        if (sqlExpr == null) {
            return;
        }
        List<SQLSelectOrderByItem> orders = sqlExpr.getItems();
        orders.removeIf(Param::unprocessed);
    }

    public static void parsingSQLSelectGroupByClause(SQLSelectGroupByClause sqlExpr) {
        if (sqlExpr == null) {
            return;
        }
        List<SQLExpr> items = sqlExpr.getItems();
        if (!ObjectUtil.isEmpty(items)) {
            items.removeIf(Param::unprocessed);
        }

        SQLExpr having = sqlExpr.getHaving();
        if (having != null) {
            SqlUtil.parserSQLObject(having);
            SQLExpr newHaving = SqlUtil.parsingWhereConstant(having);
            sqlExpr.setHaving(newHaving);
        }
    }


    /**
     * 语法段是否存在“参数不存在”占位符
     *
     * @param sql sql语法段
     * @return 是否
     */
    public static boolean unprocessed(SQLObject sql) {
        return SQLUtils.toSQLString(sql, SqlUtil.DB_TYPE_THREAD_LOCAL.get()).contains(NOT_FOUND_PARAM);
    }

    public static void parsingSQLBetweenExpr(SQLBetweenExpr part) {
        if (unprocessed(part)) {
            SQLUtils.replaceInParent(part, SQLUtils.toSQLExpr(REPLACE_NULL_CONDITION, SqlUtil.DB_TYPE_THREAD_LOCAL.get()));
        }
    }

    public static void parsingMethodInvoke(SQLMethodInvokeExpr methodInvokeExpr) {
        if (Param.unprocessed(methodInvokeExpr)) {
            SQLObject parent = methodInvokeExpr.getParent();
            if (parent instanceof SQLBinaryOpExpr) {
                ((SQLBinaryOpExpr) parent).setRight(SQLUtils.toSQLExpr("1", SqlUtil.DB_TYPE_THREAD_LOCAL.get()));
                ((SQLBinaryOpExpr) parent).setLeft(SQLUtils.toSQLExpr("1", SqlUtil.DB_TYPE_THREAD_LOCAL.get()));
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
        }
    }
}
