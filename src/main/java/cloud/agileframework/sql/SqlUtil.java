package cloud.agileframework.sql;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLObject;
import com.alibaba.druid.sql.ast.SQLOrderBy;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.expr.SQLBetweenExpr;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOpExpr;
import com.alibaba.druid.sql.ast.expr.SQLInListExpr;
import com.alibaba.druid.sql.ast.expr.SQLInSubQueryExpr;
import com.alibaba.druid.sql.ast.expr.SQLMethodInvokeExpr;
import com.alibaba.druid.sql.ast.expr.SQLPropertyExpr;
import com.alibaba.druid.sql.ast.statement.SQLDeleteStatement;
import com.alibaba.druid.sql.ast.statement.SQLExprTableSource;
import com.alibaba.druid.sql.ast.statement.SQLInsertStatement;
import com.alibaba.druid.sql.ast.statement.SQLJoinTableSource;
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
import com.alibaba.druid.sql.dialect.mysql.visitor.MySqlSchemaStatVisitor;
import com.alibaba.druid.sql.parser.SQLParserUtils;
import com.alibaba.druid.sql.parser.SQLStatementParser;
import com.alibaba.druid.util.JdbcUtils;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

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

    /**
     * 根据给定参数动态生成完成参数占位的查询条数sql语句
     *
     * @param sql        原sql模板
     * @param parameters map格式的sql语句中的参数集合，使用{paramName}方式占位
     * @return 生成的sql结果
     */
    public static String parserCountSQL(String sql, Object parameters) {
        sql = parserSQL(sql, parameters);

        return String.format("select count(1) from (%s) _select_table", sql);
    }

    public static String parserCountSQL(String sql) {
        return parserCountSQL(sql, null);
    }

    public static String parserSQL(String sql, Object parameters) {
        sql = Param.parsingSqlString(sql, Param.parsingParam(parameters));
        sql.replace("<","<  ");
        return parserSQL(sql);
    }

    /**
     * 根据给定参数动态生成完成参数占位的sql语句
     *
     * @param sql 原sql
     * @return 生成的sql结果
     */
    private static String parserSQL(String sql) {

        // 新建 MySQL Parser
        SQLStatementParser parser = SQLParserUtils.createSQLStatementParser(sql, JdbcUtils.MYSQL);

        // 使用Parser解析生成AST，这里SQLStatement就是AST
        SQLStatement statement = parser.parseStatement();

        // 使用visitor来访问AST
        MySqlSchemaStatVisitor visitor = new MySqlSchemaStatVisitor();
        statement.accept(visitor);

        parsingPart(statement);

        return statement.toString();
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
        String where = SQLUtils.toSQLString(sqlExpr);
        where = where.replaceAll(CONSTANT_CONDITION_REGEX, "").trim();
        final int minSize = 3;
        if (where.trim().length() < minSize || CONSTANT_CONDITION.equals(where)) {
            return null;
        }
        sqlExpr = SQLUtils.toSQLExpr(where);
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
        }
    }

    /**
     * 处理where info in （select）类型条件
     *
     * @param c in的druid表达式
     */
    private static void parsingInSubQuery(SQLInSubQueryExpr c) {
        SQLSelect sqlSelect = c.getSubQuery();
        SQLStatementParser sqlStatementParser = SQLParserUtils.createSQLStatementParser(parserSQL(sqlSelect.toString()), JdbcUtils.MYSQL);
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
        SQLStatementParser parser = SQLParserUtils.createSQLStatementParser(sql, JdbcUtils.MYSQL);
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

        // 新建 MySQL Parser
        SQLStatementParser parser = SQLParserUtils.createSQLStatementParser(sql, JdbcUtils.MYSQL);

        // 使用Parser解析生成AST，这里SQLStatement就是AST
        SQLStatement statement = parser.parseStatement();

        // 使用visitor来访问AST
        MySqlSchemaStatVisitor visitor = new MySqlSchemaStatVisitor();
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
}
