package io.mycat.server.handler;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOpExpr;
import com.alibaba.druid.sql.ast.expr.SQLCharExpr;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.expr.SQLIntegerExpr;
import com.alibaba.druid.sql.ast.statement.SQLInsertStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlInsertStatement;
import com.alibaba.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.google.common.base.Joiner;
import io.mycat.config.model.SchemaConfig;
import io.mycat.config.model.TableConfig;
import io.mycat.util.CollectionUtil;
import io.mycat.util.StringUtil;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public class DmInsertSqlConverter implements DmSqlHandler {

    private final static String INSERT = "INSERT";

    private final static String DUPLICATE = "duplicate";


    private final static String T1 = "T1";
    private final static String T2 = "T2";

    // table 待插入数据 唯一索引构成的条件 插入的字段 值 更新的字段和值
    private final static String MERGE_TO_TEMPLATE = "MERGE INTO %s T1 USING ( %s ) T2 ON ( %s ) WHEN NOT MATCHED THEN INSERT ( %s ) VALUES ( %s ) WHEN MATCHED THEN UPDATE SET %s";
    private final static String MERGE_TO_IGNORE_TEMPLATE = "MERGE INTO %s T1 USING ( %s ) T2 ON ( %s ) WHEN NOT MATCHED THEN INSERT ( %s ) VALUES ( %s ) ";

//    private final static String


    public String convert(String originSql, SchemaConfig schemaConfig) {
        if (StringUtil.isEmpty(originSql)) {
            return originSql;
        }

        MySqlStatementParser parser = new MySqlStatementParser(originSql);
        SQLInsertStatement statement = (SQLInsertStatement) parser.parseStatement();

        if (!(statement instanceof MySqlInsertStatement)) {
            return originSql;
        }
        MySqlInsertStatement mySqlInsertStatement = (MySqlInsertStatement) statement;

        List<SQLExpr> duplicateKeyUpdates = mySqlInsertStatement.getDuplicateKeyUpdate();
        if (!CollectionUtil.isEmpty(duplicateKeyUpdates)) {
            return duplicateKeyUpdateConvert(originSql, mySqlInsertStatement, schemaConfig);
        }

        if (mySqlInsertStatement.isIgnore()) {
            return insertIgnoreConvert(mySqlInsertStatement, schemaConfig);
        }
        return originSql;
    }


    /**
     * @param mySqlInsertStatement mySqlInsertStatement
     * @param schemaConfig         config
     * @return
     */
    private String duplicateKeyUpdateConvert(String originSql, MySqlInsertStatement mySqlInsertStatement, SchemaConfig schemaConfig) {

        List<SQLExpr> duplicateKeyUpdates = mySqlInsertStatement.getDuplicateKeyUpdate();
        if (CollectionUtil.isEmpty(duplicateKeyUpdates)) {
            return originSql;
        }
        String tableName = mySqlInsertStatement.getTableName().getSimpleName();

        Map<String, TableConfig> tables = schemaConfig.getTables();

        // uniqueIndex
        List<String> uniqueIndex = new ArrayList<>();
        TableConfig tableConfig = tables.get(tableName.toUpperCase());
        if (Objects.nonNull(tableConfig)) {
            uniqueIndex = tableConfig.getUniqueIndex();
        }
        Set<String> uniqueIndexSet = new HashSet<>(uniqueIndex);

        // columns
        List<SQLExpr> columnsExpr = mySqlInsertStatement.getColumns();
        List<String> columns = columnsExpr.stream().map(this::getSQLExprValue).collect(Collectors.toList());

        // using part
        List<SQLInsertStatement.ValuesClause> valuesList = mySqlInsertStatement.getValuesList();
        String usingPart = buildUnionValues(valuesList, columns);

        // unique index part
        String onPart = buildOnPart(uniqueIndex);

        // not match insert columns part
        String notMatchInsertColumnsPart = Joiner.on(", ").join(columns);
        // not match insert values part
        String notMatchInsertValuesPart = Joiner.on(", ").join(columns.stream().map(item -> T2 + "." + item).collect(Collectors.toList()));

        // match update part
        String matchUpdatePart = Joiner.on(", ").join(duplicateKeyUpdates.stream().map(this::getSQLExprValue)
                // 过滤掉唯一索引的字段，MERGE INTO 不能更新关联条件中的列
                .filter(item -> !uniqueIndexSet.contains(item))
                .map(item -> T1 + "." + item + " = " + T2 + "." + item).collect(Collectors.toList()));

        return String.format(MERGE_TO_TEMPLATE, tableName, usingPart, onPart, notMatchInsertColumnsPart, notMatchInsertValuesPart, matchUpdatePart);
    }

    /**
     * insertIgnore
     *
     * @param mySqlInsertStatement
     * @param schemaConfig
     * @return
     */
    private String insertIgnoreConvert(MySqlInsertStatement mySqlInsertStatement, SchemaConfig schemaConfig) {

        String tableName = mySqlInsertStatement.getTableName().getSimpleName();

        Map<String, TableConfig> tables = schemaConfig.getTables();

        // uniqueIndex
        List<String> uniqueIndex = new ArrayList<>();
        TableConfig tableConfig = tables.get(tableName.toUpperCase());
        if (Objects.nonNull(tableConfig)) {
            uniqueIndex = tableConfig.getUniqueIndex();
        }

        // columns
        List<SQLExpr> columnsExpr = mySqlInsertStatement.getColumns();
        List<String> columns = columnsExpr.stream().map(this::getSQLExprValue).collect(Collectors.toList());

        // using part
        List<SQLInsertStatement.ValuesClause> valuesList = mySqlInsertStatement.getValuesList();
        String usingPart = buildUnionValues(valuesList, columns);

        // unique index part
        String onPart = buildOnPart(uniqueIndex);

        // not match insert columns part
        String notMatchInsertColumnsPart = Joiner.on(", ").join(columns);
        // not match insert values part
        String notMatchInsertValuesPart = Joiner.on(", ").join(columns.stream().map(item -> T2 + "." + item).collect(Collectors.toList()));

        return String.format(MERGE_TO_IGNORE_TEMPLATE, tableName, usingPart, onPart, notMatchInsertColumnsPart, notMatchInsertValuesPart);

    }


    /**
     * 组合USING部分
     *
     * @param valuesList 插入的值
     * @param columns    列
     * @return SELECT <VALUE> AS <COLUMN> UNION ALL SELECT <VALUE> AS <COLUMN> UNION ALL ...
     */
    private String buildUnionValues(List<SQLInsertStatement.ValuesClause> valuesList, List<String> columns) {
        List<String> selectList = new ArrayList<>();
        int columnsSize = columns.size();
        for (SQLInsertStatement.ValuesClause valuesClause : valuesList) {
            List<SQLExpr> values = valuesClause.getValues();
            List<String> asPart = new ArrayList<>();
            for (int i = 0; i < columnsSize; i++) {
                String value = values.get(i).toString();
                String column = columns.get(i);
                asPart.add(value + " AS " + column);
            }
            String usingOnePart = Joiner.on(", ").join(asPart);
            selectList.add("SELECT " + usingOnePart);
        }
        return Joiner.on(" UNION ALL ").join(selectList);
    }

    /**
     * @param uniqueIndex 唯一索引
     * @return T1.x = T2.x AND T1.y = T2.y
     */
    private String buildOnPart(List<String> uniqueIndex) {
        if (CollectionUtil.isEmpty(uniqueIndex)) {
            return "";
        }
        // 固定用到T1 T2 表别名
        List<String> collect = uniqueIndex.stream().map(item -> T1 + "." + item + " = " + T2 + "." + item)
                .collect(Collectors.toList());
        return Joiner.on(" AND ").join(collect);
    }


    /**
     * 获取值
     *
     * @param s SQLExpr
     * @return value
     */
    private String getSQLExprValue(SQLExpr s) {
        if (s instanceof SQLIntegerExpr) {
            return ((SQLIntegerExpr) s).getNumber().toString();
        } else if (s instanceof SQLCharExpr) {
            return ((SQLCharExpr) s).getText();
        } else if (s instanceof SQLIdentifierExpr) {
            return ((SQLIdentifierExpr) s).getName();
        } else if (s instanceof SQLBinaryOpExpr) {
            return getSQLExprValue(((SQLBinaryOpExpr) s).getLeft());
        }
        return "";
    }

}
