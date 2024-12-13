package io.mycat.server.interceptor.impl;

import com.google.common.collect.Lists;
import io.mycat.MycatServer;
import io.mycat.config.model.SchemaConfig;
import io.mycat.config.model.SystemConfig;
import io.mycat.server.handler.DmInsertSqlConverter;
import io.mycat.server.interceptor.SQLInterceptor;
import io.mycat.server.parser.ServerParse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class DefaultSqlInterceptor implements SQLInterceptor {
    private static final char ESCAPE_CHAR = '\\';

    private static final int TARGET_STRING_LENGTH = 2;
    private static final Logger log = LoggerFactory.getLogger(DefaultSqlInterceptor.class);

    private static final DmInsertSqlConverter dmInsertSqlConverter = new DmInsertSqlConverter();

    private static final List<String> DM_KEYWORDS = Lists.newArrayList("order");

    /**
     * mysql driver对'转义与\',解析前改为foundationdb parser支持的'' add by sky
     *
     * @param sql
     * @return
     * @update by jason@dayima.com replace regex with general string walking
     * avoid sql being destroyed in case of some mismatch
     * maybe some performance enchanced
     */
    public static String processEscape(String sql) {
        int firstIndex = -1;
        if ((sql == null) || ((firstIndex = sql.indexOf(ESCAPE_CHAR)) == -1)) {
            return sql;
        } else {
            int lastIndex = sql.lastIndexOf(ESCAPE_CHAR, sql.length() - 2) + TARGET_STRING_LENGTH;
            StringBuilder sb = new StringBuilder(sql);
            for (int i = firstIndex; i < lastIndex; i++) {
                if (sb.charAt(i) == '\\') {
                    if (i + 1 < lastIndex
                            && sb.charAt(i + 1) == '\'') {
                        //replace
                        sb.setCharAt(i, '\'');
                    }
                    //roll over
                    i++;
                }
            }
            return sb.toString();
        }
    }

    /**
     * escape mysql escape letter sql type ServerParse.UPDATE,ServerParse.INSERT
     * etc
     */
    @Override
    public String interceptSQL(String sql, int sqlType, SchemaConfig schemaConfig) {

        //fixme hardcode

        if ("fdbparser".equals(MycatServer.getInstance().getConfig().getSystem().getDefaultSqlParser())) {
            sql = processEscape(sql);
        }

        // 全局表一致性 sql 改写拦截
        SystemConfig system = MycatServer.getInstance().getConfig().getSystem();
        if (system != null && system.getUseGlobleTableCheck() == 1) // 全局表一致性检测是否开启
            sql = GlobalTableUtil.interceptSQL(sql, sqlType);

        // other interceptors put in here ....

        log.debug("[intercept] before intercept sql:{}", sql);
        // 粗暴方法，直接对sql里面出现反引号全部去除

        // replace keywords
        //fixme hardcode
        for (String keyword : DM_KEYWORDS) {
            String quoteKeyword = "`" + keyword + "`";
            if (sql.contains(quoteKeyword)) {
                sql = sql.replace(quoteKeyword, "\"" + keyword + "\"");
            }
            if (sql.contains("." + keyword)) {
                sql = sql.replace("." + keyword, ".\"" + keyword + "\"");
            }
        }

        if (sql.contains("`")) {
            sql = sql.replace("`", "");
        }

        if (sql.contains("\\")) {
            sql = sql.replace("\\", "");
        }

        if (sql.equalsIgnoreCase("SELECT @@transaction_isolation")) {
//            sql = "select case when ISOLATION = 1 then 'READ-COMMITTED ' end AS @@transaction_isolation from v$trx limit 1";
            sql = "select isolation from v$trx limit 1";
            return sql;
        }

        if (sql.equalsIgnoreCase("SET autocommit=1,transaction_isolation='REPEATABLE-READ'")) {
//            sql = "select case when ISOLATION = 1 then 'READ-COMMITTED ' end AS @@transaction_isolation from v$trx limit 1";
            sql = "select user()";
            return sql;
        }

        if (sql.equalsIgnoreCase("SELECT LAST_INSERT_ID()")) {
            sql = "SELECT SCOPE_IDENTITY()";
            return sql;
        }


        if (sqlType == ServerParse.INSERT) {
            sql = dmInsertSqlConverter.convert(sql, schemaConfig);
        }


        log.debug("[intercept] after intercept sql:{}", sql);
        return sql;
    }

}
