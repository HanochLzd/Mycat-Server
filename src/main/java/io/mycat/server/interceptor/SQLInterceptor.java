package io.mycat.server.interceptor;

import io.mycat.config.model.SchemaConfig;

/**
 * used for interceptor sql before execute ,can modify sql befor execute
 *
 * @author wuzhih
 */
public interface SQLInterceptor {

    /**
     * return new sql to handler,ca't modify sql's type
     *
     * @param sql
     * @param sqlType
     * @return new sql
     */
    String interceptSQL(String sql, int sqlType, SchemaConfig schemaConfig);

}
