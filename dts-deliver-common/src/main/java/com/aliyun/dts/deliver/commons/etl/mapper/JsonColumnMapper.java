
package com.aliyun.dts.deliver.commons.etl.mapper;

import com.aliyun.dts.deliver.commons.etl.impl.WhiteList;
import org.apache.commons.lang3.StringUtils;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class JsonColumnMapper implements Mapper {

    private static final Pattern EXPRESSION_RECONGINIZE_PATTERN = Pattern.compile("(.+)//(.+)//(.+)\\:(.+)");
    private String srcDbType;
    private String destDbType;

    private String database;
    private String table;

    private String expressions = null;
    private Map<String, String> columnExpressionRules = null;

    public JsonColumnMapper(String srcDbType, String destDbType, String database, String table) {
        this.srcDbType = srcDbType;
        this.destDbType = destDbType;

        this.database = database;
        this.table = table;
    }

    public void initialize(String expressions) {
        this.expressions = expressions;
        parseExpressions();
    }

    /*
     * 解析正则表达式，对column进行映射
     * 表达式格式为 DB名格式//表名格式//列名格式:替换字符串
     * 其中DB名格式、表名格式和列名格式要符合Java正则表达式规范
     * 多个表达式通过';'进行分割
     */
    private void parseExpressions() {
        if (StringUtils.isEmpty(expressions)) {
            return;
        }

        columnExpressionRules = new HashMap<>();
        String[] expressionArray = this.expressions.split(";");
        for (String expression : expressionArray) {
            Matcher m = EXPRESSION_RECONGINIZE_PATTERN.matcher(expression);
            if (!m.matches()) {
                continue;
            }
            Pattern dbPattern = Pattern.compile(m.group(1));
            if (!dbPattern.matcher(this.database).matches()) {
                continue;
            }
            Pattern tablePattern = Pattern.compile(m.group(2));
            if (!tablePattern.matcher(this.table).matches()) {
                continue;
            }

            columnExpressionRules.put(m.group(3), m.group(4));
        }
    }

    @Override
    public String mapper(String original) {

        if (columnExpressionRules != null && columnExpressionRules.size() > 0) {
            for (String reg : columnExpressionRules.keySet()) {
                //只做全部列名匹配，不做部分匹配
                String regex = (reg.charAt(0) != '^' ? "^" : "")
                        + reg
                        + (reg.charAt(reg.length() - 1) != '$' ? "$" : "");
                String neworiginal = original.replaceAll(regex, columnExpressionRules.get(reg));
                //只匹配第一条规则，不做级联匹配
                if (!neworiginal.equals(original)) {
                    original = neworiginal;
                    break;
                }
            }
        }

        return WhiteList.dbMapper(this.database, this.table, original);
    }

    @Override
    public boolean contains(String original) {
        return false;
    }
}
