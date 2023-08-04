package com.aliyun.dts.deliver.protocol.record.util;

import java.util.HashMap;
import java.util.Map;

public interface JDKCharsetMapper {

    Map<String, String> JDK_ENCODINGS = new HashMap<String, String>() {
        {
            // mysql, reference as mysql-jdbc charsets.properties
            // https://github.com/mysql/mysql-connector-j/blob/1de2fe873fe26189564c030a343885011412976a/src/main/core-api/java/com/mysql/cj/CharsetMapping.java
            put("usa7", "US-ASCII");
            put("ascii", "US-ASCII");
            put("big5", "Big5");
            put("gbk", "GBK");
            put("sjis", "SJIS");
            put("gb2312", "EUC_CN");
            put("ujis", "EUC_JP");
            put("eucjpms", "EUC_JP_Solaris");
            put("euc_kr", "EUC_KR");
            put("euckr", "EUC_KR");
            put("latin1", "ISO8859_1");
            put("latin1_de", "ISO8859_1");
            put("german1", "ISO8859_1");
            put("danish", "ISO8859_1");
            put("latin2", "ISO8859_2");
            put("czech", "ISO8859_2");
            put("hungarian", "ISO8859_2");
            put("croat", "ISO8859_2");
            put("greek", "ISO8859_7");
            put("latin7", "ISO8859_7");
            put("hebrew", "ISO8859_8");
            put("latin5", "ISO8859_9");
            put("latvian", "ISO8859_13");
            put("latvian1", "ISO8859_13");
            put("estonia", "ISO8859_13");
            put("cp850", "Cp437");
            put("dos", "Cp437");
            put("Cp850", "Cp850");
            put("Cp852", "Cp852");
            put("cp866", "Cp866");
            put("koi8_ru", "KOI8_R");
            put("koi8r", "KOI8_R");
            put("tis620", "TIS620");
            put("cp1250", "Cp1250");
            put("win1250", "Cp1250");
            put("cp1251", "Cp1251");
            put("win1251", "Cp1251");
            put("cp1251cias", "Cp1251");
            put("cp1251csas", "Cp1251");
            put("cp1256", "Cp1256");
            put("win1251ukr", "Cp1251");
            put("cp1257", "Cp1257");
            put("macroman", "MacRoman");
            put("macce", "MacCentralEurope");
            put("utf8", "UTF-8");
            put("ucs2", "UnicodeBig");
            put("binary", "US-ASCII");
            put("cp932", "MS932");
            put("cp943", "Cp943");

            // mssql, reference as mssql-jdbc encoding
            put("CP437", "Cp437");
            put("CP850", "Cp850");
            put("CP874", "MS874");
            put("CP932", "MS932");
            put("CP936", "MS936");
            put("CP949", "MS949");
            put("CP950", "MS950");
            put("CP1250", "Cp1250");
            put("CP1251", "Cp1251");
            put("CP1252", "Cp1252");
            put("CP1253", "Cp1253");
            put("CP1254", "Cp1254");
            put("CP1255", "Cp1255");
            put("CP1256", "Cp1256");
            put("CP1257", "Cp1257");
            put("CP1258", "Cp1258");
            put("armscii8", "Cp1252");

            // oracle, normal charset utf-8

            // pg, normal charset utf-8

            // others, reference as history mapper
            put("geostd8", "WINDOWS-1252");
            put("hp8", "WINDOWS-1252");
            put("koi8u", "KOI8-R");
            put("utf16", "UTF-16");
            put("utf16le", "UTF-16LE");
            put("utf32", "UTF-32");
            put("utf8mb4", "UTF-8");
            put("utf8mb3", "UTF-8");
            put("swe7", "Cp1252");

            // common charset(from source.column.encoding parameter) to jdk charset, reference as https://en.wikipedia.org/wiki/Character_encoding
            // others, reference as history mapper
            put("ASCII", "US-ASCII");

            put("ISO 8859-1", "ISO8859_1");
            put("ISO 8859-2", "ISO8859_2");
            put("ISO 8859-3", "ISO8859_3");
            put("ISO 8859-4", "ISO8859_4");
            put("ISO 8859-5", "ISO8859_5");
            put("ISO 8859-6", "ISO8859_6");
            put("ISO 8859-7", "ISO8859_7");
            put("ISO 8859-8", "ISO8859_8");
            put("ISO 8859-9", "ISO8859_9");
            put("ISO 8859-10", "ISO8859_10");
            put("ISO 8859-11", "ISO8859_11");
            put("ISO 8859-12", "ISO8859_12");
            put("ISO 8859-13", "ISO8859_13");
            put("ISO 8859-14", "ISO8859_14");
            put("ISO 8859-15", "ISO8859_15");
            put("ISO 8859-16", "ISO8859_16");

            put("CP720", "Cp720");
            put("CP737", "Cp737");
            put("CP852", "Cp852");
            put("CP857", "Cp857");
            put("CP858", "Cp858");
            put("CP860", "Cp860");
            put("CP861", "Cp861");
            put("CP862", "Cp862");
            put("CP863", "Cp863");
            put("CP865", "Cp865");
            put("CP866", "Cp866");
            put("CP869", "Cp869");
            put("CP872", "Cp872");

            put("Windows-1250", "WINDOWS-1250");
            put("Windows-1251", "WINDOWS-1251");
            put("Windows-1252", "WINDOWS-1252");
            put("Windows-1253", "WINDOWS-1253");
            put("Windows-1254", "WINDOWS-1254");
            put("Windows-1255", "WINDOWS-1255");
            put("Windows-1256", "WINDOWS-1256");
            put("Windows-1257", "WINDOWS-1257");
            put("Windows-1258", "WINDOWS-1258");

            put("GB 2312", "GBK");
            put("GBK", "GBK");
            put("GB 18030", "GBK");

            put("UTF-8", "UTF-8");
            put("UTF-16", "UTF-16");
            put("UTF-32", "UTF-32");
        }
    };

    static String getJDKECharset(String dbCharset) {
        return JDK_ENCODINGS.getOrDefault(dbCharset, dbCharset);
    }

    static String getJDKECharset(String dbCharset, String defaultCharset) {
        return JDK_ENCODINGS.getOrDefault(dbCharset, defaultCharset);
    }
}
