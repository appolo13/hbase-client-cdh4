package com.wandoujia.hbase;

public class Constants {
    public static final byte[] DEFAULT_BYTE_FAMILY_NAME = new byte[] {
        'c'
    };

    public static final String DEFAULT_STR_FAMILY_NAME = "c";

    public static final int DEFAULT_POOL_SIZE = 10;

    public static final int DEFAULT_BLOCK_SIZE = 65536;

    public static final int DEFAULT_SCANNER_CACHING = 10000;

    public static final int DEFAULT_WRITE_BUFFER = 10000;

    public static final String ROW_KEY = "row_key";
}
