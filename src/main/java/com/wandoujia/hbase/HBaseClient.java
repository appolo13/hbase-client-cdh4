package com.wandoujia.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableFactory;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.io.hfile.Compression.Algorithm;
import org.apache.hadoop.hbase.regionserver.StoreFile.BloomType;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * hbase client is thread-safety
 * 
 * @author fengzanfeng
 */
public class HBaseClient {

    private HTablePool tablePool;

    public HTablePool getTablePool() {
        return tablePool;
    }

    private HBaseAdmin admin;

    /**
     * Inital hbase client with default hbase table pool size.
     */
    public HBaseClient() {
        this(Constants.DEFAULT_POOL_SIZE);
    }

    /**
     * Inital hbase client, set hbase table pool size
     * 
     * @param poolSize
     */
    public HBaseClient(int poolSize) {
        tablePool = new HTablePool(HBaseConfiguration.create(), poolSize);
    }

    /**
     * Initial hbase client, set hbase table pool size and write buffer size.
     * please call close() for safed exit, otherwise will cause data lose.
     * 
     * @param poolSize
     * @param writeBufferSize
     */
    public HBaseClient(int poolSize, final long writeBufferSize) {
        tablePool = new HTablePool(HBaseConfiguration.create(), poolSize,
                new HTableFactory() {
                    @Override
                    public HTableInterface createHTableInterface(
                            Configuration config, byte[] tableName) {
                        try {
                            HTable hTable = new HTable(config, tableName);
                            hTable.setAutoFlush(false);
                            hTable.setWriteBufferSize(writeBufferSize);
                            return hTable;
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    }
                });
    }

    /**
     * @return
     * @throws IOException
     */
    public synchronized HBaseAdmin getAdmin() throws IOException {
        if (this.admin == null)
            this.admin = new HBaseAdmin(HBaseConfiguration.create());
        return this.admin;
    }

    /**
     * create hbase table
     * 
     * @param tableName
     * @param familyName
     * @param bloomType
     * @param compressionType
     * @param inMemory
     * @param blockCacheEnabled
     * @param blockSize
     * @param maxVersions
     * @param splits
     * @return
     * @throws IOException
     */
    public synchronized HTableDescriptor create(String tableName,
            String familyName, BloomType bloomType, Algorithm compressionType,
            Boolean inMemory, Boolean blockCacheEnabled, int blockSize,
            int maxVersions, byte[][] splits) throws IOException {
        if (getAdmin().tableExists(tableName)) {
            return null;
        }
        HTableDescriptor table = HBaseUtil.getTableDescriptor(tableName,
                familyName, bloomType, compressionType, inMemory,
                blockCacheEnabled, blockSize, maxVersions);
        getAdmin().createTable(table, splits);
        return table;
    }

    /**
     * create hbase table with default descriptor
     * 
     * @param tableName
     * @return
     * @throws IOException
     */
    public synchronized HTableDescriptor create(String tableName)
            throws IOException {
        if (getAdmin().tableExists(tableName)) {
            return null;
        }
        HTableDescriptor table = HBaseUtil.getTableDescriptor(tableName,
                Constants.DEFAULT_STR_FAMILY_NAME, BloomType.ROW,
                Algorithm.NONE, false, true, Constants.DEFAULT_BLOCK_SIZE, 1);
        getAdmin().createTable(table, null);
        return table;
    }

    /**
     * @param tableName
     * @throws IOException
     */
    public synchronized void flush(String tableName) throws IOException {
        HTableInterface table = tablePool.getTable(tableName);
        try {
            table.flushCommits();
        } finally {
            if (table != null) {
                table.close();
            }
        }
    }

    /**
     * close hbase table pool
     * 
     * @throws IOException
     */
    public synchronized void close() throws IOException {
        tablePool.close();
    }

    /**
     * close hbase table
     * 
     * @param tableName
     * @throws IOException
     */
    public synchronized void close(String tableName) throws IOException {
        tablePool.closeTablePool(tableName.getBytes());
    }

    /**
     * get scanner by rowkey prefix
     * 
     * @param tablename
     * @param rowPrifix
     * @return
     * @throws IOException
     */
    public ResultScanner getScannerByPrefix(String tablename, String rowPrifix)
            throws IOException {
        ResultScanner rs = null;
        HTableInterface table = tablePool.getTable(tablename);
        try {
            Scan s = new Scan();
            s.setFilter(new PrefixFilter(rowPrifix.getBytes()));
            rs = table.getScanner(s);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (table != null) {
                table.close();
            }
        }
        return rs;
    }

    /**
     * get row with a single column
     * 
     * @param tableName
     * @param familyName
     * @param rowKey
     * @param column
     * @param cacheBlocks
     * @return
     * @throws IOException
     */
    public byte[] getRow(String tableName, byte[] familyName, String rowKey,
            byte[] column, boolean cacheBlocks) throws IOException {
        HTableInterface table = tablePool.getTable(tableName);
        try {
            Get get = new Get(rowKey.getBytes());
            get.setCacheBlocks(cacheBlocks);
            get.addColumn(familyName, column);
            Result row = table.get(get);
            if (row.raw().length >= 1) {
                return row.raw()[0].getValue();
            }
            return null;
        } finally {
            if (table != null) {
                table.close();
            }
        }
    }

    /**
     * @param tableName
     * @param familyName
     * @param rowKeys
     * @param column
     * @param cacheBlocks
     * @return key: row_key value: column value
     * @throws IOException
     */
    public Map<String, byte[]> getRows(String tableName, byte[] familyName,
            List<String> rowKeys, byte[] column, boolean cacheBlocks)
            throws IOException {
        HTableInterface table = tablePool.getTable(tableName);
        try {
            List<Get> gets = new ArrayList<Get>();
            for (String rowKey: rowKeys) {
                Get get = new Get(rowKey.getBytes());
                get.setCacheBlocks(cacheBlocks);
                get.addColumn(familyName, column);
                gets.add(get);
            }
            Result[] rows = table.get(gets);
            if (rows == null || rows.length < 1) {
                return null;
            }
            Map<String, byte[]> results = new HashMap<String, byte[]>();
            for (Result row: rows) {
                if (row.raw().length >= 1) {
                    results.put(String.valueOf(row.getRow()),
                            row.raw()[0].getValue());
                }
            }
            return results;
        } finally {
            if (table != null) {
                table.close();
            }
        }
    }

    /**
     * get a single with multi-columns
     * 
     * @param tableName
     * @param familyName
     * @param rowKey
     * @param columns
     * @param cacheBlocks
     * @return
     * @throws IOException
     */
    public Map<String, byte[]> getRow(String tableName, byte[] familyName,
            String rowKey, Set<byte[]> columns, boolean cacheBlocks)
            throws IOException {
        HTableInterface table = tablePool.getTable(tableName);
        try {
            Get get = new Get(rowKey.getBytes());
            get.setCacheBlocks(cacheBlocks);
            if (columns != null && columns.size() > 0) {
                for (byte[] column: columns) {
                    get.addColumn(familyName, column);
                }
            }
            Result row = table.get(get);
            if (row.raw().length >= 1) {
                Map<String, byte[]> result = new HashMap<String, byte[]>();
                for (KeyValue kv: row.raw()) {
                    result.put(new String(kv.getQualifier()), kv.getValue());
                }
                return result;
            }
            return null;
        } finally {
            if (table != null) {
                table.close();
            }
        }
    }

    /**
     * @param tableName
     * @param familyName
     * @param startRow
     * @param stopRow
     * @param caching
     * @param cacheBlocks
     * @return
     * @throws IOException
     */
    public ResultScanner getScanner(String tableName, byte[] familyName,
            Set<byte[]> columns, String startRow, String stopRow, int caching,
            boolean cacheBlocks) throws IOException {
        HTableInterface table = tablePool.getTable(tableName);
        try {
            Scan scan = new Scan(Bytes.toBytes(startRow),
                    Bytes.toBytes(stopRow));
            scan.setCacheBlocks(cacheBlocks);
            scan.setCaching(caching);
            if (columns != null && columns.size() > 0) {
                for (byte[] column: columns) {
                    scan.addColumn(familyName, column);
                }
            }
            return table.getScanner(scan);
        } finally {
            if (table != null) {
                table.close();
            }
        }
    }

    /**
     * @param tableName
     * @param familyName
     * @param columns
     * @param startRow
     * @param stopRow
     * @param caching
     * @param cacheBlocks
     * @return
     * @throws IOException
     */
    public ResultScanner getScanner(String tableName, byte[] familyName,
            String[] columns, String startRow, String stopRow, int caching,
            boolean cacheBlocks) throws IOException {
        HTableInterface table = tablePool.getTable(tableName);
        try {
            Scan scan = new Scan(Bytes.toBytes(startRow),
                    Bytes.toBytes(stopRow));
            scan.setCacheBlocks(cacheBlocks);
            scan.setCaching(caching);
            if (columns != null && columns.length > 0) {
                for (String column: columns) {
                    scan.addColumn(familyName, column.getBytes());
                }
            }
            return table.getScanner(scan);
        } finally {
            if (table != null) {
                table.close();
            }
        }
    }

    /**
     * @param tableName
     * @param familyName
     * @param startRow
     * @param stopRow
     * @param filter
     *            See @HBaseUtil
     * @param caching
     * @param cacheBlocks
     * @return
     * @throws IOException
     */
    public ResultScanner getScanner(String tableName, byte[] familyName,
            Set<byte[]> columns, String startRow, String stopRow,
            Filter filter, int caching, boolean cacheBlocks) throws IOException {
        HTableInterface table = tablePool.getTable(tableName);
        try {
            Scan scan = new Scan(Bytes.toBytes(startRow),
                    Bytes.toBytes(stopRow));
            scan.setCacheBlocks(cacheBlocks);
            scan.setCaching(caching);
            if (filter != null) {
                scan.setFilter(filter);
            }
            if (columns != null && columns.size() > 0) {
                for (byte[] column: columns) {
                    scan.addColumn(familyName, column);
                }
            }
            return table.getScanner(scan);
        } finally {
            if (table != null) {
                table.close();
            }
        }
    }

    /**
     * @param tableName
     * @param familyName
     * @param rowKey
     * @param values
     * @throws IOException
     */
    public void update(String tableName, byte[] familyName, String rowKey,
            Map<String, byte[]> values) throws IOException {
        HTableInterface table = tablePool.getTable(tableName);
        try {
            Put put = new Put(rowKey.getBytes());
            for (Map.Entry<String, byte[]> entry: values.entrySet()) {
                put.add(familyName, entry.getKey().getBytes(), entry.getValue());
            }
            table.put(put);
        } finally {
            if (table != null) {
                table.close();
            }
        }
    }

    /**
     * @param tableName
     * @param familyName
     * @param rowKey
     * @param values
     * @throws IOException
     */
    public void insert(String tableName, byte[] familyName, String rowKey,
            Map<String, byte[]> values) throws IOException {
        update(tableName, familyName, rowKey, values);
    }

    /**
     * @param tableName
     * @param rowKey
     * @throws IOException
     */
    public void delete(String tableName, String rowKey) throws IOException {
        HTableInterface table = tablePool.getTable(tableName);
        try {
            Delete delete = new Delete(rowKey.getBytes());
            table.delete(delete);
        } finally {
            if (table != null) {
                table.close();
            }
        }
    }
}
