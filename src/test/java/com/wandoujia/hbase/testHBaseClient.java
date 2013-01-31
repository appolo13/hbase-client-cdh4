package com.wandoujia.hbase;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import junit.framework.TestCase;

import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;

public class testHBaseClient extends TestCase {
    public static final String tableName = "muce_16";

    public void testNewInstance() throws IOException {
        HBaseClient hbaseClient = new HBaseClient();
        hbaseClient.getAdmin();
    }

    public void testHBaseAdmin() throws IOException {
        HBaseClient hbaseClient = new HBaseClient();
        HBaseAdmin hbaseAdmin = hbaseClient.getAdmin();
        ClusterStatus clusterStatus = hbaseAdmin.getClusterStatus();
        System.out.println("hbase version: " + clusterStatus.getHBaseVersion());
        System.out.println("hbase servers: " + clusterStatus.getServers());
        System.out.println("hbase region count: "
                + clusterStatus.getRegionsCount());
        System.out.println("hbase dead servers: "
                + clusterStatus.getDeadServerNames());
        System.out.println("hbase region servers: "
                + clusterStatus.getServers());
        Map<String, byte[]> values = new HashMap<String, byte[]>();
        values.put("pn", "com.wandoujia.muce".getBytes());
        hbaseClient.insert("applist", "co".getBytes(), "key_001", values);
    }

    public void testGetScanner() throws IOException {
        HBaseClient hbaseClient = new HBaseClient();
        ResultScanner scanner = hbaseClient.getScannerByPrefix(tableName,
                "1h_568_75,80_201212110");
        try {
            for (Result rr = scanner.next(); rr != null; rr = scanner.next()) {
                System.out.println(new String(rr.getRow()));
            }
        } finally {
            if (scanner != null) {
                scanner.close();
            }
        }
    }

}
