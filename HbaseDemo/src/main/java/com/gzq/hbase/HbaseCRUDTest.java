package com.gzq.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.IOException;
import java.text.DecimalFormat;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Iterator;

/**
 * ${DESCRIPTION}
 *
 * @author guozhiqiang
 * @created 2018-01-02 14:26.
 */
@RunWith(JUnit4.class)
public class HbaseCRUDTest {

    private static Connection connection = null;
    private static Configuration conf = null;


    @Test
    public void scanData() throws IOException {
        Connection connection = ConnectionFactory.createConnection();
        TableName tableName = TableName.valueOf("ns1:t1");
        Table table = connection.getTable(tableName);
        Scan scan = new Scan();
        byte[] famliy = Bytes.toBytes("cf1");
        byte[] nameColume = Bytes.toBytes("name");

        scan.addColumn(famliy, nameColume);
        scan.setStartRow(Bytes.toBytes("row003000"));
        scan.setStopRow(Bytes.toBytes("row003050"));
        Filter filter = scan.getFilter();
        filter.isFamilyEssential(Bytes.toBytes("guozhiqiang3004"));
        scan.setFilter(filter);
        ResultScanner scanner = table.getScanner(scan);
        Iterator<Result> iterator = scanner.iterator();
        while (iterator.hasNext()) {
            Result next = iterator.next();
            byte[] value = next.getValue(Bytes.toBytes("cf1"), Bytes.toBytes("name"));
            String name = Bytes.toString(value);
            System.out.println("name = " + name);


        }
        //for (Result result : scanner) {
        //    Cell[] cells = result.rawCells();
        //    for (Cell cell : cells) {
        //        System.out.print("行健: " + new String(CellUtil.cloneRow(cell)));
        //        System.out.print(" 列簇: " + new String(CellUtil.cloneFamily(cell)));
        //        System.out.print(" 列: " + new String(CellUtil.cloneQualifier(cell)));
        //        System.out.print(" 值: " + new String(CellUtil.cloneValue(cell)));
        //        System.out.println(" 时间戳: " + cell.getTimestamp());
        //    }
        //
        //}
        table.close();
        connection.close();
    }

    @Test
    public void insertBatch() throws Exception {
        Instant start = Instant.now();

        Connection connection = ConnectionFactory.createConnection();

        DecimalFormat format = new DecimalFormat();
        format.applyPattern("000000");
        TableName tableName = TableName.valueOf("ns1:t1");
        Table table = connection.getTable(tableName);
        ArrayList<Row> list = new ArrayList<>();

        byte[] columnFamily = Bytes.toBytes("cf1");

        byte[] id = Bytes.toBytes("id");
        byte[] name = Bytes.toBytes("name");
        byte[] age = Bytes.toBytes("age");
        for (int i = 0; i < 20000; i++) {
            byte[] rowkey = Bytes.toBytes("row" + format.format(i));
            byte[] idValue = Bytes.toBytes(i);
            byte[] nameValue = Bytes.toBytes("guozhiqiang" + i);
            byte[] ageValue = Bytes.toBytes(i % 100);
            Put put = new Put(rowkey);
            put.addColumn(columnFamily, id, idValue);
            put.addColumn(columnFamily, name, nameValue);
            put.addColumn(columnFamily, age, ageValue);
            list.add(put);
            //table.put(put);
        }
        Object[] objects = new Object[list.size()];
        table.batch(list, objects);
        Instant end = Instant.now();
        table.close();
        connection.close();

        //46s
        System.out.println(Duration.between(start, end).getSeconds()+"s");

    }

    @Test
    public void insertBatch2() throws Exception {
        Instant start = Instant.now();

        Connection connection = ConnectionFactory.createConnection();

        DecimalFormat format = new DecimalFormat();
        format.applyPattern("000000");
        TableName tableName = TableName.valueOf("ns1:t1");
        HTable table = (HTable)connection.getTable(tableName);
        ArrayList<Row> list = new ArrayList<>();

        byte[] columnFamily = Bytes.toBytes("cf1");

        byte[] id = Bytes.toBytes("id");
        byte[] name = Bytes.toBytes("name");
        byte[] age = Bytes.toBytes("age");
        for (int i = 20000; i < 40000; i++) {
            byte[] rowkey = Bytes.toBytes("row" + format.format(i));
            byte[] idValue = Bytes.toBytes(i);
            byte[] nameValue = Bytes.toBytes("guozhiqiang" + i);
            byte[] ageValue = Bytes.toBytes(i % 100);
            Put put = new Put(rowkey);
            put.setWriteToWAL(false);
            put.addColumn(columnFamily, id, idValue);
            put.addColumn(columnFamily, name, nameValue);
            put.addColumn(columnFamily, age, ageValue);
            table.put(put);
            if (i % 2000 == 0) {
                table.flushCommits();
            }
        }
        table.flushCommits();
        Instant end = Instant.now();
        table.close();
        connection.close();
        //90s
        System.out.println(Duration.between(start, end).getSeconds()+"s");

    }


    @Test
    public void formatNumber() {
        DecimalFormat format = new DecimalFormat();
        format.applyPattern("000000");
        System.out.println("format.format(1) = " + format.format(1));

    }


    /**
     * 从表中取出1行数据
     *
     * @throws IOException
     */
    @Test

    public void getData() throws IOException {
        Connection connection = ConnectionFactory.createConnection();

        TableName tableName = TableName.valueOf("ns1:t1");
        Table table = connection.getTable(tableName);

        byte[] row1 = Bytes.toBytes("row1");

        Get get = new Get(row1);
        Result result = table.get(get);
        Cell[] cells = result.rawCells();
        for (Cell cell : cells) {
            System.out.print("行健: " + new String(CellUtil.cloneRow(cell)));
            System.out.print(" 列簇: " + new String(CellUtil.cloneFamily(cell)));
            System.out.print(" 列: " + new String(CellUtil.cloneQualifier(cell)));
            System.out.print(" 值: " + new String(CellUtil.cloneValue(cell)));
            System.out.println(" 时间戳: " + cell.getTimestamp());
        }
        table.close();
        connection.close();
    }

    /**
     * 删除数据
     *
     * @throws IOException
     */
    @Test
    public void deleteData() throws IOException {
        Connection connection = ConnectionFactory.createConnection();

        TableName tableName = TableName.valueOf("ns1:t1");
        Table table = connection.getTable(tableName);

        byte[] row1 = Bytes.toBytes("row1");
        byte[] columnFamily = Bytes.toBytes("cf1");
        byte[] column1 = Bytes.toBytes("name");

        //什么都不填直接删除对应的一行
        Delete delete = new Delete(row1);
        //填写列族，删除对应的列
        //delete.addColumn(columnFamily, column1);

        table.delete(delete);
        getData();
        connection.close();

    }

    /**
     * 向表中插入数据
     *
     * @throws IOException
     */
    @Test
    public void putData() throws IOException {
        Connection connection = ConnectionFactory.createConnection();


        TableName tableName = TableName.valueOf("ns1:t1");
        Table table = connection.getTable(tableName);

        byte[] row1 = Bytes.toBytes("row1");
        byte[] columnFamily = Bytes.toBytes("cf1");
        byte[] column1 = Bytes.toBytes("name");
        byte[] value1 = Bytes.toBytes("guozhiqiang");
        byte[] column2 = Bytes.toBytes("age");
        byte[] value2 = Bytes.toBytes("26");

        Put put = new Put(row1);
        put.addColumn(columnFamily, column1, value1);
        put.addColumn(columnFamily, column2, value2);
        table.put(put);
        connection.close();
    }

    /**
     * 删除表，要先禁用后在删除表
     *
     * @throws IOException
     */
    @Test
    public void deleteTable() throws IOException {
        Connection connection = ConnectionFactory.createConnection();

        Admin admin = connection.getAdmin();
        TableName tableName = TableName.valueOf("ns1:t1");

        admin.disableTable(tableName);
        admin.deleteTable(tableName);
        for (TableName name : admin.listTableNames()) {
            System.out.println("name = " + name);
        }
        connection.close();
    }

    /**
     * 列出所有的表和列族
     *
     * @throws IOException
     */
    @Test
    public void listTableDetail() throws IOException {
        Connection connection = ConnectionFactory.createConnection();

        Admin admin = connection.getAdmin();
        for (TableName name : admin.listTableNames()) {
            System.out.println("name = " + name);
            HTableDescriptor tableDescriptor = admin.getTableDescriptor(name);
            HColumnDescriptor[] columnFamilies = tableDescriptor.getColumnFamilies();
            for (HColumnDescriptor columnFamily : columnFamilies) {
                System.out.println("columnFamily = " + columnFamily);
            }
        }

        connection.close();
    }

    /**
     * 创建表
     *
     * @throws IOException
     */
    @Test
    public void cerateTable() throws IOException {
        Connection connection = ConnectionFactory.createConnection();

        Admin admin = connection.getAdmin();
        TableName tableName = TableName.valueOf("ns1:t1");
        HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);
        HColumnDescriptor hColumnDescriptor1 = new HColumnDescriptor("cf1");
        HColumnDescriptor hColumnDescriptor2 = new HColumnDescriptor("cf2");
        hTableDescriptor.addFamily(hColumnDescriptor1);
        hTableDescriptor.addFamily(hColumnDescriptor2);
        admin.createTable(hTableDescriptor);
        for (TableName name : admin.listTableNames()) {
            System.out.println("name = " + name);
        }


        connection.close();
    }

    /**
     * 创建命名空间
     *
     * @throws IOException
     */
    @Test
    public void cerateNameSpace() throws IOException {
        Connection connection = ConnectionFactory.createConnection();

        Admin admin = connection.getAdmin();
        NamespaceDescriptor ns1 = NamespaceDescriptor.create("ns1").build();
        admin.createNamespace(ns1);
        NamespaceDescriptor[] namespaceDescriptors = admin.listNamespaceDescriptors();
        for (NamespaceDescriptor namespaceDescriptor : namespaceDescriptors) {
            System.out.println("namespaceDescriptor = " + namespaceDescriptor);
        }
        connection.close();
    }


}
