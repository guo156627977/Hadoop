package com.gzq.hbase;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.junit.Test;

import java.io.IOException;

/**
 * ${DESCRIPTION}
 *
 * @author guozhiqiang
 * @created 2018-01-02 14:26.
 */
public class HbaseCRUDTest {

    @Test
    public void cerateTable() throws IOException {
        //Configuration configuration = HBaseConfiguration.create();

        Connection connection = ConnectionFactory.createConnection();
        Admin admin = connection.getAdmin();
        TableName tableName = TableName.valueOf("ns1:t2");
        HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);
        HColumnDescriptor hColumnDescriptor = new HColumnDescriptor("cf");
        hTableDescriptor.addFamily(hColumnDescriptor);
        admin.createTable(hTableDescriptor);
        for (TableName name : admin.listTableNames()) {
            System.out.println("name = " + name);

        }
        ;
    }

    @Test
    public void cerateNameSpace() throws IOException {
        //Configuration configuration = HBaseConfiguration.create();

        Connection connection = ConnectionFactory.createConnection();
        Admin admin = connection.getAdmin();
        NamespaceDescriptor ns1 = NamespaceDescriptor.create("ns3").build();
        admin.createNamespace(ns1);
        NamespaceDescriptor[] namespaceDescriptors = admin.listNamespaceDescriptors();
        for (NamespaceDescriptor namespaceDescriptor : namespaceDescriptors) {
            System.out.println("namespaceDescriptor = " + namespaceDescriptor);

        }

    }


}
