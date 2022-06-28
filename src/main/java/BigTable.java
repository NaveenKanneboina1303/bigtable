
import com.google.cloud.bigtable.hbase.BigtableConfiguration;

import java.util.List;
import java.util.ArrayList;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class BigTable {

    private static final byte[] TABLE_NAME = Bytes.toBytes(requiredProperty("bigtable.tableName"));
    private static final byte[] COLUMN_FAMILY_NAME = Bytes.toBytes("cf1");
    private static final byte[] COLUMN_FAMILY_NAME2 = Bytes.toBytes("cf2");
    private static final byte[] COLUMN_NAME = Bytes.toBytes("column1");
    private static final byte[] COLUMN_NAME2 = Bytes.toBytes("column2");

    private static final String[] Records = {
            "Hello World!", "Hello Cloud Bigtable!", "Hello HBase!"
    };

    private static void execute(String projectId, String instanceId) {

        try (Connection connection = BigtableConfiguration.connect(projectId, instanceId)) {
            Admin admin = connection.getAdmin();

            try {

                if (admin.tableExists(TableName.valueOf(TABLE_NAME))) {
                    print("Table " + TableName.valueOf(TABLE_NAME) + " already exists");
                } else {
                    HTableDescriptor descriptor = new HTableDescriptor(TableName.valueOf(TABLE_NAME));
                    descriptor.addFamily(new HColumnDescriptor(COLUMN_FAMILY_NAME));
                    descriptor.addFamily(new HColumnDescriptor(COLUMN_FAMILY_NAME2));
                    print("Create table " + descriptor.getNameAsString());
                    admin.createTable(descriptor);
                }

                Table table = connection.getTable(TableName.valueOf(TABLE_NAME));

                print("Write some Records to the table");
                for (int i = 0; i < Records.length; i++) {
                    String rowKey = "row" + i;
                    Put put = new Put(Bytes.toBytes(rowKey));
                    put.addColumn(COLUMN_FAMILY_NAME, COLUMN_NAME, Bytes.toBytes(Records[i]));
                    table.put(put);
                }

                print("Reading Records from table ");
                for (int i = 0; i < Records.length; i++) {
                    String rowKey1 = "row" + i;
                    Result getResult = table.get(new Get(Bytes.toBytes(rowKey1)));
                    String val = Bytes.toString(getResult.getValue(COLUMN_FAMILY_NAME, COLUMN_NAME));
                    System.out.printf("\t%s = %s\n", rowKey1, val);

                }

                Scan scan = new Scan();
                scan.addColumn(COLUMN_FAMILY_NAME,COLUMN_NAME);

                print("Scan for all Records");
                ResultScanner scanner = table.getScanner(scan);
                for (Result row : scanner) {
                    byte[] valueBytes = row.getValue(COLUMN_FAMILY_NAME, COLUMN_NAME);
                    System.out.println('\t' + Bytes.toString(valueBytes));
                }

                Delete delete = new Delete(Bytes.toBytes("row0"));
//                delete.addColumn(COLUMN_FAMILY_NAME, COLUMN_NAME);
//                delete.addFamily(COLUMN_FAMILY_NAME, 2l);
//                delete.addFamilyVersion(COLUMN_FAMILY_NAME, 2l);
                table.delete(delete);

                String rowKey = "row0";

                //Inserting a row with column.
                Put put1 = new Put(Bytes.toBytes(rowKey));
                put1.addColumn(COLUMN_FAMILY_NAME, COLUMN_NAME, Bytes.toBytes("Inserting a row with column"));
                table.put(put1);

                //Inserting a row with multiple columns.
                Put put2 = new Put(Bytes.toBytes(rowKey));
                put2.addColumn(COLUMN_FAMILY_NAME, COLUMN_NAME, Bytes.toBytes("col1"));
                put2.addColumn(COLUMN_FAMILY_NAME, COLUMN_NAME2, Bytes.toBytes("col2"));
                table.put(put2);

                //Inserting multiple rows at one time
                List<Put> puts = new ArrayList<Put>();
                Put put11 = new Put(Bytes.toBytes(rowKey));
                put11.addColumn(COLUMN_FAMILY_NAME, COLUMN_NAME, Bytes.toBytes("col2"));
                puts.add(put11);
                Put put22 = new Put(Bytes.toBytes("greeting2"));
                put22.addColumn(COLUMN_FAMILY_NAME, COLUMN_NAME2, Bytes.toBytes("col2"));
                puts.add(put22);
                table.put(puts);

                //Inserting a row with multiple columnFamily
                Put put3 = new Put(Bytes.toBytes(rowKey));
                put3.addColumn(COLUMN_FAMILY_NAME, COLUMN_NAME2, Bytes.toBytes("col1"));
                put3.addColumn(COLUMN_FAMILY_NAME2, COLUMN_NAME2, Bytes.toBytes("col2"));
                table.put(put3);

//        print("Delete the table");
//        admin.disableTable(table.getName());
//        admin.deleteTable(table.getName());

            } catch (IOException e) {
                if (admin.tableExists(TableName.valueOf(TABLE_NAME))) {
                    print("Cleaning up table");
                    admin.disableTable(TableName.valueOf(TABLE_NAME));
                    admin.deleteTable(TableName.valueOf(TABLE_NAME));
                }
                throw e;
            }
        } catch (IOException e) {
            System.err.println("Exception while running : " + e.getMessage());
            e.printStackTrace();

            System.exit(1);
        }

        System.exit(0);
    }

    private static void print(String msg) {
        System.out.println(" ========================== " + msg + " ========================== ");
    }

    public static void main(String[] args) {
        // Consult system properties to get project/instance
        String projectId = requiredProperty("bigtable.projectID");
        String instanceId = requiredProperty("bigtable.instanceID");

        execute(projectId, instanceId);
    }

    private static String requiredProperty(String prop) {
        String value = System.getProperty(prop);
        if (value == null) {
            throw new IllegalArgumentException("Missing required system property: " + prop);
        }
        return value;
    }
}