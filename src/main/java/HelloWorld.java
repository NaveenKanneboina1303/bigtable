
import com.google.cloud.bigtable.hbase.BigtableConfiguration;

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
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class HelloWorld {

    private static final byte[] TABLE_NAME = Bytes.toBytes(requiredProperty("bigtable.tableName"));
    private static final byte[] COLUMN_FAMILY_NAME = Bytes.toBytes("cf1");
    private static final byte[] COLUMN_NAME = Bytes.toBytes("column1");

    private static final String[] GREETINGS = {
            "Hello World!", "Hello Cloud Bigtable!", "Hello HBase!"
    };

    private static void doHelloWorld(String projectId, String instanceId) {

        try (Connection connection = BigtableConfiguration.connect(projectId, instanceId)) {
            Admin admin = connection.getAdmin();

            try {

                if (admin.tableExists(TableName.valueOf(TABLE_NAME))) {
                    print("Table " + TABLE_NAME + " already exists");
                } else {
                    HTableDescriptor descriptor = new HTableDescriptor(TableName.valueOf(TABLE_NAME));
                    descriptor.addFamily(new HColumnDescriptor(COLUMN_FAMILY_NAME));
                    print("Create table " + descriptor.getNameAsString());
                    admin.createTable(descriptor);
                }

                Table table = connection.getTable(TableName.valueOf(TABLE_NAME));

                print("Write some greetings to the table");
                for (int i = 0; i < GREETINGS.length; i++) {
                    String rowKey = "greeting" + i;
                    Put put = new Put(Bytes.toBytes(rowKey));
                    put.addColumn(COLUMN_FAMILY_NAME, COLUMN_NAME, Bytes.toBytes(GREETINGS[i]));
                    table.put(put);
                }

                String rowKey = "greeting0";
                Result getResult = table.get(new Get(Bytes.toBytes(rowKey)));
                String greeting = Bytes.toString(getResult.getValue(COLUMN_FAMILY_NAME, COLUMN_NAME));
                System.out.println("Get a single greeting by row key");
                System.out.printf("\t%s = %s\n", rowKey, greeting);

                Scan scan = new Scan();
                scan.addColumn(COLUMN_FAMILY_NAME,COLUMN_NAME);

                print("Scan for all greetings:");
                ResultScanner scanner = table.getScanner(scan);
                for (Result row : scanner) {
                    byte[] valueBytes = row.getValue(COLUMN_FAMILY_NAME, COLUMN_NAME);
                    System.out.println('\t' + Bytes.toString(valueBytes));
                }

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
            System.err.println("Exception while running HelloWorld: " + e.getMessage());
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

        doHelloWorld(projectId, instanceId);
    }

    private static String requiredProperty(String prop) {
        String value = System.getProperty(prop);
        if (value == null) {
            throw new IllegalArgumentException("Missing required system property: " + prop);
        }
        return value;
    }
}