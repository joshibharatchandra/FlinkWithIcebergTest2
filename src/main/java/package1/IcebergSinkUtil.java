package package1;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.data.RowData;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.flink.sink.FlinkSink;
import org.apache.iceberg.types.Types;

public class IcebergSinkUtil {

    public static void writeToIceberg(DataStream<RowData> dataStream, String catalogPath, String bucketName, String tableName) {
        //Step 2- Define the Iceberg schema (match with your data model)
        Schema schema = new Schema(
                Types.NestedField.required(1, "order_id", Types.LongType.get()),
                Types.NestedField.required(2, "place_time", Types.LongType.get()),
                Types.NestedField.required(3, "addr_lon", Types.FloatType.get()),
                Types.NestedField.required(4, "addr_lat", Types.FloatType.get()),
                Types.NestedField.optional(5, "pizza_type", Types.StringType.get()),
                Types.NestedField.optional(6, "status", Types.StringType.get())
        );

        // Disable security manager checks (for non-secure environments)

        // Step-3 Configure Hadoop catalog
        Configuration hadoopConf = new Configuration();
        hadoopConf.set("hadoop.security.authentication", false);


        hadoopConf.set("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem");
        hadoopConf.set("spark.hadoop.fs.gs.auth.service.account.json.keyfile", "C:/Users/bhara/Downloads/your-service-account-key.json");
        hadoopConf.set("fs.defaultFS", "gs://" + bucketName);
        hadoopConf.set("google.cloud.auth.service.account.enable", "true");
        hadoopConf.set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS");

        HadoopCatalog catalog = new HadoopCatalog(hadoopConf, catalogPath);

        // Step -4 Define the table identifier
        TableIdentifier tableId = TableIdentifier.of("default", tableName);

        //Step 5-  Write the data to Iceberg
        FlinkSink.forRowData(dataStream)
                .table(catalog.loadTable(tableId))
                .overwrite(false) // Set to true if you want to overwrite the table
                .build();
    }
}
