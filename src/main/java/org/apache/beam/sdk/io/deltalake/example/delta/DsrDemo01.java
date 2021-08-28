package org.apache.beam.sdk.io.deltalake.example.delta;

import io.delta.standalone.DeltaLog;
import io.delta.standalone.Snapshot;
import io.delta.standalone.data.CloseableIterator;
import io.delta.standalone.data.RowRecord;

import org.apache.hadoop.conf.Configuration;

/**
 * https://devportal.intuit.com/app/dp/resource/4903311686969652377/msaasresource
 *
 * https://devportal.intuit.com/app/dp/resource/8506191388865920037/msaasresource
 * https://github.intuit.com/data-strmprocess/beam-sdks-java-io-deltalake
 * https://build.intuit.com/ip-data-platform/job/data-strmprocess/job/beam-sdks-java-io-deltalake/job/beam-sdks-java-io-deltalake/job/master/
 */

public class DsrDemo01
{
    // Data created in SparkInAction2 project; com.intuit.my.deltalake.QuickStart03
    static final String TABLE_PATH =
        // "/Users/mbenenson/my/intuit/data-strmprocess/mb-proc-dec-08/data/delta-lake-03/"
        "s3a://idl-spp-deltalake-test-mb/delta-lake-stream-01/"
    ;

    public static void main(String[] args)
    {
        Configuration hadoopConfiguration = new Configuration();

        //* -------
        // eiamCli login
        // eiamCli getAWSTempCredentials -a 606483043239 -r Developer -p default
        // aws s3 ls s3://idl-spp-deltalake-test-mb/delta-lake-01/  --profile default
        hadoopConfiguration.set("fs.s3a.aws.credentials.provider",
            "com.amazonaws.auth.profile.ProfileCredentialsProvider");
        //  ------*/

        DeltaLog log = DeltaLog.forTable(hadoopConfiguration, TABLE_PATH);

        Snapshot snapshot = log
        //    .snapshot()
            .getSnapshotForVersionAsOf(1)
        ;
        printSnapshotDetails("current snapshot", snapshot);

        printRecords(snapshot, 1_000);

        System.out.println("----------- completed -------------");
    }

    public static void printSnapshotDetails(String title, Snapshot snapshot) {
        System.out.println("===== " + title + " =====");
        System.out.println("version: " + snapshot.getVersion());
        System.out.println("number of data files: " + snapshot.getAllFiles().size());

        System.out.println("data files:");
        snapshot.getAllFiles().forEach(file -> System.out.println("  " + file.getPath()));
    }

    public static void printRecords(Snapshot snapshot, int printCnt)
    {
        CloseableIterator<RowRecord> iter = snapshot.open();

        System.out.println("\ndata rows:");
        RowRecord row = null;
        int numRows = 0;
        while (iter.hasNext()) {
            row = iter.next();
            numRows++;

            Long id = row.isNullAt("id") ? null : row.getLong("id");
            String key = row.getString("key");
            String name = row.getString("name");
            String data = row.getString("data");
            if (numRows < printCnt) {
                System.out.println("id=" + id + ", key=" + key + ", name=" + name + ", data: " + data);
            } else {
                System.out.println("Not all data have been read");
                break;
            }
        }

        System.out.println("\nnumber of rows: " + numRows);
        System.out.println("data schema:");
        System.out.println(row.getSchema().getTreeString());
        System.out.println("\n");
    }

}
