import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;

import org.apache.log4j.*;
import java.util.*;

public class Hw1Grp4 {
    public static void main(String[] args)
            throws IOException, URISyntaxException, MasterNotRunningException, ZooKeeperConnectionException {
        if (args.length != 3) {
            System.out.println("arguments not match, please check.");
            System.exit(1);
        }

        // parse argument
        String file = args[0].substring(2);
        int selectCol = Integer.parseInt(args[1].substring(8).split(",")[0]);
        String selectOp = args[1].substring(8).split(",")[1];
        double selectValue = Double.parseDouble(args[1].substring(8).split(",")[2]);

        String[] distinctCol = args[2].substring(9).split(",");

        // init a hashtable
        HashMap distinctMap = new HashMap();

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(file), conf);
        Path path = new Path(file);
        FSDataInputStream in_stream = fs.open(path);

        BufferedReader in = new BufferedReader(new InputStreamReader(in_stream));
        String s;
        while ((s = in.readLine()) != null) {
            // get each row from table
            String[] currentRow = s.split("\\|");
            // filter, then put the key into hashtable
            switch (selectOp) {
                case "gt":
                    if (Double.parseDouble(currentRow[selectCol]) > selectValue) {
                        String tmp_str = "";
                        for (String str : distinctCol) {
                            tmp_str += currentRow[Integer.parseInt(str.substring(1))] + ",";
                        }
                        distinctMap.put(tmp_str, 1);
                    }
                    break;
                case "ge":
                    if (Double.parseDouble(currentRow[selectCol]) >= selectValue) {
                        String tmp_str = "";
                        for (String str : distinctCol) {
                            tmp_str += currentRow[Integer.parseInt(str.substring(1))] + ",";
                        }
                        distinctMap.put(tmp_str, 1);
                    }
                    break;
                case "eq":
                    if (Double.parseDouble(currentRow[selectCol]) == selectValue) {
                        String tmp_str = "";
                        for (String str : distinctCol) {
                            tmp_str += currentRow[Integer.parseInt(str.substring(1))] + ",";
                        }
                        distinctMap.put(tmp_str, 1);
                    }
                    break;
                case "ne":
                    if (Double.parseDouble(currentRow[selectCol]) != selectValue) {
                        String tmp_str = "";
                        for (String str : distinctCol) {
                            tmp_str += currentRow[Integer.parseInt(str.substring(1))] + ",";
                        }
                        distinctMap.put(tmp_str, 1);
                    }
                    break;
                case "le":
                    if (Double.parseDouble(currentRow[selectCol]) <= selectValue) {
                        String tmp_str = "";
                        for (String str : distinctCol) {
                            tmp_str += currentRow[Integer.parseInt(str.substring(1))] + ",";
                        }
                        distinctMap.put(tmp_str, 1);
                    }
                    break;
                case "lt":
                    if (Double.parseDouble(currentRow[selectCol]) < selectValue) {
                        String tmp_str = "";
                        for (String str : distinctCol) {
                            tmp_str += currentRow[Integer.parseInt(str.substring(1))] + ",";
                        }
                        distinctMap.put(tmp_str, 1);
                    }
                    break;
                default:
                    System.out.println("Operator of the filter is wrong.");
                    System.exit(1);
                    break;
            }
        }

        in.close();

        fs.close();
        Iterator iter = distinctMap.keySet().iterator();
        int num = 0;
        while (iter.hasNext()) {
            Logger.getRootLogger().setLevel(Level.WARN);

            // create table descriptor
            String tableName = "Result";
            HTableDescriptor htd = new HTableDescriptor(TableName.valueOf(tableName));

            // create column descriptor
            HColumnDescriptor cf = new HColumnDescriptor("res");
            htd.addFamily(cf);

            // configure HBase
            Configuration configuration = HBaseConfiguration.create();
            HBaseAdmin hAdmin = new HBaseAdmin(configuration);

            if (hAdmin.tableExists(tableName)) {
                System.out.println("Table already exists");
                // hAdmin.disableTable(tableName);
                // hAdmin.deleteTable(tableName);
                // hAdmin.createTable(htd);
            } else {
                hAdmin.createTable(htd);
                System.out.println("table " + tableName + " created successfully");
            }
            hAdmin.close();

            // put

            HTable table = new HTable(configuration, tableName);
            Put put = new Put(Integer.toString(num).getBytes());
            String[] tmp_res = ((String)iter.next()).split(",");
            System.out.println("str length: " + tmp_res.length);
            for(int i = 0; i < tmp_res.length; i++) {
                put.add("res".getBytes(), (distinctCol[i]).getBytes(), tmp_res[i].getBytes());
                System.out.println("R" + distinctCol[i] + "res: " + tmp_res[i]);
            }
            table.put(put);
            if(!iter.hasNext()) {
                table.close();
            }
            System.out.println("put successfully");
            num++;
        }
    }
}
