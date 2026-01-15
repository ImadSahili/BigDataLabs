package edu.supmti.hadoop;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

public class Main {
    public static void main(String[] args) throws IOException {
        Configuration conf = new Configuration();
        // conf.set("fs.defaultFS", "hdfs://localhost:9000");
        // System.setProperty("HADOOP_USER_NAME", "root");
        FileSystem fs = FileSystem.get(conf);
        if (args.length < 1) {
            System.out.println("Usage: HadoopFileStatus <file-path>");
            System.exit(1);
        }
        Path path = new Path(args[0]);// "/user/root/input/alice.txt"
        FileStatus status = fs.getFileStatus(path);
        System.out.println("File: " + status.getPath());
        System.out.println("Size: " + status.getLen() + " bytes");
        System.out.println("Owner: " + status.getOwner());
        System.out.println("Group: " + status.getGroup());
        System.out.println("Permission: " + status.getPermission());
        System.out.println("Is Directory: " + status.isDirectory());
        System.out.println("Modification Time: " + status.getModificationTime());

        BlockLocation[] blockLocations = fs.getFileBlockLocations(status, 0, status.getLen());
        for (BlockLocation blockLocation : blockLocations) {
            System.out.println("Block Offset: " + blockLocation.getOffset());
            System.out.println("Block Length: " + blockLocation.getLength());
            String[] hosts = blockLocation.getHosts();
            for (String host : hosts) {
                System.out.println("Stored on: " + host);
            }
        }

        if (args.length >= 2) {
            Boolean succes = fs.rename(new Path(args[0]), new Path(args[1]));
            System.out.println("Renaming succesful: " + succes);
        }
    }
}