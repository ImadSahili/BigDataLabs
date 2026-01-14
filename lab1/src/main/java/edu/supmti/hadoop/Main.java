package edu.supmti.hadoop;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

// import org.apache.hadoop.fs.Path;
public class Main {
    public static void main(String[] args) throws IOException {
        Configuration conf = new Configuration();
        // conf.set("fs.defaultFS", "hdfs://localhost:9000");
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

    }
}