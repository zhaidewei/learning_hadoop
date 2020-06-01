package com.kkb;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

public class HdfsOperationsDemo {

    public static void main(String[] args) throws IOException {
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "hdfs://node01:8020");
        FileSystem fs = FileSystem.get(configuration);
        LocalFileSystem lfs = FileSystem.getLocal(new Configuration());

        //
        mkDir(fs);
        copyFromLocal(fs, args);
        listFiles(fs, args);
        rmFile(fs, args);

        putToHdfsViaStream(fs, args);

        combinSmallFiles(fs,lfs, args);

        fs.close();
        lfs.close();
    }

    static void mkDir(FileSystem fs) throws IOException {
        fs.mkdirs(new Path("/kkb/dir1"));
    }

    static void copyFromLocal(FileSystem fs, String[] args) throws IOException {
        fs.copyFromLocalFile(
                new Path(args[0]),
                new Path(args[1]));
    }

    //
    static void listFiles(FileSystem fs, String[] args) throws IOException {
        RemoteIterator<LocatedFileStatus> fileListed = fs.listFiles(new Path(args[1]), false);
        while (fileListed.hasNext()) {
            LocatedFileStatus fileStatus = fileListed.next();
            System.out.println(fileStatus.getAccessTime());
            System.out.println(fileStatus.getBlockSize());
            System.out.println(fileStatus.getGroup());
            System.out.println(fileStatus.getLen());
            System.out.println(fileStatus.getOwner());
            System.out.println(fileStatus.getPath().toString());
            BlockLocation[] bls = fileStatus.getBlockLocations();

            for (BlockLocation b:bls) {
                System.out.println(b.toString());
            }
        }
    }

    static void rmFile(FileSystem fs, String[] args) throws IOException {
        // delete so we can re-upload in next step
        fs.delete(new Path(args[1]), false);
    }

    static void putToHdfsViaStream(FileSystem fs, String[] args) throws IOException {
        // 2 创建输入流 不需要加file:///
        FileInputStream fis = new FileInputStream(
                new File(args[0].replace("file://","")));

        // 3 获取输出流
        FSDataOutputStream fos = fs.create(new Path(args[1]));

        // 4 流对拷
        IOUtils.copy(fis, fos);

        // 5 关闭资源
        IOUtils.closeQuietly(fis);
        IOUtils.closeQuietly(fos);
    }

    static void combinSmallFiles(FileSystem fs, LocalFileSystem lfs,String[] args) throws IOException {
        FSDataOutputStream fos = fs.create(new Path(args[3]));
        //获取本地文件系统 localFileSystem

        //读取本地的文件
        FileStatus[] fileStatuses = lfs.listStatus(new Path(args[2]));

        for (FileStatus fileStatus:fileStatuses) {
            //获取每一个本地的文件路径
            Path path = fileStatus.getPath();
            //读取本地小文件
            FSDataInputStream fis = lfs.open(path);
            IOUtils.copy(fis,fos);
            IOUtils.closeQuietly(fis);
        }
        IOUtils.closeQuietly(fos);
    }

}
