
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Hdfs;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.IOException;

public class HdfsOperationsByJava {

    public static void main(String[] args) throws IOException {
       mkDir();
       copyFromLocal();
    }


    static void mkDir() throws IOException {
        String nameNodeURL =  "hdfs://node01:8020";
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", nameNodeURL);
        FileSystem fs = FileSystem.get(configuration);
        fs.mkdirs(new Path("/kkb/dir1"));
        fs.close();
    }

    static void copyFromLocal() throws IOException {
        String nameNodeURL =  "hdfs://node01:8020";
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", nameNodeURL);
        FileSystem fs = FileSystem.get(configuration);
        String currentDir = new File("").getAbsolutePath();
        fs.copyFromLocalFile(
                new Path("file://" + currentDir + "/pom.xml"),
                new Path( nameNodeURL + "/kkb/dir1/pom.xml"));
        fs.close();
    }
}
