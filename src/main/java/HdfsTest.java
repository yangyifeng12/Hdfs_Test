import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.net.URI;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.jupiter.api.Test;
import org.apache.hadoop.mapreduce.Job;


public class HdfsTest {

    @Test
    public void testRead()throws Exception{
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://node151:8020"), conf, "bduser");
        FSDataInputStream fsdis = fs.open(new Path("/root/sparkTest.txt"));

        FileOutputStream fos = new FileOutputStream(new File("D:/aaaa.txt"));

        IOUtils.copyBytes(fsdis, fos, conf);;
        IOUtils.closeStream(fsdis);
        IOUtils.closeStream(fos);
        fs.close();

    }

    @Test
    public void testWrite()throws Exception{
        //因为上传文件会涉及到权限，所以需要制定配置文件来更改上传的用户
        Configuration conf = new Configuration();
        conf.addResource("");
        FileSystem fs = FileSystem.get(new URI("hdfs://node151:8020"), new Configuration());
        FSDataOutputStream fsdos = fs.create(new Path("/root/QYJ_QIYIACCT_FUND_TRANS2.txt"));
        FileInputStream fis = new FileInputStream(new File("C:\\Users\\pactera\\Desktop\\QYJ_QIYIACCT_FUND_TRANS2.txt"));
        IOUtils.copyBytes(fis, fsdos, conf);;
        IOUtils.closeStream(fis);
        IOUtils.closeStream(fsdos);
        fs.close();

    }

    @Test
    public void testRead1() throws Exception{
        // 1 创建配置信息对象
        Configuration configuration = new Configuration();

        FileSystem fs = FileSystem.get(new URI("hdfs://node151:8020"), configuration, "bduser");

        // 2 获取输入流路径
        Path path = new Path("/root/jdk-7u79-linux-x64.tar.gz");

        // 3 打开输入流
        FSDataInputStream fis = fs.open(path);

        // 4 创建输出流
        FileOutputStream fos = new FileOutputStream("D:/jdk.part1");

        // 5 流对接
        byte[] buf = new byte[1024*1024];
        for (int i = 0; i < 128; i++) {
            fis.read(buf);
            fos.write(buf);
        }

        // 6 关闭流
        IOUtils.closeStream(fis);
        IOUtils.closeStream(fos);

    }

    @Test
    public void testRead2() throws Exception{
        // 1 创建配置信息对象
        Configuration configuration = new Configuration();

        FileSystem fs = FileSystem.get(new URI("hdfs://node151:8020"), configuration, "bduser");

        // 2 获取输入流路径
        Path path = new Path("/root/jdk-7u79-linux-x64.tar.gz");

        // 3 打开输入流
        FSDataInputStream fis = fs.open(path);

        // 4 创建输出流
        FileOutputStream fos = new FileOutputStream("D:/jdk.part2");

        // 5 定位偏移量（第二块的首位）
        fis.seek(1024 * 1024 * 128);

        // 6 流对接
        IOUtils.copyBytes(fis, fos, configuration);

        // 7 关闭流
        IOUtils.closeStream(fis);
        IOUtils.closeStream(fos);

    }
}

