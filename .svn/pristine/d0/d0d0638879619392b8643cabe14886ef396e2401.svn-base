package com.fuhao.data.hadoop;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;


import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;


/**
 * @author lz
 *  2017/3/22
 */
public class HadoopClient {


    /**
     *创建文件，需要两个参数
     * @param url hdfs://192.168.1.116:8020
     * @param path hdfs://192.168.1.116:8020/dirs
     */
    public  void createFile(String url,String path){
        try {
            //该类的对象封转了客户端或者服务器的配置。
            Configuration config = new Configuration();
            URI uri = new URI(url);
            //该类的对象是一个文件系统对象，可以用该对象的一些方法来对文件进行操作。
            FileSystem fs = FileSystem.get(uri, config);
            //目标路径
            Path p = new Path(path);
            fs.create(p);
            fs.close();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 判断文件是否存在
     * @param url hdfs://192.168.1.116:8020
     * @param FileName  hdfs://192.168.1.116:8020/${fileName}
     * @return  true存在,flase不存在
     * @throws Exception
     */
    public  boolean IfFileExits(String url,String FileName)
    {
        Configuration config = new Configuration();
        boolean isExists = false;
        try {
            URI uri = new URI(url);
            FileSystem fs = FileSystem.get(uri, config);
            Path path = new Path(FileName);
           isExists = fs.exists(path);
            fs.close();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
        return isExists;
    }

    /**
     * 删除文件
     * @param url hdfs://192.168.1.116:8020
     * @param filePath  hdfs://192.168.1.116:8020/${fileName}
     * @return
     * @throws IOException
     */
    public  boolean delete(String url,String filePath){
        boolean isok = false;
        try {
            Configuration conf = new Configuration();
            URI uri = new URI(url);
            FileSystem fs = FileSystem.get(uri,conf);
            Path path = new Path(filePath);
            isok = fs.deleteOnExit(path);
            fs.close();
        } catch (IllegalArgumentException e) {
            e.printStackTrace();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return isok;
    }

    /**
     * 上传文件
     * @param url hdfs://192.168.1.116:8020 /log/
     * @param src D://
     * @param dst hdfs://192.168.1.116:8020/
     * @throws IOException
     */
    public  void uploadFile(String url,String src,String dst){
        try {
            Configuration conf = new Configuration();
            URI uri = new URI(url);
            FileSystem fs = FileSystem.get(uri,conf);
            Path srcPath = new Path(src); //原路径
            Path dstPath = new Path(dst); //目标路径
            //调用文件系统的文件复制函数,前面参数是指是否删除原文件，true为删除，默认为false
            fs.copyFromLocalFile(false,srcPath, dstPath);
            fs.close();
        } catch (IllegalArgumentException e) {
            e.printStackTrace();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void createFile(String url ,String dst,byte[] contents){
        try {
            Configuration conf = new Configuration();
            URI uri = new URI(url);
            FileSystem fs = FileSystem.get(uri,conf);
            Path dstPath = new Path(dst); //目标路径
            FSDataOutputStream fsDataOutputStream = fs.create(dstPath);
            fsDataOutputStream.write(contents);
            fsDataOutputStream.close();
            fs.close();
        } catch (IllegalArgumentException e) {
            e.printStackTrace();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     *
     * @param url
     * @param src
     * @param dst
     * @throws IOException
     */
    public  void downFile(String url,String src,String dst) {
        try {
            Configuration conf = new Configuration();
            URI uri = new URI(url);
            FileSystem fs = FileSystem.get(uri,conf);
            Path srcPath = new Path(src); //原路径
            Path dstPath = new Path(dst); //目标路径
            //调用文件系统的文件复制函数,前面参数是指是否删除原文件，true为删除，默认为false
            fs.copyToLocalFile(false,srcPath, dstPath);
            //打印文件路径
            fs.close();
        } catch (IllegalArgumentException e) {

        } catch (URISyntaxException e) {
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    /**
     *
     * @param url
     * @param oldName
     * @param newName
     * @return
     * @throws IOException
     */
    public  boolean rename(String url,String oldName,String newName) {
        boolean isok = false;
        try {
            Configuration conf = new Configuration();
            URI uri = new URI(url);
            FileSystem fs = FileSystem.get(uri,conf);
            Path oldPath = new Path(oldName);
            Path newPath = new Path(newName);
            isok = fs.rename(oldPath, newPath);
            fs.close();
        } catch (IllegalArgumentException e) {
            e.printStackTrace();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return isok;
    }

    /**
     *
     * @param url
     * @param path
     * @return
     * @throws IOException
     */
    public  boolean mkdir(String url,String path){
        boolean isok = false;
        try {
            Configuration conf = new Configuration();
            URI uri = new URI(url);
            FileSystem fs = FileSystem.get(uri,conf);
            Path srcPath = new Path(path);
            isok = fs.mkdirs(srcPath);
            fs.close();
        } catch (IllegalArgumentException e) {
            e.printStackTrace();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return isok;
    }

    //读取文件的内容
    public  byte[] readFile(String url,String filePath) {
        InputStream in = null;
        byte[] bytes = null;
        try {
            Configuration conf = new Configuration();
            URI uri = new URI(url);
            FileSystem fs = FileSystem.get(uri,conf);
            Path srcPath = new Path(filePath);

            in = fs.open(srcPath);
             bytes = new byte[in.available()];
            in.read(bytes);
        } catch (IllegalArgumentException e) {
            e.printStackTrace();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                in.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return bytes;
    }

    public static void main(String[] args) {
        String url = "hdfs://192.168.1.116:8020";
        HadoopClient client = new HadoopClient();
        client.createFile(url,url+"/lz.txt","Hello world".getBytes());
        byte[] bytes = client.readFile(url,url+"/lz.txt");
        System.out.println(new String(bytes));
    }
}
