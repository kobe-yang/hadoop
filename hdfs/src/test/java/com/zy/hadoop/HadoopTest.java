package com.zy.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;


/**
 * @FileName: HadoopTest
 * @Author: kobe-yang
 * @Date: 2021/4/7 10:55 上午
 * @Description:
 */

public class HadoopTest {

    /**
     * create Dir
     */
    @Test
    public void makeDir() {
        Configuration configuration = new Configuration();
        //设置hadoop集群
        configuration.set("fs.defaultFS", "hdfs://node01:8020");

        //获取文件系统
        try (FileSystem fileSystem = FileSystem.get(configuration)) {
            fileSystem.mkdirs(new Path("/zy/test"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 创建文件夹指定 用户 并指定权限
     */
    @Test
    public void mkDirForUsers() {
        //配置项
        Configuration configuration = new Configuration();
        //获得文件系统 configuration 必选传递 即使没有赋予属性，否则就创建不成功
        try (FileSystem fileSystem = FileSystem.get(new URI("hdfs://node01:8020"), configuration, "test");) {

            //指定权限
            FsPermission fsPermission = new FsPermission(FsAction.ALL, FsAction.READ, FsAction.READ);

            //调用方法创建目录
            fileSystem.mkdirs(new Path("/aaa"), fsPermission);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }


    @Test
    public void makeFile() {
        Configuration configuration = new Configuration();
        //设置hadoop集群
        configuration.set("fs.defaultFS", "hdfs://node01:8020");

        try (FileSystem fileSystem = FileSystem.get(configuration)) {
            fileSystem.create(new Path("/zy/xxxs.txt"));
        } catch (IOException e) {

        }
    }

    /**
     * upload File
     */
    @Test
    public void uploadFile() {
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "hdfs://node01:8020");
        try (FileSystem fileSystem = FileSystem.get(configuration)) {
            fileSystem.copyFromLocalFile(
                    new Path("/Users/zy1994/Desktop/kaikeba/bigdata/hadoop/code/hadoop/hdfs/src/main/resources/hello.text")
                    , new Path("/zy/test"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
