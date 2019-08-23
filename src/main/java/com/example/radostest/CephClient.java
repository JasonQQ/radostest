package com.example.radostest;

import com.ceph.rados.Rados;

import com.ceph.rados.exceptions.RadosException;

import java.io.File;

public class CephClient {

    public static void main(String args[]) {
        try {
            //创建连接句柄对象
            Rados cluster = new Rados("admin");
            System.out.println("Created cluster handle.");
            //读取ceph集群配置文件
            File f = new File("/etc/ceph/ceph.conf");
            cluster.confReadFile(f);
            System.out.println("Read the configuration file.");
            //连接ceph集群
            cluster.connect();
            System.out.println("Connected to the cluster.");
        } catch (RadosException e) {
            System.out.println(e.getMessage() + ": " + e.getReturnValue());
        }
    }
}

