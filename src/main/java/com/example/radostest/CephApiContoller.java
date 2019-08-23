package com.example.radostest;

import com.ceph.rados.Rados;
import com.ceph.rados.exceptions.RadosException;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.File;

@RestController
public class CephApiContoller {

    @GetMapping("test")
    public String connectToCephCluster() {
        String result = "Failed";
        try {
            Rados cluster = new Rados("admin");
            System.out.println("Create cluster handle.");
            File conf = new File("/etc/ceph/ceph.conf");
            cluster.confReadFile(conf);
            System.out.println("Read the Ceph config file.");

            cluster.connect();
            result = "Connected to the Ceph cluster";
            System.out.println(result);

        } catch (RadosException e) {
            System.out.println(e.getMessage() + ": " + e.getReturnValue());
        }
        return result;
    }
}
