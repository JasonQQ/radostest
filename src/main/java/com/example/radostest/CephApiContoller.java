package com.example.radostest;

import com.ceph.rados.IoCTX;
import com.ceph.rados.Rados;
import com.ceph.rados.exceptions.RadosException;
import org.springframework.web.bind.annotation.*;

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

    @PostMapping("/fileName/{fileName}/write")
    public String write(
            @PathVariable String pool,
            @PathVariable String fileName,
            @RequestBody byte[] bytes) {
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

            IoCTX io = cluster.ioCtxCreate(pool);
            System.out.println("io Ctx created");

            io.write(fileName, bytes);
            System.out.println("io write");

            cluster.ioCtxDestroy(io);
            System.out.println("io Ctx destroyed");

        } catch (RadosException e) {
            System.out.println(e.getMessage() + ": " + e.getReturnValue());
        }
        return result;

    }
}
