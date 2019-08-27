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

    @PostMapping("/pool/{pool}/fileName/{fileName}/write")
    public String write(
            @PathVariable String pool,
            @PathVariable String fileName,
            @RequestBody byte[] bytes) {
        String result = "Failed";
        long startMethod = System.currentTimeMillis();
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
            System.out.println("io write " + bytes.length);

            cluster.ioCtxDestroy(io);
            System.out.println("io Ctx destroyed");

        } catch (RadosException e) {
            System.out.println(e.getMessage() + ": " + e.getReturnValue());
        }

        long endMethod = System.currentTimeMillis();
        System.out.println("------>write timeCost(ms)=" + (endMethod - startMethod));
        return result;

    }

    @PostMapping("/pool/{pool}/fileName/{fileName}/{length}/read")
    public byte[] read(
            @PathVariable String pool,
            @PathVariable String fileName,
            @PathVariable int length) {
        byte[] buf = new byte[length];
        try {
            Rados cluster = new Rados("admin");
            System.out.println("Create cluster handle.");
            File conf = new File("/etc/ceph/ceph.conf");
            cluster.confReadFile(conf);
            System.out.println("Read the Ceph config file.");

            cluster.connect();
            System.out.println("Connected to the Ceph cluster");

            IoCTX io = cluster.ioCtxCreate(pool);
            System.out.println("io Ctx created");

            int len = io.read(fileName, length, 0, buf);
            System.out.println("io read " + length);

            cluster.ioCtxDestroy(io);
            System.out.println("io Ctx destroyed");

        } catch (RadosException e) {
            System.out.println(e.getMessage() + ": " + e.getReturnValue());
        }
        return buf;

    }
}
