package com.example.radostest;

import com.ceph.rados.IoCTX;
import com.ceph.rados.Rados;
import com.ceph.rados.exceptions.RadosException;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpServletResponse;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;

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

    @GetMapping("/pool/{pool}/fileName/{fileName}/{length}/read")
    public void read(
            @PathVariable String pool,
            @PathVariable String fileName,
            @PathVariable int length,
            HttpServletResponse response) {
        long startTime = System.currentTimeMillis();
        byte[] buf = new byte[length];
        response.reset();
        response.setContentType("application/x-msdownload");
        response.setHeader("Content-Disposition", "attachment; filename=" + fileName);
        OutputStream outputStream = null;
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

            outputStream = response.getOutputStream();
            outputStream.write(buf, 0, len);

            cluster.ioCtxDestroy(io);
            System.out.println("io Ctx destroyed");

        } catch (RadosException e) {
            System.out.println(e.getMessage() + ": " + e.getReturnValue());
        } catch (IOException e) {
            System.out.println("Fail to write to outputStream: " + e.getMessage());
        } finally {
            if (outputStream != null) {
                try {
                    outputStream.close();
                } catch (IOException e) {
                    System.out.println("Fail to close outputStream: " + e.getMessage());
                }
            }
        }
        long endTime = System.currentTimeMillis();
        System.out.println("------>read/download timeCost(ms)=" + (endTime - startTime));
    }
}
