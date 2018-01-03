package com.gzq.storm.utils;

import java.io.IOException;
import java.io.OutputStream;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;

/**
 * ${DESCRIPTION}
 *
 * @author guozhiqiang
 * @created 2017-12-26 16:27.
 */
public class NCUtil {
    /**
     * 返回主机名
     *
     * @return
     */
    public static String getHostName() {
        try {
            String hostName = InetAddress.getLocalHost().getHostName();
            return hostName;
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 返回进程pid
     *
     * @return
     */
    public static String getPID() {
        String info = ManagementFactory.getRuntimeMXBean().getName();
        String pid = info.split("@")[0];
        return pid;
    }

    /**
     * 返回线程id
     *
     * @return
     */
    public static String getTID() {
        String tid = Thread.currentThread().getName();
        return tid;
    }

    /**
     * 返回对象信息
     *
     * @param object
     * @return
     */
    public static String getOID(Object object) {
        String cname = object.getClass().getName();
        int hashCode = object.hashCode();

        return cname + "@" + hashCode;
    }

    public static String info(Object obj, String msg) {
        return getHostName() + "," + getPID() + "," + getTID() + "," + getOID(obj) + "," + msg;
    }

    public static void sendMessageToClient(Object object, String msg) {
        OutputStream outputStream =null;
        try {
            String info = info(object, msg);
            Socket socket = new Socket("192.168.11.160", 8888);
            outputStream = socket.getOutputStream();
            outputStream.write((info+"\r\n").getBytes());
            outputStream.flush();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (outputStream != null) {
                try {
                    outputStream.close();
                } catch (IOException e) {
                }
            }
        }

    }
}
