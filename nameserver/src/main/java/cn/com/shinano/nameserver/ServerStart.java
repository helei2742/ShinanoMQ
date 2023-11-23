package cn.com.shinano.nameserver;


import java.sql.Time;
import java.util.concurrent.TimeUnit;

/**
 * @author lhe.shinano
 * @date 2023/11/23
 */
public class ServerStart {

    public static void main(String[] args) {
        try {
            new ServerStart().init();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
    public void init() throws InterruptedException {
        NameServerService nameServerService1 = new NameServerService("server1", "localhost", 10001, new String[]{"localhost:10002", "localhost:10003"});
        TimeUnit.SECONDS.sleep(1);
        NameServerService nameServerService2 = new NameServerService("server2", "localhost", 10002, new String[]{"localhost:10001", "localhost:10003"});
        TimeUnit.SECONDS.sleep(1);
        NameServerService nameServerService3 = new NameServerService("server3", "localhost", 10003, new String[]{"localhost:10002", "localhost:10001"});

    /*    new Thread(()->{
            NameServerService nameServerService1 = new NameServerService("server1", "localhost", 10001, new String[]{"localhost:10002", "localhost:10003"});

        }).start();
        new Thread(()->{
            NameServerService nameServerService2 = new NameServerService("server2", "localhost", 10002, new String[]{"localhost:10001", "localhost:10003"});

        }).start();
        new Thread(()->{
            NameServerService nameServerService3 = new NameServerService("server3", "localhost", 10003, new String[]{"localhost:10002", "localhost:10001"});

        }).start();*/

        try {
            Thread.sleep(199999);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
