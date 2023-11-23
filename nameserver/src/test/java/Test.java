import cn.com.shinano.nameserver.NameServerService;

import java.util.concurrent.TimeUnit;

/**
 * @author lhe.shinano
 * @date 2023/11/23
 */
public class Test {

    @org.junit.Test
    public void testStart1() throws InterruptedException {
        NameServerService nameServerService1 = new NameServerService("server1", "localhost",
                10001, new String[]{"server2@localhost:10002", "server3@localhost:10003"});

        while (true) {
            System.out.println(nameServerService1.getMaster());
            System.out.println(nameServerService1.getClusterConnectMap());
            TimeUnit.SECONDS.sleep(10);
        }

    }
    @org.junit.Test
    public void testStart2() throws InterruptedException {
        NameServerService nameServerService2 = new NameServerService("server2", "localhost",
                10002, new String[]{"server1@localhost:10001", "server3@localhost:10003"});
        while (true) {
            System.out.println(nameServerService2.getMaster());
            System.out.println(nameServerService2.getClusterConnectMap());
            TimeUnit.SECONDS.sleep(10);
        }
    }
    @org.junit.Test
    public void testStart3() throws InterruptedException {
        NameServerService nameServerService3 = new NameServerService("server3", "localhost",
                10003, new String[]{"server2@localhost:10002", "server1@localhost:10001"});
        while (true) {
            System.out.println(nameServerService3.getMaster());
            System.out.println(nameServerService3.getClusterConnectMap());
            TimeUnit.SECONDS.sleep(10);
        }
    }
}
