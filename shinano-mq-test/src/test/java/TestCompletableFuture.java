import cn.com.shinano.ShinanoMQ.base.dto.SendCommandFuture;
import org.junit.jupiter.api.Test;

import java.util.concurrent.*;

/**
 * @author lhe.shinano
 * @date 2023/12/4
 */
public class TestCompletableFuture {

    @Test
    public void test1() throws InterruptedException {
        ExecutorService executors1 = Executors.newFixedThreadPool(10);
        ExecutorService executors2 = Executors.newFixedThreadPool(10);

        for (int i = 0; i < 1000; i++) {
            int finalI = i;
            CompletableFuture<Integer> future = CompletableFuture.supplyAsync(() -> {
                System.out.println("task 1 run - " + finalI);
                try {
                    TimeUnit.SECONDS.sleep(1);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                return finalI;
            }, executors1);

            future.whenCompleteAsync((integer,t) -> {
                System.out.println("task 2 fun - " + finalI);
                try {
                    TimeUnit.SECONDS.sleep(2);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }, executors2);
        }

        TimeUnit.SECONDS.sleep(500);
    }
    @Test
    public void test2() throws InterruptedException {
        ExecutorService executors1 = Executors.newFixedThreadPool(10);
        ExecutorService executors2 = Executors.newFixedThreadPool(10);

        for (int i = 0; i < 1000; i++) {
            int finalI = i;
            SendCommandFuture future = new SendCommandFuture();
            executors1.execute(() -> {
                System.out.println("task 1 run - " + finalI);
                try {
                    TimeUnit.SECONDS.sleep(1);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                future.setResult(finalI);
            });

            executors2.execute(()->{
                try {
                    try {
                        TimeUnit.SECONDS.sleep(1);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    Object result = future.getResult();
                    System.out.println("task 2 fun - " + result);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            });
        }
        TimeUnit.SECONDS.sleep(500);
    }

}
