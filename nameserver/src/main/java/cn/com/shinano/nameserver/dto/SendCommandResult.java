package cn.com.shinano.nameserver.dto;


/**
 * @author lhe.shinano
 * @date 2023/11/24
 */
public class SendCommandResult {
    private volatile boolean isDone = false;
    private Object res = null;

    public synchronized boolean setResult(Object res) {
        if(isDone) {
            return false;
        }
        this.res = res;
        this.notifyAll();
        return isDone = true;
    }

    public synchronized Object getResult() throws InterruptedException {
        while (res == null) {
            this.wait();
        }
        return res;
    }
}
