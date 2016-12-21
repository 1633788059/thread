package test;

import java.util.Date;
import java.util.Timer;
import java.util.TimerTask;

import com.wangjubao.dolphin.common.util.date.DateUtils;

/**
 * @author ckex created 2013-7-26 - 下午4:03:06 Test.java
 * @explain - test
 */
public class Test {

    public static void main(String[] args) {
        new Thread() {
            @Override
            public void run() {
                Timer timer = new Timer();
                TimerTask task = new TimerTask() {
                    public void run() {
                        execute();
                    }
                };
                timer.scheduleAtFixedRate(task, new Date(), 1000);
            }
        }.start();
    }

    private static void execute() {
        try {
            Thread.sleep(10 * 1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println(Thread.currentThread().getName() + " time : "
                + DateUtils.formatDate(DateUtils.now(), "yyyy-MM-dd HH:mm:ss:sss"));
    }

}
