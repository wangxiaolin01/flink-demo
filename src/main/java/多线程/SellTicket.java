package 多线程;

import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;

public class SellTicket implements Runnable{
     static int numTickts = 10;
    @Override
    public void run() {
            while (numTickts>0){
                try {
                    sell();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }


    public  void sell() throws InterruptedException {
        synchronized (SellTicket.class) {
            System.out.println(Thread.currentThread() + "正在卖第" + numTickts + "zhangpiao");
            if (numTickts%2==0)  Thread.currentThread().yield() ;
            numTickts--;
            Thread.sleep(0);
        }
    }

}

