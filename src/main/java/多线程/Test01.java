package 多线程;

public class Test01 {
    public static void main(String[] args) {
        Thread thread = new Thread(new SellTicket());
        Thread thread1 = new Thread(new SellTicket());

        thread.setPriority(Thread.MAX_PRIORITY);
        thread.start();
        thread1.start();
    }
}
