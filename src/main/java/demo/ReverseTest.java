package demo;

public class ReverseTest {
    public static void main(String[] args) {
        System.out.println(reverse(1234567899));
//        System.out.println(Integer.MIN_VALUE);
//        System.out.println(-214384741*10);
    }

    public static int reverse(int x) {
        int rs = 0;
        int num = x;
        while (num!=0){
            int a = num%10;
            if(rs > Integer.MAX_VALUE/10 || rs == Integer.MAX_VALUE/10 && a > 7){
                return 0;
            }
            if(rs < Integer.MIN_VALUE/10 || rs == Integer.MIN_VALUE && a < -8){
                return 0;
            }
            rs = rs*10+a;
            num = num/10;
        }
        return rs;
    }

//    public static int reverse2(int x) {
//        int cur = 0, pre = 0;
//        while (x != 0) {
//            pre = cur;
//            cur = cur * 10 + x % 10;
//            if (cur / 10 != pre) {
//                return 0;
//            }
//            x /= 10;
//        }
//        return cur;
//    }
}
