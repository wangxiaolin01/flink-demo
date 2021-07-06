package demo;

public class Test {
    public static void main(String[] args) {
        String s = "leetcode";
        System.out.println(isUnique(s));
    }

    public static boolean isUnique(String s){
        boolean flag= true;
        for (int i =0;i<s.length()-1;i++){
            for (int j =i+1;j<s.length();j++){
                if (s.charAt(i) == s.charAt(j)){
                    flag =  false;
                }
            }
        }
        return flag;
    }
}
