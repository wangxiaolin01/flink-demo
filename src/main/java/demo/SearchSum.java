package demo;

import java.util.Arrays;

public class SearchSum {
    public static void main(String[] args) {
        int[] sourceArr = {20, 70, 110, 150};
        int sum = 90;
        int[] sum1 = twoSum(sourceArr, sum);
        for (int i : sum1) {
            System.out.println(i);
        }
    }


    public static int[] twoSum(int[] num,int target){
        int ii = -1;
        int jj = -1;
        int[] resutlArr = new int[2];
         for (int i = 0;i< num.length-1;i++){
             for (int j =i;j<num.length;j++){
                 if (num[i]>target) continue;
                 if ((num[i] + num[j]) == target){
                    resutlArr[0] = i;
                    resutlArr[1] = j;
                 }
             }
          }
         return resutlArr;

    }
}
