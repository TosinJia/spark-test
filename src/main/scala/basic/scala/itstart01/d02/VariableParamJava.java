package basic.scala.itstart01.d02;

public class VariableParamJava {
    //java中可变参数
    public static int sum(int... ints){
        int sum = 0;
        for (int i: ints) {
            sum += i;
        }
        return sum;
    }

    public static void main(String[] args){
        System.out.println((sum(1,2)));
        System.out.println((sum(1,2,3,4,5)));
    }
}
