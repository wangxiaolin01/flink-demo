package javademo;

public class Circle {
    double radius = 0;

    public Circle(double radius) {
        this.radius = radius;
    }

    class Draw{
        public  void drawSahpe(){
            System.out.println("drawSahpe");
            System.out.println(radius);
        }
    }

    public static void main(String[] args) {
        Circle circle = new Circle(3);
        System.out.println(circle.radius);
    }
}
