package org.example;


import org.apache.hadoop.shaded.org.checkerframework.checker.units.qual.C;

class Vehicle {

    public void msg(){
        System.out.println("hello vehicle");
    }
    public void Driver() {

    }
}

class Car extends Vehicle {

    public void msg() {
        System.out.println("Hello car");
    }
    public void Driver() {
        System.out.println("Hello Driver");
    }
}
public class Example {

    public static void main(String[] args) {
        Vehicle v1 = new Car();
        v1.msg();
        v1.Driver();
        Car c1 = new Car();
        c1.msg();
        c1.Driver();


    }
}
