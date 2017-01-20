package com.miked;

public class ClusterUtils {
    //Enable this to see messages being exchanged
    public static boolean debug = false;

    public static void sleepLessThan(int max) {
        try {
            Thread.sleep((int) (Math.random() * max));
        } catch (InterruptedException e) {
            System.out.println(e);
        }
    }
}
