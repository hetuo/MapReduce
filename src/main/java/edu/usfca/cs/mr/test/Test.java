package edu.usfca.cs.mr.test;


import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by tuo on 06/11/17.
 */
public class Test {

    public static void main(String[] args) {

        SimpleDateFormat t = new SimpleDateFormat("MM");
        long s = Long.parseLong("1426442400000");
        String str = t.format(s);
        System.out.println(str);
    }
}
