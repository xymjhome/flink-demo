package com.analysis.project.adclick;


import java.text.SimpleDateFormat;
import org.junit.Test;

public class AdStatisticsByGeoWithBlacklistTest {

    @Test
    public void testMidnightTime() {
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        long currentTimeMillis = System.currentTimeMillis();
        System.out.println("currentTimeMillis:" + currentTimeMillis + " | " + dateFormat.format(currentTimeMillis));

        long zeroPoint = (currentTimeMillis / (24 * 60 * 60 * 1000) + 1) * (24 * 60 * 60 * 1000) + (currentTimeMillis % (24 * 60 * 60 * 1000));
        System.out.println("        zeroPoint:" + zeroPoint + " | " + dateFormat.format(zeroPoint));
    }
}
