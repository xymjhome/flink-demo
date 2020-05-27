package streamapi.window.time;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.junit.Test;

public class EventTimeSlidingWindowsTest {
    private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

    @Test
    public void testSlidingWindow() {

        long timestamp = 1590732662000L;//[2020-05-29 14:11:02.000]

        long size = Time.seconds(10).toMilliseconds();
        long slide = Time.seconds(3).toMilliseconds();
        long offset = 0;
        List<TimeWindow> windows = new ArrayList<>((int) (size / slide));
        long lastStart = TimeWindow.getWindowStartWithOffset(timestamp, offset, slide);
        for (long start = lastStart; start > timestamp - size; start -= slide) {
            windows.add(new TimeWindow(start, start + size));
        }
        System.out.println(windows.toString());
        windows.forEach(window -> {
            System.out.println(sdf.format(window.getStart()) + "<-->" + sdf.format(window.getEnd()));
        });
    }

}
