package node.service.timer;

import java.util.Calendar;
import java.util.Date;

import node.service.ControlService;

public class GregorianTimer implements ControlService {
    public static final String SERVICE_ID = "gregorian";

    private static final long MIN_PERIOD = 60 * 1000;

    @Override
    public String getId() {
        return SERVICE_ID;
    }

    @Override
    public String execute(long currentTs, String pipelineNo, String params) {
        // 1986/07/03 00:00:00|60000
        System.out.println(new Date(currentTs) + "/GregorianTimer/" + pipelineNo + "/" + params);

        String[] paramss = params.split("\\|");

        String[] ss = paramss[0].split(" ");
        String[] dates = ss[0].split("/");
        String[] timess = ss[1].split(":");
        Integer year = Integer.parseInt(dates[0]);
        Integer month = Integer.parseInt(dates[1]) - 1;
        Integer day = Integer.parseInt(dates[2]);

        Integer hour = Integer.parseInt(timess[0]);
        Integer minute = Integer.parseInt(timess[1]);

        try {
            Calendar calendar = Calendar.getInstance();
            calendar.set(year, month, day, hour, minute);

            Long period = Long.parseLong(paramss[1]);
            dealLongPeriod(currentTs, pipelineNo, calendar, period);
            return null;
        } catch (NumberFormatException e) {
            // e.printStackTrace();
        }

        Calendar now = Calendar.getInstance();
        int nowYear = now.get(Calendar.YEAR);
        int nowMonth = now.get(Calendar.MONTH);
        int nowDay = now.get(Calendar.DATE);

        Calendar nextCalendar = Calendar.getInstance();
        long nextTs = 0;
        switch (paramss[1]) {
        case "day":
            nextCalendar.set(nowYear, nowMonth, nowDay, hour, minute);
            if (nextCalendar.getTime().getTime() <= System.currentTimeMillis()) {
                nowDay++;
            }
            nextCalendar.set(nowYear, nowMonth, nowDay, hour, minute);
            break;
        case "week":
            nextCalendar.set(nowYear, nowMonth, nowDay, hour, minute);
            if (nextCalendar.getTime().getTime() <= System.currentTimeMillis()) {
                nowDay += 7;
            }
            nextCalendar.set(nowYear, nowMonth, nowDay, hour, minute);
            break;
        case "month":
            nextCalendar.set(nowYear, nowMonth, day, hour, minute);
            if (nextCalendar.getTime().getTime() <= System.currentTimeMillis()) {
                nowMonth++;
            }
            nextCalendar.set(nowYear, nowMonth, day, hour, minute);
            break;
        case "year":
            nextCalendar.set(nowYear, month, day, hour, minute);
            if (nextCalendar.getTime().getTime() <= System.currentTimeMillis()) {
                nowYear++;
            }
            nextCalendar.set(nowYear, month, day, hour, minute);
            break;
        default:
            return null;
        }
        nextTs = nextCalendar.getTime().getTime();
        TimerExecuter.getInstance().addNextTs(pipelineNo, nextTs);

        return null;
    }

    private void dealLongPeriod(long currentTs, String pipelineNo, Calendar calendar, long period) {

        long nextTs = calendar.getTime().getTime();
        nextTs = nextTs / MIN_PERIOD * MIN_PERIOD;

        if (nextTs < currentTs && period > 0) {
            period = period < MIN_PERIOD ? MIN_PERIOD : period;
            long sub = currentTs - nextTs + period;
            long times = sub / period;
            nextTs += times * period;
        }

        if (period < MIN_PERIOD && System.currentTimeMillis() > nextTs) {
            return;
        }
        TimerExecuter.getInstance().addNextTs(pipelineNo, nextTs);
    }

}
