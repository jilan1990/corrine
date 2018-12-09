package node.service.timer;

import java.util.Calendar;
import java.util.Date;

import node.service.ControlService;

public class GregorianTimer implements ControlService {

    private static final long MIN_PERIOD = 60 * 1000;

    @Override
    public String getId() {
        return "gregorian";
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

        long nextTs = 0;
        switch (paramss[1]) {
        case "month":
            Calendar next0 = Calendar.getInstance();
            next0.set(nowYear, nowMonth, day, hour, minute);
            if (next0.getTime().getTime() <= System.currentTimeMillis()) {
                nowMonth++;
                nowYear += nowMonth / 12;
                nowMonth = nowMonth % 12;
            }
            next0.set(nowYear, nowMonth, day, hour, minute);
            nextTs = next0.getTime().getTime();
            break;
        case "year":
            Calendar next1 = Calendar.getInstance();
            next1.set(nowYear, month, day, hour, minute);
            if (next1.getTime().getTime() <= System.currentTimeMillis()) {
                nowYear++;
            }
            next1.set(nowYear, month, day, hour, minute);
            nextTs = next1.getTime().getTime();
            break;
        default:
            break;
        }
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
        TimerExecuter.getInstance().addNextTs(pipelineNo, nextTs);
    }

}
