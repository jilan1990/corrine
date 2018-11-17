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

        Calendar calendar = Calendar.getInstance();
        calendar.set(year, month, day, hour, minute);
        long nextTs = calendar.getTime().getTime() - 60 * 1000;
        nextTs = nextTs / MIN_PERIOD * MIN_PERIOD;

        long period = Long.parseLong(paramss[1]);

        if (nextTs < currentTs && period > 0) {
            period = period < MIN_PERIOD ? MIN_PERIOD : period;
            long sub = currentTs - nextTs + period;
            long times = sub / period;
            nextTs += times * period;
        }
        TimerExecuter.getInstance().addNextTs(pipelineNo, nextTs);
        return null;
    }

}
