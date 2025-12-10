package training;

import org.quartz.SchedulerException;

import java.io.IOException;

public class Main {
    static void main(String[] args)  throws SchedulerException, IOException {
        CronTabScheduler cronTabScheduler = new CronTabScheduler();
        cronTabScheduler.scheduleTab("/app/jobs.txt");
    }
}
