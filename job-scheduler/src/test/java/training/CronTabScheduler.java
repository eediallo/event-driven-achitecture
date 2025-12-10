package training;

import org.quartz.*;
import org.quartz.impl.StdSchedulerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

public class CronTabScheduler {

    // keeps track of the job number, starting at 1.
    private final AtomicInteger jobCounter = new AtomicInteger(1);

    public void scheduleTab(String cronTabs) throws SchedulerException, IOException {
        // Set up the Quartz scheduler instance
        Scheduler scheduler = StdSchedulerFactory.getDefaultScheduler();
        scheduler.start();
//
        // Path to the crontab file
        Path crontabFile = Path.of(cronTabs);

        if (!Files.exists(crontabFile)) {
            System.out.println("Cron tab file not found. Creating default file");
            Files.writeString(crontabFile, "* * * * *\n15 * * * *");
        } else {
            System.out.println("Cron tab file exist. Reading existing content");
        }

        // Read the file and schedule jobs
        try (Stream<String> lines = Files.lines(crontabFile)) {

            lines.filter(line -> !line.trim().isEmpty()).filter(line -> !line.trim().startsWith("#")).forEach(line -> {
                // getAndIncrement atomically returns the current value and then increments it.
                int lineNumber = jobCounter.getAndIncrement();
                String jobId = String.valueOf(lineNumber);

                // Create a JobDetail for the job
                JobDetail job = JobBuilder.newJob(CronTabJob.class).withIdentity("job-" + jobId).usingJobData("jobId", jobId).build();

                // Build a CronTrigger using the expression from the file
                Trigger trigger = TriggerBuilder.newTrigger().withIdentity("trigger-" + jobId).withSchedule(CronScheduleBuilder.cronSchedule(convertToQuartzCron(line))).build();

                // Schedule the job with the trigger
                try {
                    scheduler.scheduleJob(job, trigger);
                } catch (SchedulerException e) {
                    System.err.println("Failed to schedule job " + jobId + ": " + e.getMessage());
                }
            });
        }

        // Keep the application running for a specified duration
        try {
            Thread.sleep(300 * 1000 * 4); // 20 minutes
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        // Shut down the scheduler
        scheduler.shutdown();
    }

    private static String convertToQuartzCron(String cronTabLine) {
        String trimmedLine = cronTabLine.trim();
        String[] parts = trimmedLine.split("\\s+");

        if (parts.length != 5) {
            // Not a standard 5-part cron, return as is (or throw an error)
            return cronTabLine;
        }

        // Standard Crontab fields: [0]Min [1]Hour [2]Day-of-Month [3]Month [4]Day-of-Week
        String minutes = parts[0];
        String hours = parts[1];
        String dayOfMonth = parts[2];
        String month = parts[3];
        String dayOfWeek = parts[4];

        // Quartz 6 fields: [0]S [1]M [2]H [3]D [4]M [5]W
        String seconds = "0"; // Add mandatory seconds field

        // Handle the mutually exclusive Day-of-Month and Day-of-Week fields
        if (!dayOfMonth.equals("*") && !dayOfWeek.equals("*")) {
            // If a specific value is set for both, it's ambiguous.
            // For simplicity, we prioritize Day-of-Week and set Day-of-Month to '?'
            dayOfMonth = "?";
        } else if (dayOfMonth.equals("*") && !dayOfWeek.equals("*")) {
            // Specific Day-of-Week is defined, so set Day-of-Month to '?'
            dayOfMonth = "?";
        } else if (!dayOfMonth.equals("*") && dayOfWeek.equals("*")) {
            // Specific Day-of-Month is defined, so set Day-of-Week to '?'
            dayOfWeek = "?";
        } else {
            // If both are '*', set Day-of-Month to '?' for reliable scheduling
            // as Quartz requires one to be non-wildcard or '?'
            dayOfMonth = "?";
        }

        // Reconstruct the 6-part Quartz expression
        return String.format("%s %s %s %s %s %s", seconds, minutes, hours, dayOfMonth, month, dayOfWeek);
    }

}