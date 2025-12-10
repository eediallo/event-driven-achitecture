package org.example;

public class Main {
    public static void main(String[] args) {
        String mode = System.getenv("APP_MODE");

        if (mode == null || mode.isEmpty()) {
            System.err.println("Error: APP_MODE environment variable not set (use 'producer' or 'consumer')");
            System.exit(1);
        }

        System.out.println("Starting application in mode: " + mode);

        switch (mode.toLowerCase()) {
            case "producer":
                DistributedScheduler.main(args);
                break;
            case "consumer":
                JobConsumer.main(args);
                break;
            default:
                System.err.println("Unknown mode: " + mode);
                System.exit(1);
        }
    }
}