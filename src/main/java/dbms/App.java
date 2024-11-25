package dbms;

import java.util.Scanner;

public class App {
    public static void main(String[] args) {
        System.out.println("Welcome to CobaltDB! Type your commands or 'exit' to quit.");
        Scanner scanner = new Scanner(System.in);

        boolean continueRunning = true;
        while (continueRunning) {
            System.out.print("dbms> ");
            String input = scanner.nextLine().trim();
            String[] arguments = input.split("\\s+");

            if (arguments.length == 0) {
                continue;
            }

            switch (arguments[0].toLowerCase()) {
                case "exit":
                case "quit":
                    continueRunning = false;
                    System.out.println("Exiting CobaltDB. Goodbye!");
                    break;

                case ".file":
                    if (arguments.length > 1) {
                        FileStorage storage = new FileStorage(arguments[1], null);
                        storage.startCSVProcess();
                    } else {
                        System.out.println("Error: Filename required");
                    }
                    break;

                case "create":
                    if (arguments.length > 2) {
                        switch (arguments[1].toLowerCase()) {
                            case "table":
                                try {
                                    FileStorage storage = new FileStorage(arguments[2], null);
                                    storage.startCSVProcess();
                                } catch (Exception e) {
                                    System.out.println("Error: " + e.getMessage());
                                }
                                break;
                            case "index":
                                // TODO: Implement index creation
                                break;
                            default:
                                System.out.println("Error: Invalid create command");
                        }
                    } else {
                        System.out.println("Error: Invalid create command syntax");
                    }
                    break;

                case "show":
                    // TODO: Implement show command
                    break;

                case "insert":
                    // TODO: Implement insert command
                    break;

                case "delete":
                    // TODO: Implement delete command
                    break;

                case "select":
                    // TODO: Implement select command
                    break;

                case "update":
                    // TODO: Implement update command
                    break;

                default:
                    System.out.println("Error: Unknown command");
            }
        }

        scanner.close();
    }
}