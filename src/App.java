import java.sql.*;
import java.util.ArrayList;
import java.util.Scanner;
import menuOptions.*;

public class App {
  public static void main(String[] args) throws Exception {
    String url = "jdbc:sqlite:src/yoober.db";
    try (Connection conn = DriverManager.getConnection(url); Scanner in = new Scanner(System.in)) {
      ArrayList<MenuOption> menuOptions = new ArrayList<MenuOption>();
      menuOptions.add(new AllAccountsMenuOption(conn, in));
      menuOptions.add(new DriverAverageRatingMenuOption(conn, in));
      menuOptions.add(new CreateAccountMenuOption(conn, in));
      menuOptions.add(new SubmitRideRequestMenuOption(conn, in));
      menuOptions.add(new CompleteRideMenuOption(conn, in));

      System.out.println("Welcome to Yoober!\n");

      while (true) {
        printMenu(menuOptions);
        MenuOption selectedOption = getSelectedOption(menuOptions, in);
        System.out.println();

        if (selectedOption == null) {
          System.out.println("Exiting... Goodbye");
          break;
        }
        try {
          selectedOption.execute();
        } catch (SQLException e) {
          System.out.println("ERROR OCCURRED RUNNING [" + selectedOption.getMenuText() + "]");
          System.out.println(e.getMessage());
          e.printStackTrace();
          System.out.println();
        }

        System.out.print("Would you like to select another option? (Y/N): ");
        String choice = in.nextLine();
        if (choice.toUpperCase().charAt(0) != 'Y') {
          System.out.println("Goodbye!");
          break;
        }
        System.out.println();
      }
    }
  }

  private static MenuOption getSelectedOption(ArrayList<MenuOption> options, Scanner in) {
    System.out.print("Please enter your choice (or type exit to quit): ");
    while (true) {
      String choice = in.nextLine();

      if (choice.toUpperCase().equals("EXIT")) {
        return null;
      }

      int position;
      try {
        position = Integer.parseInt(choice);
        if (position < 1 || position > options.size()) {
          System.out.print("Invalid option, please enter a number between 1 and " + options.size() + ": ");
          continue;
        }
        return options.get(position - 1);
      } catch (Exception e) {
        System.out.print("Invalid option, please enter a number between 1 and " + options.size() + ": ");
        continue;
      }
    }
  }

  private static void printMenu(ArrayList<MenuOption> options) {
    int count = 1;
    System.out.println("MENU\n----");
    for (MenuOption opt : options) {
      System.out.println(count++ + ": " + opt.getMenuText() + ".");
    }
  }
}
