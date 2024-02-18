package menuOptions;

import java.sql.*;
import java.util.*;

import database.DatabaseMethods;

public abstract class MenuOption {
  protected DatabaseMethods databaseMethods;
  protected Scanner scanner;

  public MenuOption(Connection conn, Scanner in) {
    this.databaseMethods = new DatabaseMethods(conn);
    this.scanner = in;
  }

  public abstract String getMenuText();

  public abstract void execute() throws SQLException;

  protected String getPassengerEmail(String prompt) throws SQLException {
    String passengerEmail = "";

    boolean passengerExists = false;

    do {
      System.out.print(prompt);
      passengerEmail = this.scanner.nextLine();
      passengerExists = this.databaseMethods.checkPassengerExists(passengerEmail);
      if (!passengerExists) {
        System.out.println("\n** Passenger with email " + passengerEmail + " doesn't exist. **");
      }
    } while (!passengerExists);

    return passengerEmail;
  }

  protected String getDriverEmail(String prompt) throws SQLException {
    String driverEmail = "";

    boolean driverExists = false;

    do {
      System.out.print(prompt);
      driverEmail = this.scanner.nextLine();
      driverExists = this.databaseMethods.checkDriverExists(driverEmail);
      if (!driverExists) {
        System.out.println("\n** Driver with email " + driverEmail + " doesn't exist. **");
      }
    } while (!driverExists);

    return driverEmail;
  }

}
