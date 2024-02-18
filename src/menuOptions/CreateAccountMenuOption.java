package menuOptions;

import java.sql.*;
import java.util.*;

import dataClasses.*;
import dataClasses.Driver;

public class CreateAccountMenuOption extends MenuOption {

  public CreateAccountMenuOption(Connection conn, Scanner in) {
    super(conn, in);
  }

  @Override
  public String getMenuText() {
    return "Create a new account";
  }

  @Override
  public void execute() throws SQLException {
    Account account = getAccountDetails();
    Passenger passenger = null;
    Driver driver = null;
    if (account.isPassenger()) {
      passenger = getPassengerDetails();
    }
    if (account.isDriver()) {
      driver = getDriverDetails();
    }

    this.databaseMethods.createAccount(account, passenger, driver);

    System.out.println("\nAccount successfully added!\n");
  }

  private Account getAccountDetails() {
    System.out.println("Please provide the following account details:");
    System.out.print("First name: ");
    String firstName = scanner.nextLine();

    System.out.print("Last name: ");
    String lastName = scanner.nextLine();

    System.out.print("Birthdate: ");
    String birthdate = scanner.nextLine();

    System.out.print("Street: ");
    String street = scanner.nextLine();

    System.out.print("City: ");
    String city = scanner.nextLine();

    System.out.print("Province: ");
    String province = scanner.nextLine();

    System.out.print("Postal code: ");
    String postalCode = scanner.nextLine();

    System.out.print("Phone number: ");
    String phoneNumber = scanner.nextLine();

    System.out.print("Email address: ");
    String emailAddress = scanner.nextLine();

    System.out.print("Will this account be used by a passenger (P), driver (D), or both (B)? ");
    String accountTypeInput = scanner.nextLine();

    boolean isPassenger = false;
    boolean isDriver = false;

    if (accountTypeInput.equalsIgnoreCase("P")) {
      isPassenger = true;
    } else if (accountTypeInput.equalsIgnoreCase("D")) {
      isDriver = true;
    } else if (accountTypeInput.equalsIgnoreCase("B")) {
      isPassenger = true;
      isDriver = true;
    }

    return new Account(firstName, lastName, street, city, province, postalCode,
        phoneNumber, emailAddress, birthdate, isPassenger, isDriver);
  }

  private Passenger getPassengerDetails() {
    System.out.println("Please enter the following passenger details:");

    System.out.print("Credit card number: ");
    String creditCardNumber = scanner.nextLine();

    return new Passenger(creditCardNumber);
  }

  private Driver getDriverDetails() {
    System.out.println("Please enter the following driver details:");

    System.out.print("License number: ");
    String licenseNumber = scanner.nextLine();

    System.out.print("License expiry date: ");
    String expiryDate = scanner.nextLine();

    return new Driver(licenseNumber, expiryDate);
  }
}
