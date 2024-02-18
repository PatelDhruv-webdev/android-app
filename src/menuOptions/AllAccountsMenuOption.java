package menuOptions;

import java.sql.*;
import java.util.*;

import dataClasses.Account;

public class AllAccountsMenuOption extends MenuOption {

  public AllAccountsMenuOption(Connection conn, Scanner in) {
    super(conn, in);
  }

  @Override
  public String getMenuText() {
    return "View all account details";
  }

  @Override
  public void execute() throws SQLException {
    List<Account> accounts = this.databaseMethods.getAllAccounts();

    printAccounts(accounts);
  }

  private void printAccounts(List<Account> accounts) {
    List<String> headers = Arrays.asList("First Name", "Last Name", "Street", "City", "Province", "Postal Code",
        "Phone Number", "Email", "Is Passenger?", "Is Driver?");
    List<List<String>> data = new ArrayList<>();

    for (Account account : accounts) {
      List<String> rowData = Arrays.asList(
          account.getFirstName(),
          account.getLastName(),
          account.getStreet(),
          account.getCity(),
          account.getProvince(),
          account.getPostalCode(),
          account.getPhoneNumber(),
          account.getEmail(),
          account.isPassenger() ? "Yes" : "No",
          account.isDriver() ? "Yes" : "No");
      data.add(rowData);
    }

    PrintableTable.printTable("All Accounts", headers, data);
  }

}
