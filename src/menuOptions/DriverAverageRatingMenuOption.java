package menuOptions;

import java.sql.*;
import java.util.*;

public class DriverAverageRatingMenuOption extends MenuOption {

  public DriverAverageRatingMenuOption(Connection conn, Scanner in) {
    super(conn, in);
  }

  @Override
  public String getMenuText() {
    return "Calculate the average rating for a specific driver";
  }

  @Override
  public void execute() throws SQLException {
    String driverEmail = getDriverEmail("Enter the email of the driver: ");
    double averageRating = this.databaseMethods.getAverageRatingForDriver(driverEmail);

    PrintableTable.printTable("Average Driver Rating", Arrays.asList("Driver Email", "Average Rating"),
        Arrays.asList(Arrays.asList(driverEmail, String.format("%.2f", averageRating))));
  }
}
