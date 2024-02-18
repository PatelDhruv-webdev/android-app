package menuOptions;

import java.sql.*;
import java.util.*;

import dataClasses.*;

public class CompleteRideMenuOption extends MenuOption {

  public CompleteRideMenuOption(Connection conn, Scanner in) {
    super(conn, in);
  }

  @Override
  public String getMenuText() {
    return "Complete a ride";
  }

  @Override
  public void execute() throws SQLException {
    ArrayList<RideRequest> uncompletedRideRequests = this.databaseMethods.getUncompletedRideRequests();

    printUncompletedRideRequests(uncompletedRideRequests);

    System.out.print("Please enter the ID of the ride request you want to complete: ");
    boolean valid = false;
    int rideRequestId = -1;
    while (!valid) {
      int selectedId = this.scanner.nextInt();
      for (RideRequest request : uncompletedRideRequests) {
        if (request.getId() == selectedId) {
          rideRequestId = selectedId;
          valid = true;
          break;
        }
      }
      if (!valid) {
        System.out.print("Please enter a valid ID: ");
      }
    }
    this.scanner.nextLine(); // flush buffer

    System.out.println("Please enter the following information to complete the ride:");
    String driverEmail = getDriverEmail("Driver email: ");
    System.out.print("Actual start date: ");
    String startDate = this.scanner.nextLine();
    System.out.print("Actual start time: ");
    String startTime = this.scanner.nextLine();
    System.out.print("End date: ");
    String endDate = this.scanner.nextLine();
    System.out.print("End time: ");
    String endTime = this.scanner.nextLine();
    System.out.print("Distance travelled (km): ");
    double distance = this.scanner.nextDouble();
    this.scanner.nextLine();
    System.out.print("Charge: $");
    double charge = this.scanner.nextDouble();
    this.scanner.nextLine();
    System.out.print("Driver's rating: ");
    int ratingFromDriver = this.scanner.nextInt();
    this.scanner.nextLine();
    System.out.print("Passenger's rating: ");
    int ratingFromPassenger = this.scanner.nextInt();
    this.scanner.nextLine();

    Ride ride = new Ride(rideRequestId, driverEmail, startDate, startTime, endDate, endTime, distance, charge,
        ratingFromDriver,
        ratingFromPassenger);

    this.databaseMethods.insertRide(ride);

    System.out.println("\nRide has been completed!\n");
  }

  private void printUncompletedRideRequests(ArrayList<RideRequest> rideRequests) {
    List<String> headers = Arrays.asList("ID", "Passenger First Name", "Passenger Last Name", "Pickup Street",
        "Pickup City", "Dropoff Street", "Dropoff City", "Desired Pickup Date", "Desired Pickup Time");
    List<List<String>> data = new ArrayList<>();

    for (RideRequest rideRequest : rideRequests) {
      List<String> rowData = Arrays.asList(
          String.valueOf(rideRequest.getId()),
          rideRequest.getPassengerFirstName(),
          rideRequest.getPassengerLastName(),
          rideRequest.getPickupStreet(),
          rideRequest.getPickupCity(),
          rideRequest.getDropoffStreet(),
          rideRequest.getDropOffCity(),
          rideRequest.getDesiredPickupDate(),
          rideRequest.getDesiredPickupTime());
      data.add(rowData);
    }

    PrintableTable.printTable("Uncompleted Ride Requests", headers, data);
  }

}
