package menuOptions;

import java.sql.*;
import java.util.*;

import dataClasses.Address;
import dataClasses.FavouriteDestination;

public class SubmitRideRequestMenuOption extends MenuOption {

  public SubmitRideRequestMenuOption(Connection conn, Scanner in) {
    super(conn, in);
  }

  @Override
  public String getMenuText() {
    return "Submit a ride request";
  }

  @Override
  public void execute() throws SQLException {
    String passengerEmail = getPassengerEmail("Enter the email of the passenger making the request: ");
    int addressId = getDropOffLocationId(passengerEmail);

    System.out.println("When do you want to be picked up?");
    System.out.print("Date: ");
    String date = this.scanner.nextLine();
    System.out.print("Time: ");
    String time = this.scanner.nextLine();

    System.out.print("How many passengers (including yourself) will be taking this ride? ");
    int numberOfPassengers = this.scanner.nextInt();
    this.scanner.nextLine(); // flush buffer

    this.databaseMethods.insertRideRequest(passengerEmail, addressId, date, time, numberOfPassengers);

    System.out.println("\nRide request submitted!\n");
  }

  private int getDropOffLocationId(String passengerEmail) throws SQLException {
    System.out.print("Do you want to choose from your favourite destinations? (Y/N): ");
    String response = this.scanner.nextLine();
    int addressId = -1;
    if (response.toUpperCase().charAt(0) == 'Y') {
      ArrayList<FavouriteDestination> favouriteDestinations = this.databaseMethods
          .getFavouriteDestinationsForPassenger(passengerEmail);

      System.out.println();

      if (favouriteDestinations.size() == 0) {
        System.out.println("** You have no favourite destinations! **");
        addressId = handleNewAddress(passengerEmail);
      } else {
        printFavouriteDestinations(favouriteDestinations);
        System.out.print("Please enter the name of your selected destination: ");
        boolean valid = false;
        while (!valid) {
          String name = scanner.nextLine();
          for (FavouriteDestination fav : favouriteDestinations) {
            if (fav.getName().equalsIgnoreCase(name)) {
              addressId = fav.getAddressId();
              valid = true;
              break;
            }
          }
          if (!valid) {
            System.out.print("Please enter a valid name: ");
          }
        }
      }
    } else {
      addressId = handleNewAddress(passengerEmail);
    }
    return addressId;
  }

  private Address getAddressDetails() {
    System.out.println("\nPlease enter the following details of your destination address:");
    System.out.print("Street: ");
    String street = this.scanner.nextLine();
    System.out.print("City: ");
    String city = this.scanner.nextLine();
    System.out.print("Province: ");
    String province = this.scanner.nextLine();
    System.out.print("Postal Code: ");
    String postalCode = this.scanner.nextLine();

    return new Address(street, city, province, postalCode);
  }

  private int handleNewAddress(String email) throws SQLException {
    Address address = getAddressDetails();
    int addressId = this.databaseMethods.insertAddressIfNotExists(address);

    System.out.print("Do you want to add this address as a new favourite? ");
    String response = this.scanner.nextLine();
    if (response.toUpperCase().charAt(0) == 'Y') {
      System.out.print("Give this destination a name: ");
      String name = this.scanner.nextLine();
      this.databaseMethods.insertFavouriteDestination(name, email, addressId);
    }

    return addressId;
  }

  private void printFavouriteDestinations(List<FavouriteDestination> favouriteDestinations) {
    List<String> headers = List.of("Name", "Street", "City", "Province", "Postal Code");
    List<List<String>> data = new ArrayList<>();
    for (FavouriteDestination destination : favouriteDestinations) {
      List<String> rowData = new ArrayList<>();
      rowData.add(destination.getName());
      rowData.add(destination.getStreet());
      rowData.add(destination.getCity());
      rowData.add(destination.getProvince());
      rowData.add(destination.getPostalCode());
      data.add(rowData);
    }

    // Print the table
    PrintableTable.printTable("Favourite Destinations", headers, data);

  }

}
