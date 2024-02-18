/*
 * Group members: YOUR NAMES HERE
 * Instructions: For Project 2, implement all methods in this class, and test to confirm they behave as expected when the program is run.
 */

package database;

import java.sql.*;
import java.util.*;

import dataClasses.*;
import dataClasses.Driver;

public class DatabaseMethods {
  private Connection conn;

  public DatabaseMethods(Connection conn) {
    this.conn = conn;
  }

  /*
   * Accepts: Nothing
   * Behaviour: Retrieves information about all accounts
   * Returns: List of account objects
   */
  public ArrayList<Account> getAllAccounts() throws SQLException {
    ArrayList<Account> accounts = new ArrayList<Account>();

    Statement statement = this.conn.createStatement();
    String query = "SELECT a.*, d.id IS NOT NULL as IS_DRIVER, p.id IS NOT NULL as IS_PASSENGER, ad.* from accounts a "
        + "LEFT JOIN addresses ad on ad.id==a.id "
        + "LEFT JOIN drivers d on d.id==a.id "
        + "LEFT JOIN passengers p on p.id==a.id ";

    ResultSet resultSet = statement.executeQuery(query);

    Account tmp_ac = null;
    while (resultSet.next()) {
      tmp_ac = new Account(resultSet.getString("FIRST_NAME"), resultSet.getString("LAST_NAME"),
          resultSet.getString("STREET"),
          resultSet.getString("CITY"), resultSet.getString("PROVINCE"), resultSet.getString("POSTAL_CODE"),
          resultSet.getString("PHONE_NUMBER"),
          resultSet.getString("EMAIL"), resultSet.getString("BIRTHDATE"), resultSet.getBoolean("IS_PASSENGER"),
          resultSet.getBoolean("IS_DRIVER"));
      accounts.add(tmp_ac);
    }

    resultSet.close();
    statement.close();

    return accounts;
  }

  /*
   * Accepts: Email address of driver
   * Behaviour: Calculates the average rating over all rides performed by the
   * driver specified by the email address
   * Returns: The average rating value
   */
  public double getAverageRatingForDriver(String driverEmail) throws SQLException {
    double averageRating = 0.0;

    Statement statement = this.conn.createStatement();
    String query = "SELECT AVG(RATING_FROM_PASSENGER) as avg_rating from rides where driver_id=(select id from accounts where email='"
        + driverEmail + "');";

    ResultSet resultSet = statement.executeQuery(query);
    if (resultSet.next()) {
      averageRating = resultSet.getFloat("avg_rating");
    }
    resultSet.close();
    statement.close();

    return averageRating;
  }

  /*
   * Accepts: Account details, and passenger and driver specific details.
   * Passenger or driver details could be
   * null if account is only intended for one type of use.
   * Behaviour:
   * - Insert new account using information provided in Account object
   * - For non-null passenger/driver details, insert the associated data into the
   * relevant tables
   * Returns: Nothing
   */
  public void createAccount(Account account, Passenger passenger, Driver driver) throws SQLException {
    int account_id = this.insertAccount(account);
    if (passenger != null) {
      this.insertPassenger(passenger, account_id);
    }
    if (driver != null) {
      this.insertDriver(driver, account_id);
    }
  }

  /*
   * Accepts: Account details (which includes address information)
   * Behaviour: Inserts the new account, as well as the account's address if it
   * doesn't already exist. The new/existing address should
   * be linked to the account
   * Returns: Id of the new account
   */
  public int insertAccount(Account account) throws SQLException {
    int accountId = -1;

    int addressId = this.insertAddressIfNotExists(account.getAddress());

    Statement statement = this.conn.createStatement();

    String insertQuery = "INSERT INTO accounts (FIRST_NAME, LAST_NAME, BIRTHDATE, ADDRESS_ID, PHONE_NUMBER, EMAIL) VALUES (?, ?, ?, ?, ?, ?)";
    String query = "SELECT id from accounts where first_name='" + account.getFirstName() + "' and last_name='"
        + account.getLastName() + "' and birthdate='" + account.getBirthdate() + "' and address_id='"
        + addressId + "' and phone_number='" + account.getPhoneNumber() + "' and email='"
        + account.getEmail() + "';";

    PreparedStatement preparedStatement = this.conn.prepareStatement(insertQuery);

    preparedStatement.setString(1, account.getFirstName());
    preparedStatement.setString(2, account.getLastName());
    preparedStatement.setString(3, account.getBirthdate());
    preparedStatement.setInt(4, addressId);
    preparedStatement.setString(5, account.getPhoneNumber());
    preparedStatement.setString(6, account.getEmail());

    int rowsAffected = preparedStatement.executeUpdate();

    ResultSet resultSet = statement.executeQuery(query);
    accountId = resultSet.getInt("id");

    resultSet.close();
    preparedStatement.close();
    statement.close();

    return accountId;
  }

  /*
   * Accepts: Passenger details (should not be null), and account id for the
   * passenger
   * Behaviour: Inserts the new passenger record, correctly linked to the account
   * id
   * Returns: Id of the new passenger
   */
  public int insertPassenger(Passenger passenger, int accountId) throws SQLException {

    Statement statement = this.conn.createStatement();

    String insertQuery = "INSERT INTO passengers (ID, CREDIT_CARD_NUMBER) VALUES (?, ?)";

    PreparedStatement preparedStatement = this.conn.prepareStatement(insertQuery);

    preparedStatement.setInt(1, accountId);
    preparedStatement.setString(2, passenger.getCreditCardNumber());

    int rowsAffected = preparedStatement.executeUpdate();

    preparedStatement.close();
    statement.close();

    return accountId;
  }

  /*
   * Accepts: Driver details (should not be null), and account id for the driver
   * Behaviour: Inserts the new driver and driver's license record, correctly
   * linked to the account id
   * Returns: Id of the new driver
   */
  public int insertDriver(Driver driver, int accountId) throws SQLException {
    int licenseId = this.insertLicense(driver.getLicenseNumber(), driver.getLicenseExpiryDate());

    Statement statement = this.conn.createStatement();

    String insertQuery = "INSERT INTO drivers (ID, LICENSE_ID) VALUES (?, ?)";

    PreparedStatement preparedStatement = this.conn.prepareStatement(insertQuery);

    preparedStatement.setInt(1, accountId);
    preparedStatement.setInt(2, licenseId);

    int rowsAffected = preparedStatement.executeUpdate();

    preparedStatement.close();
    statement.close();

    return accountId;
  }

  /*
   * Accepts: Driver's license number and license expiry
   * Behaviour: Inserts the new driver's license record
   * Returns: Id of the new driver's license
   */
  public int insertLicense(String licenseNumber, String licenseExpiry) throws SQLException {
    int licenseId = -1;

    Statement statement = this.conn.createStatement();

    String insertQuery = "INSERT INTO licenses (NUMBER, EXPIRY_DATE) VALUES (?, ?)";
    String query = "SELECT id from licenses where number='" + licenseNumber + "' and expiry_date='" + licenseExpiry
        + "';";

    PreparedStatement preparedStatement = this.conn.prepareStatement(insertQuery);

    preparedStatement.setString(1, licenseNumber);
    preparedStatement.setString(2, licenseExpiry);

    int rowsAffected = preparedStatement.executeUpdate();

    ResultSet resultSet = statement.executeQuery(query);
    licenseId = resultSet.getInt("id");

    resultSet.close();
    preparedStatement.close();
    statement.close();

    return licenseId;
  }

  /*
   * Accepts: Address details
   * Behaviour:
   * - Checks if an address with these properties already exists.
   * - If it does, gets the id of the existing address.
   * - If it does not exist, creates the address in the database, and gets the id
   * of the new address
   * Returns: Id of the address
   */
  public int insertAddressIfNotExists(Address address) throws SQLException {
    int addressId = -1;

    Statement statement = this.conn.createStatement();
    String query = "SELECT id from addresses where street='" + address.getStreet() + "' and city='" + address.getCity()
        + "' and province='" + address.getProvince() + "' and postal_code='" + address.getPostalCode() + "';";
    ResultSet resultSet = statement.executeQuery(query);
    if (resultSet.next()) {
      addressId = resultSet.getInt("id");
    } else {
      resultSet.close();

      String insertQuery = "INSERT INTO addresses (STREET, CITY, PROVINCE, POSTAL_CODE) VALUES (?, ?, ?, ?)";
      PreparedStatement preparedStatement = this.conn.prepareStatement(insertQuery);

      preparedStatement.setString(1, address.getStreet());
      preparedStatement.setString(2, address.getCity());
      preparedStatement.setString(3, address.getProvince());
      preparedStatement.setString(4, address.getPostalCode());

      int rowsAffected = preparedStatement.executeUpdate();
      resultSet = statement.executeQuery(query);
      addressId = resultSet.getInt("id");

      preparedStatement.close();
    }

    resultSet.close();
    statement.close();

    return addressId;
  }

  /*
   * Accepts: Name of new favourite destination, email address of the passenger,
   * and the id of the address being favourited
   * Behaviour: Finds the id of the passenger with the email address, then inserts
   * the new favourite destination record
   * Returns: Nothing
   */
  public void insertFavouriteDestination(String favouriteName, String passengerEmail, int addressId)
      throws SQLException {
    String insertQuery = "INSERT INTO favourite_locations (PASSENGER_ID, LOCATION_ID, NAME) "
        + "VALUES (?, ?, ?)";
    PreparedStatement preparedStatement = this.conn.prepareStatement(insertQuery);

    preparedStatement.setInt(1, this.getPassengerIdFromEmail(passengerEmail));
    preparedStatement.setInt(2, addressId);
    preparedStatement.setString(3, favouriteName);

    int rowsAffected = preparedStatement.executeUpdate();

    preparedStatement.close();
  }

  /*
   * Accepts: Email address
   * Behaviour: Determines if a driver exists with the provided email address
   * Returns: True if exists, false if not
   */
  public boolean checkDriverExists(String email) throws SQLException {
    boolean res = false;

    Statement statement = this.conn.createStatement();
    String query = "SELECT id from drivers where id=(select id from accounts where email='" + email + "');";
    ResultSet resultSet = statement.executeQuery(query);
    res = resultSet.next();

    resultSet.close();
    statement.close();

    return res;
  }

  /*
   * Accepts: Email address
   * Behaviour: Determines if a passenger exists with the provided email address
   * Returns: True if exists, false if not
   */
  public boolean checkPassengerExists(String email) throws SQLException {
    boolean res = false;

    Statement statement = this.conn.createStatement();
    String query = "SELECT id from passengers where id=(select id from accounts where email='" + email + "');";
    ResultSet resultSet = statement.executeQuery(query);
    res = resultSet.next();

    resultSet.close();
    statement.close();

    return res;
  }

  /*
   * Accepts: Email address of passenger making request, id of dropoff address,
   * requested date/time of ride, and number of passengers
   * Behaviour: Inserts a new ride request, using the provided properties
   * Returns: Nothing
   */
  public void insertRideRequest(String passengerEmail, int dropoffLocationId, String date, String time,
      int numberOfPassengers) throws SQLException {
    int passengerId = this.getPassengerIdFromEmail(passengerEmail);
    int pickupAddressId = this.getAccountAddressIdFromEmail(passengerEmail);

    String insertQuery = "INSERT INTO ride_requests (PASSENGER_ID, PICKUP_LOCATION_ID, PICKUP_DATE, PICKUP_TIME, "
        + "NUMBER_OF_RIDERS, DROPOFF_LOCATION_ID) "
        + "VALUES (?, ?, ?, ?, ?, ?)";
    PreparedStatement preparedStatement = this.conn.prepareStatement(insertQuery);

    preparedStatement.setInt(1, passengerId);
    preparedStatement.setInt(2, pickupAddressId);
    preparedStatement.setString(3, date);
    preparedStatement.setString(4, time);
    preparedStatement.setInt(5, numberOfPassengers);
    preparedStatement.setInt(6, dropoffLocationId);

    int rowsAffected = preparedStatement.executeUpdate();

    preparedStatement.close();
  }

  /*
   * Accepts: Email address
   * Behaviour: Gets id of passenger with specified email (assumes passenger
   * exists)
   * Returns: Id
   */
  public int getPassengerIdFromEmail(String passengerEmail) throws SQLException {
    int passengerId = -1;
    Statement statement = this.conn.createStatement();
    String query = "SELECT id from passengers where id=(select id from accounts where email='" + passengerEmail + "');";
    ResultSet resultSet = statement.executeQuery(query);
    passengerId = resultSet.getInt("id");
    resultSet.close();
    statement.close();
    return passengerId;
  }

  /*
   * Accepts: Email address
   * Behaviour: Gets id of driver with specified email (assumes driver exists)
   * Returns: Id
   */
  public int getDriverIdFromEmail(String driverEmail) throws SQLException {
    int driverId = -1;
    Statement statement = this.conn.createStatement();
    String query = "SELECT id from drivers where id=(select id from accounts where email='" + driverEmail + "');";
    ResultSet resultSet = statement.executeQuery(query);
    driverId = resultSet.getInt("id");
    resultSet.close();
    statement.close();
    return driverId;
  }

  /*
   * Accepts: Email address
   * Behaviour: Gets the id of the address tied to the account with the provided
   * email address
   * Returns: Address id
   */
  public int getAccountAddressIdFromEmail(String email) throws SQLException {
    int addressId = -1;
    Statement statement = this.conn.createStatement();
    String query = "SELECT id from addresses where id=(select ADDRESS_ID from accounts where email='" + email + "');";
    ResultSet resultSet = statement.executeQuery(query);
    addressId = resultSet.getInt("id");
    resultSet.close();
    statement.close();
    return addressId;
  }

  /*
   * Accepts: Email address of passenger
   * Behaviour: Gets a list of all the specified passenger's favourite
   * destinations
   * Returns: List of favourite destinations
   */
  public ArrayList<FavouriteDestination> getFavouriteDestinationsForPassenger(String passengerEmail)
      throws SQLException {
    ArrayList<FavouriteDestination> favouriteDestinations = new ArrayList<FavouriteDestination>();

    Statement statement = this.conn.createStatement();
    String query = "select * from addresses JOIN favourite_locations ON addresses.id=favourite_locations.LOCATION_ID where favourite_locations.PASSENGER_ID=(select id from accounts where email='"
        + passengerEmail + "');";
    ResultSet resultSet = statement.executeQuery(query);
    FavouriteDestination tmp_dst = null;
    while (resultSet.next()) {
      tmp_dst = new FavouriteDestination(resultSet.getString("name"), resultSet.getInt("location_id"),
          resultSet.getString("STREET"), resultSet.getString("CITY"),
          resultSet.getString("PROVINCE"), resultSet.getString("POSTAL_CODE"));
      favouriteDestinations.add(tmp_dst);
    }
    resultSet.close();
    statement.close();
    return favouriteDestinations;
  }

  /*
   * Accepts: Nothing
   * Behaviour: Gets a list of all uncompleted ride requests (i.e. requests
   * without an associated ride record)
   * Returns: List of all uncompleted rides
   */
  public ArrayList<RideRequest> getUncompletedRideRequests() throws SQLException {
    ArrayList<RideRequest> uncompletedRideRequests = new ArrayList<RideRequest>();

    Statement statement = this.conn.createStatement();
    String query = "select rr.id, ac.FIRST_NAME,ac.LAST_NAME, padd.STREET as p_street,padd.CITY as  p_city, dadd.STREET as d_street, dadd.CITY as d_city, "
        + "rr.PICKUP_DATE, rr.PICKUP_TIME "
        + "from ride_requests as rr "
        + "join accounts as ac ON ac.ID=rr.PASSENGER_ID "
        + "JOIN addresses as padd on padd.ID = rr.PICKUP_LOCATION_ID "
        + "JOIN addresses as dadd on dadd.ID = rr.DROPOFF_LOCATION_ID "
        + "where rr.id not in (select REQUEST_ID from rides);";
    ResultSet resultSet = statement.executeQuery(query);
    RideRequest tmp_rr = null;
    while (resultSet.next()) {
      tmp_rr = new RideRequest(resultSet.getInt("id"), resultSet.getString("first_name"),
          resultSet.getString("last_name"),
          resultSet.getString("p_street"), resultSet.getString("p_city"),
          resultSet.getString("d_street"), resultSet.getString("d_city"),
          resultSet.getString("pickup_date"), resultSet.getString("pickup_time"));
      uncompletedRideRequests.add(tmp_rr);
    }
    resultSet.close();
    statement.close();
    return uncompletedRideRequests;
  }

  /*
   * Accepts: Ride details
   * Behaviour: Inserts a new ride record
   * Returns: Nothing
   */
  public void insertRide(Ride ride) throws SQLException {
    String insertQuery = "INSERT INTO rides (DRIVER_ID, REQUEST_ID, ACTUAL_START_DATE, ACTUAL_START_TIME, "
        + "ACTUAL_END_DATE, ACTUAL_END_TIME, RATING_FROM_DRIVER, RATING_FROM_PASSENGER, DISTANCE, CHARGE) "
        + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
    PreparedStatement preparedStatement = this.conn.prepareStatement(insertQuery);

    preparedStatement.setInt(1, this.getDriverIdFromEmail(ride.getDriverEmail()));
    preparedStatement.setInt(2, ride.getRideRequestId());
    preparedStatement.setString(3, ride.getStartDate());
    preparedStatement.setString(4, ride.getStartTime());
    preparedStatement.setString(5, ride.getEndDate());
    preparedStatement.setString(6, ride.getEndTime());
    preparedStatement.setFloat(7, ride.getRatingFromDriver());
    preparedStatement.setFloat(8, ride.getRatingFromPassenger());
    preparedStatement.setDouble(9, ride.getDistance());
    preparedStatement.setDouble(10, ride.getCharge());

    preparedStatement.executeUpdate();
    preparedStatement.close();
  }

}
