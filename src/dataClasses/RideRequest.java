package dataClasses;

public class RideRequest {
  private int id;
  private String passengerFirstName;
  private String passengerLastName;
  private String pickupStreet;
  private String pickupCity;
  private String dropoffStreet;
  private String dropOffCity;
  private String desiredPickupDate;
  private String desiredPickupTime;

  public RideRequest() {
  }

  public RideRequest(int id, String passengerFirstName, String passengerLastName, String pickupStreet,
      String pickupCity,
      String dropoffStreet, String dropOffCity, String desiredPickupDate, String desiredPickupTime) {
    this.id = id;
    this.passengerFirstName = passengerFirstName;
    this.passengerLastName = passengerLastName;
    this.pickupStreet = pickupStreet;
    this.pickupCity = pickupCity;
    this.dropoffStreet = dropoffStreet;
    this.dropOffCity = dropOffCity;
    this.desiredPickupDate = desiredPickupDate;
    this.desiredPickupTime = desiredPickupTime;
  }

  public int getId() {
    return id;
  }

  public void setId(int id) {
    this.id = id;
  }

  public String getPassengerFirstName() {
    return passengerFirstName;
  }

  public void setPassengerFirstName(String passengerFirstName) {
    this.passengerFirstName = passengerFirstName;
  }

  public String getPassengerLastName() {
    return passengerLastName;
  }

  public void setPassengerLastName(String passengerLastName) {
    this.passengerLastName = passengerLastName;
  }

  public String getPickupStreet() {
    return pickupStreet;
  }

  public void setPickupStreet(String pickupStreet) {
    this.pickupStreet = pickupStreet;
  }

  public String getPickupCity() {
    return pickupCity;
  }

  public void setPickupCity(String pickupCity) {
    this.pickupCity = pickupCity;
  }

  public String getDropoffStreet() {
    return dropoffStreet;
  }

  public void setDropoffStreet(String dropoffStreet) {
    this.dropoffStreet = dropoffStreet;
  }

  public String getDropOffCity() {
    return dropOffCity;
  }

  public void setDropOffCity(String dropOffCity) {
    this.dropOffCity = dropOffCity;
  }

  public String getDesiredPickupDate() {
    return desiredPickupDate;
  }

  public void setDesiredPickupDate(String desiredPickupDate) {
    this.desiredPickupDate = desiredPickupDate;
  }

  public String getDesiredPickupTime() {
    return desiredPickupTime;
  }

  public void setDesiredPickupTime(String desiredPickupTime) {
    this.desiredPickupTime = desiredPickupTime;
  }
}
