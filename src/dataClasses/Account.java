package dataClasses;

public class Account {
  private String firstName;
  private String lastName;
  private Address address;
  private String phoneNumber;
  private String email;
  private String birthdate;
  private boolean isPassenger;
  private boolean isDriver;

  public Account() {
    this.address = new Address();

  }

  public Account(String firstName, String lastName, String street, String city, String province, String postalCode,
      String phoneNumber, String email, String birthdate, boolean isPassenger, boolean isDriver) {
    this.firstName = firstName;
    this.lastName = lastName;
    this.address = new Address(street, city, province, postalCode);
    this.phoneNumber = phoneNumber;
    this.email = email;
    this.birthdate = birthdate;
    this.isPassenger = isPassenger;
    this.isDriver = isDriver;
  }

  public String getFirstName() {
    return firstName;
  }

  public String getLastName() {
    return lastName;
  }

  public Address getAddress() {
    return address;
  }

  public String getStreet() {
    return address.getStreet();
  }

  public String getCity() {
    return address.getCity();
  }

  public String getProvince() {
    return address.getProvince();
  }

  public String getPostalCode() {
    return address.getPostalCode();
  }

  public String getPhoneNumber() {
    return phoneNumber;
  }

  public String getEmail() {
    return email;
  }

  public String getBirthdate() {
    return birthdate;
  }

  public boolean isPassenger() {
    return isPassenger;
  }

  public boolean isDriver() {
    return isDriver;
  }

  public void setFirstName(String firstName) {
    this.firstName = firstName;
  }

  public void setLastName(String lastName) {
    this.lastName = lastName;
  }

  public void setAddress(Address address) {
    this.address = address;
  }

  public void setStreet(String street) {
    this.address.setStreet(street);
  }

  public void setCity(String city) {
    this.address.setCity(city);
  }

  public void setProvince(String province) {
    this.address.setProvince(province);
  }

  public void setPostalCode(String postalCode) {
    this.address.setPostalCode(postalCode);
  }

  public void setPhoneNumber(String phoneNumber) {
    this.phoneNumber = phoneNumber;
  }

  public void setEmail(String email) {
    this.email = email;
  }

  public void setBirthdate(String birthdate) {
    this.birthdate = birthdate;
  }

  public void setIsPassenger(boolean isPassenger) {
    this.isPassenger = isPassenger;
  }

  public void setIsDriver(boolean isDriver) {
    this.isDriver = isDriver;
  }

  public boolean isDriverAndPassenger() {
    return this.isPassenger && this.isDriver;
  }

}
