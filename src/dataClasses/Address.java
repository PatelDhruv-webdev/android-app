package dataClasses;

public class Address {
  private int id;
  private String street;
  private String city;
  private String province;
  private String postalCode;

  public Address() {

  }

  public Address(String street, String city, String province, String postalCode) {
    this.street = street;
    this.city = city;
    this.province = province;
    this.postalCode = postalCode;
  }

  public Address(int id, String street, String city, String province, String postalCode) {
    this(street, city, province, postalCode);
    this.id = id;
  }

  public int getId() {
    return id;
  }

  public String getStreet() {
    return street;
  }

  public String getCity() {
    return city;
  }

  public String getProvince() {
    return province;
  }

  public String getPostalCode() {
    return postalCode;
  }

  public void setId(int id) {
    this.id = id;
  }

  public void setStreet(String street) {
    this.street = street;
  }

  public void setCity(String city) {
    this.city = city;
  }

  public void setProvince(String province) {
    this.province = province;
  }

  public void setPostalCode(String postalCode) {
    this.postalCode = postalCode;
  }

}
