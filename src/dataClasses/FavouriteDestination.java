package dataClasses;

public class FavouriteDestination {
  private String name;
  private int addressId;
  private String street;
  private String city;
  private String province;
  private String postalCode;

  public FavouriteDestination() {
  }

  public FavouriteDestination(String name, int addressId, String street, String city, String province,
      String postalCode) {
    this.addressId = addressId;
    this.name = name;
    this.street = street;
    this.city = city;
    this.province = province;
    this.postalCode = postalCode;
  }

  public String getName() {
    return name;
  }

  public int getAddressId() {
    return addressId;
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

  public void setName(String name) {
    this.name = name;
  }

  public void setAddressId(int id) {
    this.addressId = id;
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
