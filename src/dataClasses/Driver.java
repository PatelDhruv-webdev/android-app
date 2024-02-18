package dataClasses;

public class Driver {
  private String licenseNumber;
  private String licenseExpiryDate;

  public Driver() {
  }

  public Driver(String licenseNumber, String licenseExpiryDate) {
    this.licenseNumber = licenseNumber;
    this.licenseExpiryDate = licenseExpiryDate;
  }

  public String getLicenseNumber() {
    return licenseNumber;
  }

  public String getLicenseExpiryDate() {
    return licenseExpiryDate;
  }

  public void setLicenseNumber(String licenseNumber) {
    this.licenseNumber = licenseNumber;
  }

  public void setLicenseExpiryDate(String licenseExpiryDate) {
    this.licenseExpiryDate = licenseExpiryDate;
  }
}
