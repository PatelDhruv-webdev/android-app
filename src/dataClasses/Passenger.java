package dataClasses;

public class Passenger {
  private String creditCardNumber;

  public Passenger() {
  }

  public Passenger(String creditCardNumber) {
    this.creditCardNumber = creditCardNumber;
  }

  public String getCreditCardNumber() {
    return this.creditCardNumber;
  }

  public void setCreditCardNumber(String creditCardNumber) {
    this.creditCardNumber = creditCardNumber;
  }
}
