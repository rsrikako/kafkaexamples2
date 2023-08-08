package json;

import java.util.Date;

public class Customer {
    private int customerId;
    private String customerName;

    public Date getBirthDate() {
        return birthDate;
    }

    public void setBirthDate(Date birthDate) {
        this.birthDate = birthDate;
    }

    private Date birthDate;

    public Customer(int customerId, String customerName, Date birthDate) {
        this.customerId = customerId;
        this.customerName = customerName;
        this.birthDate = birthDate;
    }

    public void setCustomerId(int customerId) {
        this.customerId = customerId;
    }

    public void setCustomerName(String customerName) {
        this.customerName = customerName;
    }

    public int getCustomerId() {
        return customerId;
    }

    public String getCustomerName() {
        return customerName;
    }

    public  Customer(){

    }

    @Override
    public String toString() {
        return "Customer{" +
                "customerId=" + customerId +
                ", customerName='" + customerName + '\'' +
                ", birthDate=" + birthDate +
                '}';
    }
}
