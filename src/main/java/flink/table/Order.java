package flink.table;

public class Order {
    public long user;
    public String product;
    public int count;

    public Order() {
    }

    public Order(int count, String product, long user) {
        this.user = user;
        this.product = product;
        this.count = count;
    }

    @Override
    public String toString() {
        return "Order{" +
                "user=" + user +
                ", product='" + product + '\'' +
                ", count=" + count +
                '}';
    }
}
