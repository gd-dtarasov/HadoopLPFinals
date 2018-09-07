package su.test.hiveudfiprange;

import org.apache.hadoop.io.Text;

public class TestMain {
    public static void main(String[] args) {
        IsInRange isInRange = new IsInRange();
        System.out.println(isInRange.evaluate(new Text("89.22.0.0/21"),new Text("89.22.1.3")));
    }
}
