package su.test.hiveudfiprange;

import org.apache.commons.net.util.SubnetUtils;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.Text;

@Description(name = "is_in_range", value = "_FUNC_(subnet, ip) - check if ip belongs to subnet in CIDR notation")
public class IsInRange extends UDF {
    public BooleanWritable evaluate(Text subnet, Text ip) {
        SubnetUtils subnetUtils = new SubnetUtils(subnet.toString());
        return new BooleanWritable(subnetUtils.getInfo().isInRange(ip.toString()));
    }
}
