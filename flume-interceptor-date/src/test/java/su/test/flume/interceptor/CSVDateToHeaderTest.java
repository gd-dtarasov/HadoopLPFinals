package su.test.flume.interceptor;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.event.SimpleEvent;
import org.apache.flume.interceptor.Interceptor;
import org.apache.flume.interceptor.TimestampInterceptor;
import org.junit.Assert;
import org.junit.Test;

public class CSVDateToHeaderTest {
    @Test
    public void parseTest() {
        Event event = new SimpleEvent();
        event.setBody("Destroyed Navy Blue Male Jeans,60.09,2018-08-07T05:28:14,BOGO,218.116.51.57".getBytes());
        CSVDateToHeader.Builder builder = new CSVDateToHeader.Builder();
        Context context = new Context();
        context.put(CSVDateToHeader.Builder.PATTERN_PARAM_NAME, "yyyy-MM-dd'T'HH:mm:ss");
        context.put(CSVDateToHeader.Builder.LINE_SEPARATOR_PARAM_NAME, ",");
        context.put(CSVDateToHeader.Builder.DATE_FIELD_INDEX_PARAM_NAME, Integer.toString(2));
        builder.configure(context);
        Interceptor csvDateToHeader = builder.build();
        csvDateToHeader.initialize();
        csvDateToHeader.intercept(event);
        Assert.assertEquals("1533605294000", event.getHeaders().get(TimestampInterceptor.Constants.TIMESTAMP));
    }
}
