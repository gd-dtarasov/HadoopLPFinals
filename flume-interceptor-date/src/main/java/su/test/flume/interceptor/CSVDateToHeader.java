package su.test.flume.interceptor;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;
import org.apache.flume.interceptor.TimestampInterceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class CSVDateToHeader implements Interceptor {
    Logger logger = LoggerFactory.getLogger(CSVDateToHeader.class);
    private String format;
    private String lineSeparator;
    private int dateFieldIndex;
    private DateFormat dateFormat;

    public CSVDateToHeader(String format, String lineSeparator, int dateFieldIndex) {
        this.format = format;
        this.lineSeparator = lineSeparator;
        this.dateFieldIndex = dateFieldIndex;
    }

    public void initialize() {
        dateFormat = new SimpleDateFormat(format);
    }

    public Event intercept(Event event) {
        String body = new String(event.getBody());
        logger.debug("RECIEVED BODY " + body);
        String[] bodyParts = body.split(lineSeparator);
        try {
            Date date = dateFormat.parse(bodyParts[dateFieldIndex]);
            logger.debug("PARSED DATE " + date);
            Map<String, String> headers = event.getHeaders();
            headers.put(TimestampInterceptor.Constants.TIMESTAMP, Long.toString(date.getTime()));
            return event;
        } catch (Exception e) {
            logger.error("Error parsing ",e);
            throw new RuntimeException(e);
        }
    }

    public List<Event> intercept(List<Event> list) {
        Iterator i$ = list.iterator();

        while (i$.hasNext()) {
            Event event = (Event) i$.next();
            this.intercept(event);
        }

        return list;
    }

    public void close() {

    }

    public static class Builder implements Interceptor.Builder {
        public static final String LINE_SEPARATOR_PARAM_NAME = "lineSeparator";
        public static final String PATTERN_PARAM_NAME = "pattern";
        public static final String DATE_FIELD_INDEX_PARAM_NAME = "dateFieldIndex";
        private String format;
        private String lineSeparator;
        private int dateFieldIndex;

        public Builder() {
        }

        public Interceptor build() {
            return new CSVDateToHeader(format, lineSeparator, dateFieldIndex);
        }

        public void configure(Context context) {
            format = context.getString(PATTERN_PARAM_NAME, "yyyy-MM-dd'T'HH:mm:ss");
            lineSeparator = context.getString(LINE_SEPARATOR_PARAM_NAME, ",");
            dateFieldIndex = context.getInteger(DATE_FIELD_INDEX_PARAM_NAME, 0);
        }
    }
}
