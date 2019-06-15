package ucr.ppci.nosql.movies;

import com.google.gson.JsonParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Util {

    final private static Logger logger = LoggerFactory.getLogger(Util.class);

    public static float parseFloat(String floatString) {
        float floatNumber = 0;

        try {
            floatNumber = Float.parseFloat(floatString);
        } catch (Exception e) {
            logger.error("Failed to parse float: {}. {}", floatString, e.getMessage());
        }

        return floatNumber;
    }

    public static String sanitizeJson(String jsonString) {
        String sanitizedJson = "";

        // these JSON values INCORRECTLY use single quotes for string values and GSON parser can fix that
        try {
            sanitizedJson = new JsonParser().parse(jsonString).toString();
        } catch (Exception e) {
            logger.error("Failed to parse JSON: {}. {}", jsonString, e.getMessage());
        }

        return sanitizedJson;
    }
}
