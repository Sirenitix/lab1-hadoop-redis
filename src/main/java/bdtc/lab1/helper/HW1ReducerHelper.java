package bdtc.lab1.helper;

import bdtc.lab1.util.IntPair;
import java.util.Base64;
import org.apache.commons.lang3.SerializationUtils;

/**
 * Константы нужные для работы класса HW1Reducer
 */
public class HW1ReducerHelper {
    public static final String REDIS_HASH = "screen temperature";
    public static final String lowTemperature = "Низкая температура";
    public static final String averageTemperature = "Средняя температура";
    public static final String highTemperature = "Высокая температура";

    public static final String lowIntPair =
        intPairToByteArray(new IntPair(0, 2));
    public static final String averageIntPair =
        intPairToByteArray(new IntPair(3, 5));
    public static final String highIntPair =
        intPairToByteArray(new IntPair(6, 8));

    public static String intPairToByteArray(IntPair pair) {
        return Base64.getEncoder().encodeToString(SerializationUtils.serialize(pair));
    }

    public static IntPair stringToIntPair(String pair) {
        return SerializationUtils.deserialize(Base64.getDecoder().decode(pair));
    }
}
