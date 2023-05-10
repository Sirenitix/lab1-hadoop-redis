package bdtc.lab1.helper;

import bdtc.lab1.util.Rectangle;
import java.util.Base64;
import org.apache.commons.lang3.SerializationUtils;

/**
 * Константы нужные для работы класса HW1Mapper
 */
public class HW1MapperHelper {
    public static final String REDIS_HASH = "screen scope";

    public static final String rectangleLeftTopAngleKey =
        rectangleToByteArray(new Rectangle(0, 0, 500, 500));
    public static final String rectangleRightTopAngleKey =
        rectangleToByteArray(new Rectangle(500, 0, 1000, 500));
    public static final String rectangleLeftLowerAngleKey =
        rectangleToByteArray(new Rectangle(0, 500, 500, 1000));
    public static final String rectangleRightLowerAngleKey = rectangleToByteArray(
        new Rectangle(500, 500, 1000, 1000)
    );

    public static final String rectangleLeftTopAngleValue = "Левый верхний угол";
    public static final String rectangleRightTopAngleValue = "Правый верхний угол";
    public static final String rectangleLeftLowerAngleValue = "Левый нижний угол";
    public static final String rectangleRightLowerAngleValue = "Правый нижний угол";
    
    public static String rectangleToByteArray(Rectangle rectangle) {
        return Base64.getEncoder().encodeToString(SerializationUtils.serialize(rectangle));
    }

    public static Rectangle stringToRectangle(String rectangle) {
        return SerializationUtils.deserialize(Base64.getDecoder().decode(rectangle));
    }

}
