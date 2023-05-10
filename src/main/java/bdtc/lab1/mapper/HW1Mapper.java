package bdtc.lab1.mapper;

import static bdtc.lab1.config.RedisConfig.*;
import static bdtc.lab1.helper.HW1MapperHelper.*;

import bdtc.lab1.util.CounterType;
import bdtc.lab1.util.Rectangle;
import java.io.IOException;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;
import redis.clients.jedis.Jedis;

/*
* Кастомный mapper класс
*/
public class HW1Mapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    /*
    * Четыре области экрана которые записываются в Redis
    */
    @Override
    public void setup(Context context) {
        try (Jedis jedis = new Jedis(REDIS_HOST, REDIS_PORT)) {
            jedis.hset(REDIS_HASH, rectangleLeftTopAngleKey, rectangleLeftTopAngleValue);
            jedis.hset(REDIS_HASH, rectangleRightTopAngleKey, rectangleRightTopAngleValue);
            jedis.hset(REDIS_HASH, rectangleLeftLowerAngleKey, rectangleLeftLowerAngleValue);
            jedis.hset(REDIS_HASH, rectangleRightLowerAngleKey, rectangleRightLowerAngleValue);
        }
    }

    /*
     * Маппинг данных из входного файла и передача в reducer
     */
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split(",");

        // Валидация входной строки
        if (!isValidLine(fields, context)) {
            return;
        }

        // Получаем координаты нажатия
        int x = Integer.parseInt(fields[0]);
        int y = Integer.parseInt(fields[1]);

        // Определяем область экрана, в которой произошло нажатие
        String areaName = "Неизвестная область";
        try (Jedis jedis = new Jedis(REDIS_HOST, REDIS_PORT)) {
            Map<String, String> areas = jedis.hgetAll(REDIS_HASH);
            for (Map.Entry<String, String> entry : areas.entrySet()) {
                Rectangle rect = stringToRectangle(entry.getKey());
                if (rect.contains(x, y)) {
                    areaName = entry.getValue();
                    break;
                }
            }
        }

        // Отправляем результат в reducer
        context.write(new Text(areaName + ", "), new IntWritable(1));
    }

    /**
    * Логика валидации массива
    * @param fields массив данных
    * @param context передаем context, чтобы найти счетчик
    * @return boolean значение, которое показывает валидные данные или нет
    */
    private boolean isValidLine(String[] fields, Context context) {
        boolean isValid = fields.length >= 2
            && StringUtils.isNumeric(fields[0])
            && StringUtils.isNumeric(fields[1]);
        if (!isValid) {
            Counter counter = context.getCounter(CounterType.MALFORMED);
            counter.increment(1);
        }
        return isValid;
    }

}

