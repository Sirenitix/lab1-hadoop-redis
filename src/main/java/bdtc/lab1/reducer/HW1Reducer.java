package bdtc.lab1.reducer;

import static bdtc.lab1.config.RedisConfig.REDIS_HOST;
import static bdtc.lab1.config.RedisConfig.REDIS_PORT;
import static bdtc.lab1.helper.HW1ReducerHelper.REDIS_HASH;
import static bdtc.lab1.helper.HW1ReducerHelper.averageIntPair;
import static bdtc.lab1.helper.HW1ReducerHelper.averageTemperature;
import static bdtc.lab1.helper.HW1ReducerHelper.highIntPair;
import static bdtc.lab1.helper.HW1ReducerHelper.highTemperature;
import static bdtc.lab1.helper.HW1ReducerHelper.lowIntPair;
import static bdtc.lab1.helper.HW1ReducerHelper.lowTemperature;
import static bdtc.lab1.helper.HW1ReducerHelper.stringToIntPair;

import bdtc.lab1.mapper.HW1Mapper;
import bdtc.lab1.util.IntPair;
import java.io.IOException;
import java.util.Map;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import redis.clients.jedis.Jedis;

/**
 * Reducer: суммирует все единицы полученные от {@link HW1Mapper},
 * выдаёт суммарное температуру и количество нажатии на определенные области экрана
 */
public class HW1Reducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    /*
     * Пишем справочные данные температур в Redis
     */
    static {
        try (Jedis jedis = new Jedis(REDIS_HOST, REDIS_PORT)) {
            jedis.hset(REDIS_HASH, lowIntPair, lowTemperature);
            jedis.hset(REDIS_HASH, averageIntPair, averageTemperature);
            jedis.hset(REDIS_HASH, highIntPair, highTemperature);
        }
    }

    /*
     * Суммируем количество нажатий на области и находим температуру
     */
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException {
            int clickCount = 0;
            while (values.iterator().hasNext()) {
                clickCount += values.iterator().next().get();
            }
            String temperature = getTemperature(clickCount);
            Text text = new Text(key.toString() + temperature);
            context.write(text, new IntWritable(clickCount));
    }

    /**
     * Выявляем температуру из справочника по количеству нажатии
     * @param clickCount количество нажатий на область экрана
     * @return возвращает относительное описание температуры (Низкая-Высокая)
     */
    private String getTemperature(int clickCount) {
        String temperatureName = "Неизвестная температура";
        try (Jedis jedis = new Jedis(REDIS_HOST, REDIS_PORT)) {
            Map<String, String> temperatures = jedis.hgetAll(REDIS_HASH);
            for (Map.Entry<String, String> entry : temperatures.entrySet()) {
                IntPair range = stringToIntPair(entry.getKey());
                if (clickCount >= range.getLeft() && clickCount <= range.getRight()) {
                    temperatureName = entry.getValue();
                    break;
                }
            }
        }
        return temperatureName;
    }
}
