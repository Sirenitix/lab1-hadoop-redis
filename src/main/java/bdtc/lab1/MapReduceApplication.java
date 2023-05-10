package bdtc.lab1;

import bdtc.lab1.exception.ArgumentsNotGivenException;
import bdtc.lab1.mapper.HW1Mapper;
import bdtc.lab1.reducer.HW1Reducer;
import bdtc.lab1.util.CounterType;
import lombok.extern.log4j.Log4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

/**
 * Базовый класс для запуска приложения
 */
@Log4j
public class MapReduceApplication {

    /**
     * Базовый метод с конфигурацией и запуском job-ы
     */
    public static void main(String[] args) throws Exception {
        log.info("Starting job with args: \n" + String.join(", ", args));
        if (args.length < 2) {
            throw new ArgumentsNotGivenException("You should specify input and output folders!");
        }
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Temperature calculation");
        job.setJarByClass(MapReduceApplication.class);
        job.setMapperClass(HW1Mapper.class);
        job.setReducerClass(HW1Reducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        Path outputDirectory = new Path(args[1]);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, outputDirectory);
        log.info("=====================JOB STARTED=====================");
        job.waitForCompletion(true);
        log.info("=====================JOB ENDED=====================");
        // проверяем статистику по счётчикам
        Counter counter = job.getCounters().findCounter(CounterType.MALFORMED);
        log.info("=====================COUNTERS "
            + counter.getName() + ": "
            + counter.getValue() + "=====================");
    }

}
