package com.mob.kafkawapper.test2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

import com.lamfire.logger.Logger;
import com.lamfire.utils.RandomUtils;
import com.mob.kafkawapper.producer.MessageProducer;

/**
 * @author zxc Jul 26, 2017 3:10:56 PM
 */
class TestInnerProducer implements Callable<Map<String, Integer>> {

    private static final Logger LOGGER = Logger.getLogger(TestInnerProducer.class);
    private int                 taskNum;
    private MessageProducer     producer;

    TestInnerProducer(int taskNum, MessageProducer producer) {
        this.taskNum = taskNum;
        this.producer = producer;
    }

    public Map<String, Integer> call() throws Exception {
        long start = System.currentTimeMillis();
        LOGGER.debug(taskNum + " task run");

        String key = RandomUtils.randomText(12, 12);
        int size = RandomUtils.nextInt(10000);
        Map<String, Integer> map = new HashMap<String, Integer>(1);
        map.put(taskNum + " " + key, size);

        for (int j = 0; j < size; j++) {
            producer.send("testinner_topic", key, false);
            LOGGER.debug("task " + taskNum + "run " + j + " times");
        }

        Thread.sleep(500);
        LOGGER.debug(taskNum + "task finish【" + (System.currentTimeMillis() - start) + "ms】");
        return map;
    }
}

/**
 * @author zxc Jul 26, 2017 3:10:56 PM
 */
public class ProducerTest {

    private static final Logger _ = Logger.getLogger(ProducerTest.class);

    public static void main(String[] args) {
        try {
            long start = System.currentTimeMillis();
            MessageProducer producer = MessageProducer.getInstance();
            _.info("....................start....................");
            int taskSize = 10;
            ExecutorService pool = Executors.newFixedThreadPool(taskSize);
            List<Future<Map<String, Integer>>> list = new ArrayList<Future<Map<String, Integer>>>(taskSize);
            for (int i = 0; i < taskSize; i++) {
                TestInnerProducer analysisProducer = new TestInnerProducer(i, producer);
                Future<Map<String, Integer>> submit = pool.submit(analysisProducer);
                list.add(submit);
            }
            pool.shutdown();
            producer.close();

            for (Future<Map<String, Integer>> future : list) {
                Map<String, Integer> map = future.get();
                for (Map.Entry<String, Integer> entry : map.entrySet()) {
                    _.debug(entry.getKey() + " : " + entry.getValue());
                }
            }
            _.info("....................end...................." + (System.currentTimeMillis() - start));
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
    }
}
