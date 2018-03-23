import com.hazelcast.core.*;
import com.hazelcast.config.*;
import com.hazelcast.hotrestart.HotRestartService;

import java.io.File;
import java.time.LocalDate;
import java.util.Map;

public class Demo {

    public static final String TIME_SERIES = "timeSeries";
    private final HazelcastInstance hazelcastInstance;

    private IMap<LocalDate, Integer> timeSeries;

    public Demo(HazelcastInstance hazelcastInstance) {
        this.hazelcastInstance = hazelcastInstance;
    }

    public static void main(String[] args) {


        Config config = new Config();

        HotRestartPersistenceConfig hotRestartPersistenceConfig = new HotRestartPersistenceConfig();
        hotRestartPersistenceConfig.setEnabled(true);
        hotRestartPersistenceConfig.setBaseDir(new File("hot-restart"));
        hotRestartPersistenceConfig.setParallelism(1);
        hotRestartPersistenceConfig.setValidationTimeoutSeconds(120);
        hotRestartPersistenceConfig.setDataLoadTimeoutSeconds(900);
        hotRestartPersistenceConfig.setClusterDataRecoveryPolicy(HotRestartClusterDataRecoveryPolicy.FULL_RECOVERY_ONLY);
        config.setHotRestartPersistenceConfig(hotRestartPersistenceConfig);

        MapConfig mapConfig = config.getMapConfig(TIME_SERIES);
        mapConfig.getHotRestartConfig().setEnabled(true);

        HazelcastInstance instance = Hazelcast.newHazelcastInstance(config);

        Demo demo = new Demo(instance);

        demo.initializeData( 3);
        demo.runAggregate();
        demo.createBackup();

        instance.shutdown();

    }

    public void createBackup () {
        HotRestartService service = hazelcastInstance.getCluster().getHotRestartService();
        service.backup();

    }

    private static void printMap(Map<LocalDate, Integer> result, String name) {
        System.out.println(name);
        result.entrySet().forEach(e -> System.out.println(e.getKey() + " : " + e.getValue()));
    }

    private Map<LocalDate, Integer> runAggregate() {
        Map<LocalDate, Integer> aggregate = timeSeries.aggregate(new CumulativeSumAggregator());

        printMap(timeSeries, "Input");
        printMap(aggregate, "Result");

        return aggregate;
    }

    private void initializeData(int size) {
        IMap<LocalDate, Integer> timeSeries = hazelcastInstance.getMap(TIME_SERIES);

        for (int i = 0; i < size; i++) {
            timeSeries.put(LocalDate.now().minusDays(i), randomWithRange(0, 3));
        }

        this.timeSeries = timeSeries;

    }


    int randomWithRange(int min, int max) {
        int range = (max - min) + 1;
        return (int)(Math.random() * range) + min;
    }
}
