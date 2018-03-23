import com.hazelcast.aggregation.Aggregator;
import com.hazelcast.aggregation.impl.AbstractAggregator;

import java.time.LocalDate;
import java.util.HashMap;
import java.util.Map;

public class CumulativeSumAggregator extends Aggregator<Map.Entry<LocalDate, Integer>, Map<LocalDate, Integer>> {

    private Map<LocalDate, Integer> result = new HashMap<>();
    private int currentSum = 0;


    @Override
    public void accumulate(Map.Entry<LocalDate, Integer> input) {
        currentSum += input.getValue();
        result.put(input.getKey(), currentSum);
    }

    @Override
    public void combine(Aggregator aggregator) {
        result.putAll(((CumulativeSumAggregator)aggregator).result);
    }

    @Override
    public Map<LocalDate, Integer> aggregate() {
        return result;
    }
}
