import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DataStructure<E> {

    private List<E> list = new ArrayList<>();
    private Map<E, Integer> map = new HashMap<>();

    void add(E value) {
        if(map.containsKey(value)) {
            Integer localValue = map.get(value);
            localValue++;
            map.put(value, localValue);
            list.add(value);
        } else {
            map.put(value, 1);
            list.add(value);
        }
    }

    void delete(E value) throws Exception {
        if(map.containsKey(value)) {
            if(map.get(value).equals(1)) {
                map.remove(value);
            } else {
                Integer localValue = map.get(value);
                map.put(value, localValue--);
            }
            list.remove(value);
        } else {
            throw new Exception("value " + value + " is not present");
        }
    }

    E getRandom() throws Exception {
        double randomNumber = Math.random();
        int listLength = list.size();
        if(listLength < 1) {
            throw new Exception("No elements are present");
        } else {
            int index = (int) (randomNumber % listLength);
            return list.get(index);
        }
    }
}
