import java.util.HashMap;
import java.util.Map;

public class MapTest {

    public static void main(String args[]) {
        DummyClass dummyClass = new DummyClass();
        Map<DummyClass, Integer> map = new HashMap<>();
        map.put(dummyClass, 1);
        System.out.println("Does map contains element " + map.containsKey(dummyClass));
        dummyClass.a =10;
        System.out.println("Does map contains element " + map.containsKey(dummyClass));
    }
}

class DummyClass {
    Integer a = 5;
    Integer b = 7;
}


