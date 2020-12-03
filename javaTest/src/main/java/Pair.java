import java.util.HashSet;
import java.util.Set;
import java.util.TreeSet;

/**
 * <h3>bigdata</h3>
 *
 * @author : zhao
 * @version :
 * @date : 2020-08-05 20:50
 */
public class Pair<T> {
    private T value;
    public Pair(T value) {
        this.value=value;
    }
    public T getValue() {
        return value;
    }
    public void setValue(T value) {
        this.value = value;
    }

    public static void main(String[] args) {
        // 泛型参数T具体传入String类型
        Pair<String> pair=new Pair<String>("Hello");
        String str=pair.getValue();
        System.out.println(str);

        // 泛型参数T具体传入Integer类型
        Pair<Integer> pair1 = new Pair<Integer>(11);
        Integer integer = pair1.getValue();
        System.out.println(integer);


        Set<?> s = new HashSet<String>();
        s = new HashSet<Integer>();

    }
}
