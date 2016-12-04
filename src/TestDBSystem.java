import java.util.Map;

/**
 * Created by kc on 12/4/16.
 */
public class TestDBSystem {
  public static void testConstructor() {
    System.out.println("Test Constructor");
    DBSystem test = new DBSystem();
    for (Site s : test._sites) {
      System.out.println("site " + s._sid + ": ");
      for (Map.Entry<String, Variable> e : s._variables.entrySet()) {
        System.out.println(e.getValue()._vid + ": " + e.getValue().readLastCommited());
      }
      System.out.println();
    }
  }

  public static void main(String[] args) {
    testConstructor();
  }
}
