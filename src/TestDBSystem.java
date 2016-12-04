import java.util.Map;

/**
 * Created by kc on 12/4/16.
 */
public class TestDBSystem {
  public static void testConstructor() {
    System.out.println("Test Constructor");
    DBSystem test = new DBSystem();
    test.dump();
  }

  public static void testDump() {
    System.out.println("Test Constructor");
    DBSystem test = new DBSystem();
    for (int i = 2; i <= test.NUM_SITE; i += 2) {
      String testVid = "x" + i;
      Variable v = test._sites.get(i - 1).getVariable(testVid);
      v.write(i);
      v.commit(i);
    }
    test.dump();
  }
  public static void main(String[] args) {
    testConstructor();
  }
}
