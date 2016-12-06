import java.io.IOException;

/**
 * Created by kc on 12/4/16.
 */
public class TestDBSystem {
  public static void testInput(int i) throws IOException{
    System.out.println("Test Input");
    DBSystem test = new DBSystem();
    test.run("tests/new/test" + i + ".txt");
  }

  public static void main(String[] args) throws IOException {
    testInput(8);
  }
}
