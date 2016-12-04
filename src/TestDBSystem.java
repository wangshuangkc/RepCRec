import java.io.IOException;

/**
 * Created by kc on 12/4/16.
 */
public class TestDBSystem {

  public static void testConstructor() throws IOException {
    System.out.println("Test Constructor");
    DBSystem test = new DBSystem();
    test.readInputFile("test1.txt");
  }

  public static void main(String[] args) throws IOException {
    testConstructor();
  }
}
