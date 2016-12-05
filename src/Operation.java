/**
 * Operation object storing:
 * operation type (Read or Write), the starting timestamp, the variable Id
 * @author Shuang on 11/29/16.
 */
public class Operation {
  public final OperationType _type;
  public final String _variableId;
  private int _value;

  /**
   * Constructor for Read
   * @param type Read
   * @param variable variable Id
   */
  public Operation(OperationType type, String variable) {
    _type = type;
    _variableId = variable;
  }

  /**
   * Constructor for Write
   * @param type Write
   * @param variable variable Id
   * @param value the writing value
   * @throws IllegalArgumentException if the type is not Write
   */
  public Operation(OperationType type, String variable, int value) {
    if (type != OperationType.W) {
      throw new IllegalArgumentException("Error: operation should be WRITE");
    }

    _type = type;
    _variableId = variable;
    _value = value;
  }

  /**
   * return parivate write operation value
   * when re-execute the write operation in waiting list
   * @author Yuchang
   */
  public int readValue() {
    return _value;
  }
}

enum OperationType {
  R ("READ"),
  W ("WRITE");

  private final String _type;

  OperationType(String type) {
    _type = type;
  }

  @Override
  public String toString() {
    return _type;
  }

  public static void main(String[] args) {
    OperationType t1 = OperationType.R;
    OperationType t2 = OperationType.R;
    OperationType t3 = OperationType.W;

    System.out.println(t1 == t2);
    System.out.println(t1 == t3);
  }
}