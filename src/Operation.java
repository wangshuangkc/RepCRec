/**
 * Operation object storing:
 * operation type (Read or Write), the starting timestamp, the variable Id
 * @author Shuang on 11/29/16.
 */
public class Operation {
  public final OperationType _type;
  public final int _timeStamp;
  public final int _variableId;
  private int _value;

  /**
   * Constructor for Read
   * @param type Read
   * @param timeStamp start time
   * @param variable variable Id
   */
  public Operation(OperationType type, int timeStamp, int variable) {
    _type = type;
    _timeStamp = timeStamp;
    _variableId = variable;
  }

  /**
   * Constructor for Write
   * @param type Write
   * @param timeStamp start time
   * @param variable variable Id
   * @param value the writing value
   * @throws IllegalArgumentException if the type is not Write
   */
  public Operation(OperationType type, int timeStamp, int variable, int value) {
    if (type != OperationType.W) {
      throw new IllegalArgumentException("Error: operation should be WRITE");
    }

    _type = type;
    _timeStamp = timeStamp;
    _variableId = variable;
    _value = value;
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
}