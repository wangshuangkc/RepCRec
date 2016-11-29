/**
 * Created by kc on 11/29/16.
 */
public class Operation {
  private final OperationType _type;
  private final long _timeStamp;
  private final int _variableId;
  private int _value;


  public Operation(OperationType type, long timeStamp, int variable) {
    _type = type;
    _timeStamp = timeStamp;
    _variableId = variable;
  }

  public Operation(OperationType type, long timeStamp, int variable, int value) {
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