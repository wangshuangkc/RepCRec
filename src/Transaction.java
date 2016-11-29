import java.util.ArrayList;
import java.util.List;

/**
 * Created by kc on 11/29/16.
 */
public class Transaction {
  private final int _number;
  private final long _timestamp;
  private final boolean _readOnly;
  private List<Operation> _operations;

  public Transaction(int number, long timestamp, boolean readOnly) {
    _number = number;
    _timestamp = timestamp;
    _readOnly = readOnly;
    _operations = new ArrayList<>();
  }

  public void addOperation(Operation op) {
    _operations.add(op);
  }
}
