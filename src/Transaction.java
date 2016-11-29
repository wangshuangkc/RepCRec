import java.util.ArrayList;
import java.util.List;

/**
 * Created by kc on 11/29/16.
 */
public class Transaction {
  private final int _number;
  private final long _startTimestamp;
  private final boolean _readOnly;
  private final List<Operation> _operations;
  private long _endTimeStamp;

  public Transaction(int number, long timestamp, boolean readOnly) {
    _number = number;
    _startTimestamp = timestamp;
    _readOnly = readOnly;
    _operations = new ArrayList<>();
  }

  public void commit(long endTime) {
    _endTimeStamp = endTime;
  }

  public void addOperation(Operation op) {
    _operations.add(op);
  }
}
