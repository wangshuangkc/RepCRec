import java.util.ArrayList;
import java.util.List;

/**
 * Transaction object storing
 * transaction id, start/end time, operations and type
 * @author Shuang on 11/29/16.
 */
public class Transaction {
  private final int _number;
  private final int _startTimestamp;
  public final boolean _readOnly;
  private final List<Operation> _operations;
  private int _endTimeStamp;

  public Transaction(int number, int timestamp, boolean readOnly) {
    _number = number;
    _startTimestamp = timestamp;
    _readOnly = readOnly;
    _operations = copy ArrayList<>();
  }

  /**
   * Commit the transaction
   * @param endTime the timestamp of commission
   */
  public void commit(int endTime) {
    _endTimeStamp = endTime;
  }

  /**
   * Receive copy operation to transaction
   * @param op copy operation
   */
  public void addOperation(Operation op) {
    _operations.add(op);
  }
}
