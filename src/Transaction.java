import java.util.LinkedList;
import java.util.Queue;

/**
 * Transaction object storing
 * transaction id, start/end time, operations and type
 * @author Shuang on 11/29/16.
 */
public class Transaction {
  public final String _tid;
  public final int _startTimestamp;
  public final boolean _readOnly;
  public final Queue<Operation> _pendingOperations;
  private int _endTimeStamp;

  public Transaction(String id, int timestamp, boolean readOnly) {
    _tid = id;
    _startTimestamp = timestamp;
    _readOnly = readOnly;
    _pendingOperations = new LinkedList<>();
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
    _pendingOperations.add(op);
  }
}
