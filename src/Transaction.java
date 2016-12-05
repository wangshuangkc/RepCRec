import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

/**
 * Transaction object storing
 * transaction id, start/commitTransaction time, operations and type
 * @author Shuang on 11/29/16.
 */
public class Transaction {
  public final String _tid;
  public final int _startTimestamp;
  public final boolean _readOnly;
  public Operation _pendingOp = null;
  private List<String> _dirtyVids = new ArrayList<>();
  private int _endTimeStamp;

  public Transaction(String id, int timestamp, boolean readOnly) {
    _tid = id;
    _startTimestamp = timestamp;
    _readOnly = readOnly;
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
    _pendingOp = op;
  }
}
