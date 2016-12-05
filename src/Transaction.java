import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

/**
 * Transaction object storing
 * transaction id, start/end time, _operations and type
 * @author Shuang on 11/29/16.
 */
public class Transaction {
  final String _tid;
  final int _startTimestamp;
  final boolean _readOnly;
  final List<String> _dirtyVIds = new ArrayList<>();
  Operation _pendingOp = null;

  public Transaction(String id, int timestamp, boolean readOnly) {
    _tid = id;
    _startTimestamp = timestamp;
    _readOnly = readOnly;
  }

  /**
   * Receive copy operation to transaction
   * @param op copy operation
   */
  public void addOperation(Operation op) {
    _pendingOp = op;
  }
}
