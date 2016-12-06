import java.util.*;

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
  final Map<Integer, Integer> _touchSiteTime = new HashMap<>();

  public Transaction(String id, int timestamp, boolean readOnly) {
    _tid = id;
    _startTimestamp = timestamp;
    _readOnly = readOnly;
  }

  /**
   * Receive copy operation to transaction
   * @param op copy operation
   *
   * @author Shuang
   */
  public void addOperation(Operation op) {
    _pendingOp = op;
  }

  public void addTouchedSite(int sid, int timestamp) {
    if (!_touchSiteTime.containsKey(sid)) {
      _touchSiteTime.put(sid, timestamp);
    }
  }

  /**
   * Return when the site is touched first
   * @param sid site id
   * @return the timestamp the site is first touched; -1 if never touched the site
   *
   * @author Shuang
   */
  public int whenTouchSite(int sid) {
    if (!_touchSiteTime.containsKey(sid)) {
      return -1;
    }

    return _touchSiteTime.get(sid);
  }
}
