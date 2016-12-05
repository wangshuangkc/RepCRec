import java.util.*;

/**
 * TransactionManager object
 *
 * @author Shuang on 11/29/16.
 */
public class TransactionManager {
  private Map<String, Transaction> _transactions = new HashMap<>();
  private List<String> _waitingList = new ArrayList<>();
  private List<String> _abortList = new ArrayList<>();
  private final DBSystem _dbs;
  private Map<String, Set<String>> _waitForGraph = new HashMap<>();
  //private Map<String, List<String>> _waitForGraph = new HashMap<>();
  private List<String> cycle = new ArrayList<>();

  public TransactionManager(DBSystem dbs) {
    _dbs = dbs;
  }

  /**
   * Begin a transaction, setting the id, start time, and if Read-Only
   *
   * @param tid the id of the transaction
   * @author Shuang
   */
  public void begin(String tid, int timestamp, boolean readOnly) {
    Transaction t = new Transaction(tid, timestamp, readOnly);
    if (_transactions.containsKey(tid)) {
      System.out.println("Error: " + tid + " has already begun.");
      return;
    }
    _transactions.put(tid, t);
    _dbs.printVerbose(tid + " begins");
  }

  /**
   * Transaction attempts to read variable
   * Won't read when the transaction is missing, aborted, or waiting.
   *
   * @param tid transaction id
   * @param vid variable id
   * @author Shuang
   */
  public void read(String tid, String vid) {
    if (_abortList.contains(tid)) {
      _dbs.printVerbose("Failed: " + tid + " is aborted.");
      return;
    }

    if (_waitingList.contains(tid)) {
      _dbs.printVerbose("Failed: " + tid + " is waiting.");
      return;
    }

    Transaction t = _transactions.get(tid);
    if (t == null) {
      System.out.println("Error: " + tid + " did not begin.");

      return;
    }

    Site site = selectSite(vid);
    if (site == null) {
      _dbs.printVerbose("No available site for accessing " + vid);
      Operation op = new Operation(OperationType.R, vid);
      t.addOperation(op);
      _waitingList.add(tid);
      return;
    }

    if (t._readOnly) {
      int value = site.readVariable(vid, t._startTimestamp);
      System.out.println(tid + " reads " + vid + " at site" + site._sid + ": " + value);

      return;
    }

    boolean canRead = site.RLockVariable(tid, vid, _waitForGraph);
    if (canRead) {
      int value = site.readVariable(vid, false);
      System.out.println(tid + " reads " + vid + " at site " + site._sid + ": " + value);
    } else {
      Operation op = new Operation(OperationType.R, vid);
      t.addOperation(op);
      if (_waitingList.contains(tid)) {
        handleDeadLock(_transactions.get(tid));
      } else {
        _waitingList.add(tid);
      }
    }
  }

  private Site selectSite(String vid) {
    int vIdx = Integer.parseInt(vid.substring(1));
    if (vIdx % 2 == 1) {
      int sid = 1 + vIdx % _dbs.NUM_SITE;
      Site s = _dbs._sites.get(sid - 1);
      if (s.isFailed()) {
        System.out.println("Site" + sid + " has failed.");
        return null;
      }
      return s;
    }

    for (Site s : _dbs._sites) {
      if (!s.isFailed() && s.getVariable(vid).canRead()) {
        return s;
      }
    }

    System.out.println("All sites have failed.");
    return null;
  }

  /**
   * Transaction attempts to write variable
   * Won't write when the transaction is missing, aborted, or waiting.
   *
   * @param tid transaction id
   * @param vid variable id
   * @author Yuchang, Shuang
   */
  public void write(String tid, String vid, int val) {
    if (_abortList.contains(tid)) {
      System.out.println("Error: cannot write " + vid + " because " + tid + " is aborted.");
      return;
    }

    if (_waitingList.contains(tid)) {
      System.out.println("Error: cannot write " + vid + " because " + tid + " is waiting.");
      return;
    }

    if (!_transactions.containsKey(tid)) {
      System.out.println("Error: " + tid + " did not begin.");
      return;
    }
    Transaction t = _transactions.get(tid);

    if (canWrite(tid, vid)) {
      int vidx = Integer.valueOf(vid.substring(1));
      if (vidx % 2 == 1) {
        int sid = 1 + vidx % _dbs.NUM_SITE;
        Site s = _dbs._sites.get(sid - 1);
        s.writeOnSite(vid, val);
        _transactions.get(tid)._dirtyVIds.add(vid);
        _dbs.printVerbose(tid + " writes on " + vid + " at site " + s._sid + ": " + val);
      } else {
        //write on all sites
        for (Site s : _dbs._sites) {
          s.writeOnSite(vid, val);
        }
        _transactions.get(tid)._dirtyVIds.add(vid);
        _dbs.printVerbose(tid + " writes on " + vid + " at all available sites: " + val);
      }
    } else {
      System.out.println(tid + " cannot write currently");
      Operation op = new Operation(OperationType.W, vid, val);
      t.addOperation(op);
      if (isDeadLock("?", tid)) {
        handleDeadLock(_transactions.get(tid));
      } else {
        System.out.println(tid + " has to wait");
        _waitingList.add(tid);
      }
    }
  }

  private boolean canWrite(String tid, String vid) {
    boolean res = true;
    int vidx = Integer.valueOf(vid.substring(1));
    int count = 0;
    List<Site> temp = new ArrayList<>();
    if (vidx % 2 == 1) {
      int sid = 1 + vidx % _dbs.NUM_SITE;
      Site s = _dbs._sites.get(sid - 1);
      temp.add(s);
    } else {
      temp.addAll(_dbs._sites);
    }

    for (Site s : temp) {
      if (s.isFailed()) {
        count++;
        _dbs.printVerbose("cannot write on failed site " + s._sid);
        continue;
      }
      if (s._lockTable.containsKey(vid) && !s._lockTable.get(vid).isEmpty()) {

        Lock lock = s._lockTable.get(vid).get(0);  // current lock for vid
        // if current lock for vid is not tid, it's lock by other, cannot write
        if (!lock._transactionId.equals(tid)) {
          res = false;           //should return false
          List<Lock> locks = s._lockTable.get(vid);
          Set<String> waited = new HashSet<>();
          for (Lock l : locks) {
            waited.add(l._transactionId);
          }
          if (!_waitForGraph.containsKey(tid)) {
            _waitForGraph.put(tid, new HashSet<String>());
          }
          _waitForGraph.get(tid).addAll(waited);
          s._lockTable.get(vid).add(new Lock(LockType.WL, tid, vid));
        } else {
          if (lock._type.equals(LockType.RL)) {
            s._lockTable.get(vid).remove(lock);           // the first lock is by tid, but is read lock, remove it
            s._lockTable.get(vid).add(0, new Lock(LockType.WL, tid, vid));  // replace the read lock with write lock
          }
        }
      } else {
        List<Lock> locks = new ArrayList<>();
        locks.add(new Lock(LockType.WL, tid, vid));     //vid is not locked, add it a write lock
        s._lockTable.put(vid, locks);
      }
    }
    if (count == _dbs.NUM_SITE)  {
      res = false; //all the sites fail or no working sites has var
    }
    return res;
  }

  /**
   * DeadLock detect when transaction need to be added into wait list
   * Run the detector when Ti is going to be added to the wailist while it is already in
   *
   * @param tran given transaction id
   * @author Yuchang
   */
  public void handleDeadLock(Transaction tran) {
    String tid = tran._tid;
    System.out.println("Dead Lock detected!");
    List<String> cycle = getCycle(tid);
    Transaction aborted = tran;
    int ts = -1;
    for (String id : cycle) {
      Transaction t = _transactions.get(id);
      if (t._startTimestamp > ts) {
        ts = t._startTimestamp;
        aborted = t;
      }
    }
    System.out.println("Abort " + aborted._tid + ".");
    abortTransaction(aborted);
  }

  private boolean isDeadLock(String tid, String start) {
    if (tid.equals(start)) return true;
    if (tid.equals("?")) tid = start;
    if (!_waitForGraph.containsKey(tid) || _waitForGraph.get(tid).isEmpty()) return false;
    for (String next : _waitForGraph.get(tid)) {
      if (isDeadLock(next, start)) return true;
    }
    return false;
  }


  private List<String> getCycle(String start) {
    List<String> cycle = new ArrayList<>();
    List<String> res = new ArrayList<>();
    dfs(start, cycle, res);
    return res;
  }

  private void dfs(String current, List<String> cycle, List<String> res) {
    if (_waitForGraph.size() == 0 || !_waitForGraph.containsKey(current)) {
      return;
    }
    for (String tid : _waitForGraph.get(current)) {
      if (!cycle.contains(tid)) {
        cycle.add(tid);
        dfs(tid, cycle, res);
        cycle.remove(tid);
      } else {
        res = new ArrayList<>(cycle);
      }
    }
  }

  private void abortTransaction(Transaction abortOne) {
    for (Site site : _dbs._sites) {
      site.releaseLocks(abortOne);
    }

    _waitingList.remove(abortOne);

    for (String tid : _waitForGraph.keySet()) {
      for (String child : _waitForGraph.get(tid)) {
        if (abortOne._tid.equals(child)) {
          _waitForGraph.get(tid).remove(child);
          break;
        }
      }
    }
    _waitForGraph.remove(abortOne._tid);
    runNextWaiting();
  }

  /**
   * commitTransaction operation
   * commitTransaction means commit the value of a variable
   *
   * @param tid       given transaction id
   * @param timestamp current time
   * @author Yuchang, Shuang
   */
  public void commitTransaction(String tid, int timestamp) {
    Transaction t = _transactions.get(tid);
    boolean canCommit = true;
    for (String dv : t._dirtyVIds) {
      int dvidx = Integer.valueOf(dv.substring(1));
      if (dvidx % 2 == 1) {
        int sid = 1 + dvidx % _dbs.NUM_SITE;
        Site s = _dbs._sites.get(sid - 1);
        if (s.isFailed()) {
          canCommit = false;
          break;
        }
      } else {
        for (Site s : _dbs._sites) {
          if (s.isFailed()) {
            canCommit = false;
            break;
          }
        }
      }
    }

    for (Site s : _dbs._sites) {
      for (String dv : t._dirtyVIds) {
        Variable v = s.getVariable(dv);
        if (v != null) {
          if (canCommit) {
            v.commit(timestamp);
          } else {
            v.revert();
          }
        }
      }
    }

    if (!canCommit) {
      _abortList.add(tid);
      _dbs.printVerbose("abort " + tid);
    }
//    for(Site s: _dbs._sites) {
//      s.commitValue(tid, timestamp);
//    }

    abortTransaction(_transactions.get(tid));
  }

  private void runNextWaiting() {
    if (_waitingList.isEmpty()) {
      //System.out.println("There is no transaction waiting currently");
      return;
    }
    for (int i = 0; i < _waitingList.size(); ) {
      String nextTid = _waitingList.get(i);
      if (_waitForGraph.containsKey(nextTid) && !_waitForGraph.get(nextTid).isEmpty()) {
        System.out.println("Transaction " + nextTid + " still need to wait!");
        break;
      }
      _waitingList.remove(nextTid);
      String nextVid = _transactions.get(nextTid)._pendingOp._variableId;
      if (_transactions.get(nextTid)._pendingOp._type == OperationType.R) {
        read(nextTid, nextVid);
      } else if (_transactions.get(nextTid)._pendingOp._type == OperationType.W) {
        int nextVal = _transactions.get(nextTid)._pendingOp.readValue();
        write(nextTid, nextVid, nextVal);
      }
    }
  }

  public static void main(String[] args) {
    System.out.println("Test deadlock detection");
    DBSystem dbs = new DBSystem();
    TransactionManager tm = new TransactionManager(dbs);
    tm._transactions.put("T1", new Transaction("T1", 1, false));
    tm._transactions.put("T1", new Transaction("T2", 2, false));
    tm._transactions.put("T1", new Transaction("T3", 3, false));
    tm._waitingList.add("T1");
    tm._waitingList.add("T2");
    tm._waitingList.add("T3");
    Set<String> l1 = new HashSet<>();
    l1.add("T3");
    tm._waitForGraph.put("T1", l1);
    Set<String> l2 = new HashSet<>();
    l2.add("T1");
    tm._waitForGraph.put("T2", l2);
    Set<String> l3 = new HashSet<>();
    l3.add("T2");
    tm._waitForGraph.put("T3", l3);
    tm.handleDeadLock(tm._transactions.get("T1"));
  }
}
