import java.util.*;

/**
 * TransactionManager object
 * @author Shuang on 11/29/16.
 */
public class TransactionManager {
  private Map<String, Transaction> _transactions = new HashMap<>();
  private List<String> _waitingList = new ArrayList<>();
  private List<String> _abortList = new ArrayList<>();
  private final DBSystem _dbs;
  private Map<String, List<String> > _waitForGraph = new HashMap<>();

  public TransactionManager (DBSystem dbs) {
    _dbs = dbs;
  }


  /**
   * Begin a transaction, setting the id, start time, and if Read-Only
   * @param tid the id of the transaction
   *
   * @author Shuang
   */
  public void begin(String tid, int timestamp, boolean readOnly) {
    Transaction t= new Transaction(tid, timestamp, readOnly);
    if (_transactions.containsKey(tid)) {
      System.out.println("Error: " + tid + " has already begun.");
      return;
    }

    _transactions.put(tid, t);
  }

  /**
   * Transaction attempts to read variable
   * Won't read when the transaction is missing, aborted, or waiting.
   *
   * @param tid transaction id
   * @param vid variable id
   *
   * @author Shuang
   */
  public void read(String tid, String vid) {
    if (_abortList.contains(tid)) {
      System.out.println("Error: cannot read " + vid + " because " + tid  + " is aborted.");
      return;
    }

    if (_waitingList.contains(tid)) {
      System.out.println("Error: cannot read " + vid + " because " + tid  + " is waiting.");
      return;
    }

    Transaction t = _transactions.get(tid);
    if (t == null) {
      System.out.println("Error: " + tid + " did not begin.");

      return;
    }

    Site site = selectSite(vid);
    if (site == null) {
      Operation op = new Operation(OperationType.R, vid);
      t.addOperation(op);
      _waitingList.add(tid);
      return;
    }

    if (t._readOnly) {
      int value = site.getVariable(vid).readOnly(t._startTimestamp);
      System.out.println(tid + " reads " + vid + " on Site" + site._sid + ": " + value);

      return;
    }

    boolean canRead = site.RLockVariable(tid, vid, _waitForGraph);
    if (canRead) {
      int value = site.getVariable(vid).read();
      System.out.println(tid + " reads " + vid + " on site " + site._sid + ": " + value);
    } else {
      Operation op = new Operation(OperationType.R, vid);
      t.addOperation(op);
      _waitingList.add(tid);
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
   *
   * @author Yuchang
   */
  public void write(String tid, String vid, int val) {
    if (_abortList.contains(tid)) {
      System.out.println("Error: cannot write " + vid + " because " + tid  + " is aborted.");
      return;
    }

    if (_waitingList.contains(tid)) {
      System.out.println("Error: cannot write " + vid + " because " + tid  + " is waiting.");
      return;
    }

    Transaction t = _transactions.get(tid);
    if (t == null) {
      System.out.println("Error: " + tid + " did not begin.");

      return;
    }

    if(canWrite(tid, vid)) {
      //write on all sites
      for(Site s: _dbs._sites) {
        if(s.getVariable(vid) == null) continue;
        s.writeOnSite(vid, val);
      }
    } else {
      Operation op = new Operation(OperationType.W, vid, val);
      t.addOperation(op);
      _waitingList.add(tid);
    }
  }

  private boolean canWrite(String tid, String vid) {
    boolean res = true;
    int numOfSites = _dbs._sites.size(), count = 0;
    for(Site s: _dbs._sites) {
      if(s.isFailed() || s.getVariable(vid)==null) {
        count++;
        continue;
      }
      if(s._lockTable.containsKey(vid)) {
        Lock lock = s._lockTable.get(vid).get(0);  // current lock for vid
        // if current lock for vid is not tid, it's lock by other, cannot write
        if(!lock._transactionId.equals(tid)) {
          res = false;           //should return false
          s._lockTable.get(vid).add(new Lock(LockType.WL, tid, vid));
        } else {
          if(lock._type.equals(LockType.RL)) {
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
    if(count == numOfSites) res = false; //all the sites fail or no working sites has var
    return res;
  }


  /**
   * DeadLock detect when transaction need to be added into wait list
   * Run the detector when Ti is going to be added to the wailist while it is already in
   * @param tran given transaction id
   * @author Yuchang
   */
  public void detectDeadLock(Transaction tran) {
    String tid = tran._tid;
    List<String> cycle = new ArrayList<>();
    if (!_waitingList.contains(tid)) {
      _waitingList.add(tid);
    } else {
      System.out.println("Dead Lock detected!");
      cycle = getCycle(tid);
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
  }

  private List<String> getCycle(String start) {
    List<String> cycle = new ArrayList<>();
    dfs(start, cycle);
    
    return cycle;
  }

  private void dfs(String current, List<String> cycle) {
    if (_waitForGraph.size() == 0 || !_waitForGraph.containsKey(current)) {
      return;
    }
    
    for (String tid : _waitForGraph.get(current)) {
      if (!cycle.contains(tid)) {
        cycle.add(tid);
        dfs(tid, cycle);
        cycle.remove(tid);
      }
    }
  }

  private void abortTransaction(Transaction abortOne) {
    for(Site site: _dbs._sites) {
      site.releaseLocks(abortOne);
    }

    _waitingList.remove(abortOne);

    //update the _waitForGraph, clear all t waiting for abort t, remove abort t from waitlist if any t has it, by Yuchang
    for(String tid: _waitForGraph.keySet()) {
      if(tid == abortOne._tid) _waitForGraph.get(tid).clear();
      else {
        for (String child : _waitForGraph.get(tid)) {
          if (child == abortOne._tid) _waitForGraph.get(tid).remove(child);
        }
      }
    }
  }

  /**
   * end operation
   * end means commit the value of a variable
   * @param tid given transaction id
   * @param  timestamp current time
   * @author Yuchang
   */
  public void end(String tid, int timestamp) {
    _transactions.get(tid).commit(timestamp);
    for(Site s: _dbs._sites) {
      s.commitValue(tid, timestamp);
    }
    abortTransaction(_transactions.get(tid));
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
    List<String> l1 = new ArrayList<>();
    l1.add("T3");
    tm._waitForGraph.put("T1", l1);
    List<String> l2 = new ArrayList<>();
    l2.add("T1");
    tm._waitForGraph.put("T2", l2);
    List<String> l3 = new ArrayList<>();
    l3.add("T2");
    tm._waitForGraph.put("T3", l3);
    tm.detectDeadLock(tm._transactions.get("T1"));
  }
}
