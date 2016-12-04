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

    Site site = selectSite(vid.charAt(1));
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

    List<Lock> locks = site._lockTable.get(vid);
    if (locks.isEmpty()) {
      locks.add(new Lock(LockType.RL, tid, vid));
      int value = site.getVariable(vid).read();
      System.out.println(tid + " reads " + vid + " on Site" + site._sid + ": " + value);
    } else {
      boolean canRead = true;
      List<String> waited = new ArrayList<>();
      List<Integer> offsets = new ArrayList<>();
      for (Lock l : locks) {
        if ((canRead || waited.isEmpty()) && l._transactionId.equals(tid)) {
          int value = site.getVariable(vid).read();
          System.out.println(tid + " reads " + vid + " on Site" + site._sid + ": " + value);
        } else if (l._type == LockType.RL) {
          waited.add(l._transactionId);
          if (!canRead) {
            List<String> tmp = new ArrayList<>();
            for (int i : offsets) {
              tmp.add(waited.get(i));
            }
            _waitForGraph.put(l._transactionId, tmp);
          }
        } else {
          _waitForGraph.put(l._transactionId, waited);
          waited.add(l._transactionId);
          offsets.add(waited.size() - 1);
          canRead = false;
        }
      }
      if (canRead) {
        locks.add(new Lock(LockType.RL, tid, vid));
        int value = site.getVariable(vid).read();
        System.out.println(tid + " reads " + vid + " on Site" + site._sid + ": " + value);
      } else {
        locks.add(new Lock(LockType.RL, tid, vid));
        Operation op = new Operation(OperationType.R, vid);
        t.addOperation(op);
        _waitingList.add(tid);
      }
    }
  }

  private Site selectSite(int vid) {
    Site s = null;
    if (vid % 2 == 1) {
      int sid = 1 + vid % _dbs.NUM_SITE;
      s = _dbs._sites.get(sid - 1);
      if (s.isFailed()) {
        System.out.println("Site" + sid + " has failed.");
        return null;
      }
      return s;
    }

    Random random = new Random();
    int sidx = random.nextInt(_dbs.NUM_SITE);
    int cnt = sidx + 1;
    while (s == null && cnt != sidx) {
      s = _dbs._sites.get(cnt);
      cnt = (cnt + 1) % (_dbs.NUM_SITE - 1);
    }

    if (s == null) {
      System.out.println("All sites have failed.");
    }

    return s;
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
      if(tid.equals(abortOne._tid)) _waitForGraph.get(tid).clear();
      else {
        for (String child : _waitForGraph.get(tid)) {
          if (child.equals(abortOne._tid)) _waitForGraph.get(tid).remove(child);
        }
      }
    }
  }

  public static void main(String[] args) {
    /*
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
    */

    Random random = new Random();
    for (int i = 0; i < 20; i++) {
      System.out.println(random.nextInt(10));
    }
  }
}
