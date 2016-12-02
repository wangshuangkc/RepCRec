import java.util.*;

/**
 * TransactionManager object
 * @author Shuang on 11/29/16.
 */
public class TransactionManager {
  private int _timestamp = 0;
  private Map<Integer, Transaction> _transactions = new HashMap<>();
  private List<Integer> _waitingList = new ArrayList<>();
  private List<Integer> _abortList = new ArrayList<>();
  private List<Site> _runningSites;

  public TransactionManager (DBSystem dbs) {
    _runningSites = dbs._sites;
  }

  /**
   * Begin a Read-Write transaction, setting the id, start time
   * @param tid the id of the transaction
   */
  public void begin(int tid) {
    Transaction t= new Transaction(tid, _timestamp++, false);
    if (_transactions.containsKey(tid)) {
      System.out.println("Error: the transaction T" + tid + " has already begun.");
      return;
    }
    _transactions.put(tid, t);
  }

  /**
   * Begin a Read-Only transaction, setting the id, start time
   * @param tid the id of the transaction
   */
  public void beginRO(int tid) {
    Transaction t = new Transaction(tid, _timestamp++, true);
    if (_transactions.containsKey(tid)) {
      System.out.println("Error: the transaction T" + tid + " has already begun.");
      return;
    }

    _transactions.put(tid, t);
  }

  public boolean read(int tid, int vid) {
    if (_abortList.contains(tid)) {
      System.out.println("Error: cannot read X" + vid + " because T" + tid  + " is borted.");
      return false;
    }

    Transaction t = _transactions.get(tid);
    if (t == null) {
      System.out.println("Error: T" + tid + " did not begin.");
      return false;
    }

    if (t._readOnly) {
      int value = getValue(vid);
    }
    return false;
  }

  private int getValue(int vid) {
    return 0;
  }

  /**
   * Remove failed site from the accessible site list
   * @param site failed site
   */
  public void failSite(Site site) {
    _runningSites.remove(site);
  }

  /**
   * Add the recovered site to the accessible site list
   * @param site recoved site
   */
  public void recoverSite(Site site) {
    _runningSites.add(site);
  }

  // DeadLock detect when transaction need to be added into wait list, by Yuchang
  public void detectDeadLock(Transaction t) {
    if(_waitingList.contains(t) && _waitingList.size() > 1) {
      System.out.println("Dead Lock Detected!");
      // find the transaction in wait list which timestamp is smallest
      Transaction abortOne = t;
      int ts = 0;
      for(int id: _waitingList) {
        if(_transactions.get(id)._startTimestamp > ts) {
          ts = _transactions.get(id)._startTimestamp;
          abortOne =  _transactions.get(id);
        }
      }
      abortTransaction(abortOne);
    }
  }

  // absort transaction which is youngest, by Yuchang
  public void abortTransaction(Transaction abortOne) {
    for(Site site: _runningSites) {
      site.releaseLocks(abortOne);
    }
    //remove t from wait list
    _waitingList.remove(abortOne);
  }
}
