import java.util.*;

/**
 * Site Object storing:
 * site Id, variable map, status (if failed), and
 * locks table with variable id as the key (each variables can hold several Read Locks but only one Write Lock)
 *
 * @author Shuang on 11/29/16.
 */
public class Site {
  final int _sid;
  final Map<String, List<Lock>> _lockTable = new HashMap<>();
  final Map<String, Variable> _variables = new HashMap<>();
  private boolean _failed = false;
  private int _failedTimeStamp = -1;

  public Site(int id) {
    _sid = id;
  }

  /**
   * Fail the site for test purpose, and the site is no longer accessible
   *
   * @author Shuang
   */
  public void fail(int timestamp) {
    _failed = true;
    _failedTimeStamp = timestamp;
    _lockTable.clear();
  }

  /**
   * Recover the site, and the site is accessible
   *
   * @author Shuang
   */
  public void recover() {
    _failed = false;
    for (Map.Entry<String, Variable> e : _variables.entrySet()) {
      String vid = e.getKey().substring(1);
      int id = Integer.valueOf(vid);
      if (id % 2 == 0) {
        e.getValue().blockRead();
      }
    }
  }

  /**
   * Check if the site is accessible
   * @return true if the site is down, false otherwise
   *
   * @author Shuang
   */
  public boolean isFailed() {
    return _failed;
  }

  /**
   * Get the last timestamp when the site failed
   * @return when the site last failed
   *
   * @author Shuang
   */
  public int whenFailed() {
    return _failedTimeStamp;
  }

  /**
   * Add a variable to the Site
   * @param variable id
   *
   * @author Shuang
   */
  public void addVariable(Variable variable) {
    if (!_variables.containsKey(variable)) {
      _variables.put(variable._vid, variable);
    }
  }

  /**
   * Get the variable by id
   * @param vid variable id
   * @return requested variable or null if id not exists
   *
   * @author Shuang
   */
  public Variable getVariable(String vid) {
    if (!_variables.containsKey(vid)) {
      return null;
    }

    return _variables.get(vid);
  }

  /**
   * Add a new lock to the variable
   * @param tid the transaction id
   * @param vid variable id
   * @return True if the variable does not hold any lock or the current lock can be shared with the new lock, false other wise
   * @throws IllegalArgumentException if the variable is not found in the site
   *
   * @author Shuang
   */
  public boolean RLockVariable(String tid, String vid, Map<String, Set<String>> waitForGraph) {
    if (!_variables.containsKey(vid)) {
      throw new IllegalArgumentException("Error: " + vid + " not found in the site " + _sid);
    }

    if (!_lockTable.containsKey(vid)) {
      _lockTable.put(vid, new ArrayList<Lock>());
    }
    List<Lock> locks = _lockTable.get(vid);
    if (locks.isEmpty()) {
      locks.add(new Lock(LockType.RL, tid, vid));
      return true;
    } else {
      boolean canRead = true;
      Set<String> waited = new HashSet<>();
      List<Integer> offsets = new ArrayList<>();
      for (Lock l : locks) {
        if (canRead && l._transactionId.equals(tid)) {
          return true;
        } else if (l._type == LockType.WL) {
          waited.add(l._transactionId);
          canRead = false;
        }
      }
      if (!waitForGraph.containsKey(tid)) {
        waitForGraph.put(tid, new HashSet<String>());
      }
      waitForGraph.get(tid).addAll(waited);
      locks.add(new Lock(LockType.RL, tid, vid));

      return canRead;
    }
  }

  /**
   * Read the current value of the requested variable OR
   * Read the committed value of the requested variable
   * @param vid variable id
   * @param committed if requesting committed value
   * @return variable value
   *
   * @author Shuang
   */
  public int readVariable(String vid, boolean committed) {
    Variable v = getVariable(vid);

    if (committed) {
      return v.readLastCommited();
    }

    return v.read();
  }

  /**
   * Read the committed value of the requested variable before the timestamp
   * @param vid variable id
   * @param timestamp if requesting committed value
   * @return the committed variable value
   *
   * @author Shuang
   */
  public int readVariable(String vid, int timestamp) {
    Variable v = getVariable(vid);

    return v.readOnly(timestamp);
  }

  /**
   * write var on every site it stored
   * @param vid variable id, val value to write
   * @param val written value
   *
   * @author Yuchang
   */
  public void writeOnSite(String vid, int val) {
    if(!_variables.containsKey(vid)) {
      throw new IllegalArgumentException("Error: " + vid + " not found in the site " + _sid);
    }
    Variable var = _variables.get(vid);
    var.write(val);
    if (_failedTimeStamp > -1) {
      var.recoverVariable();
    }
    var.recoverVariable();
    _variables.put(vid, var);
  }


  /**
   * Release the locks from an absorted trasaction in this site when dead lock detected
   * @param t aborted transaction
   *
   * @author Yuchang
   */
  public void releaseLocks(Transaction t) {
    for(String var: _lockTable.keySet()) {
      for(Lock lock: _lockTable.get(var)) {
        if(lock._transactionId.equals(t._tid)) {
          _lockTable.get(var).remove(lock);
          break;
        }
      }
    }
  }

  /**
   * Get a list of variable ids stored in the site
   * @return a sorted array of variable Id
   *
   * @author Shuang
   */
  public List<String> getAllVariableIds() {
    List<String> vids = new ArrayList<>();
    for (String v : _variables.keySet()) {
      vids.add(v);
    }
    Collections.sort(vids, new Comparator<String>() {
      @Override
      public int compare(String o1, String o2) {
        int i1 = Integer.valueOf(o1.substring(1));
        int i2 = Integer.valueOf(o2.substring(1));

        return i1 - i2;
      }
    });

    return vids;
  }

  @Override
  public boolean equals(Object ob) {
    if (ob == this) {
      return true;
    }

    if (!(ob instanceof Site)) {
      return false;
    }

    Site s = (Site) ob;
    return _sid == s._sid;
  }

  @Override
  public int hashCode() {
    return Objects.hash(_sid);
  }
}
