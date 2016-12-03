import java.util.*;
import java.util.Map;
/**
 * Site Object storing:
 * site Id, variable map, status (if failed), and
 * locks table with variable id as the key (each variables can hold several Read Locks but only one Write Lock)
 *
 * @author Shuang on 11/29/16.
 */
public class Site {
  public final int _sid;
  public final Map<String, List<Lock>> _lockTable = new HashMap<>();
  private Map<String, Variable> _variables = new HashMap<>();
  private boolean _failed = false;

  public Site(int id) {
    _sid = id;
  }

  /**
   * Fail the site for test purpose, and the site is no longer accessible
   *
   * @author Shuang
   */
  public void fail() {
    _failed = true;
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
      e.getValue().blockRead();
    }
  }

  /**
   * Check if the site is accessible
   * @return true if the site is down, false otherwise
   */
  public boolean isFailed() {
    return _failed;
  }

  /**
   * Add a variable to the Site
   * @param variable id
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
   */
  public Variable getVariable(String vid) {
    return _variables.get(vid);
  }

  /**
   * Add a new lock to the variable
   * @param lock
   * @param variable id
   * @return true if the variable does not hold any lock or the current lock can be shared with the new lock, false other wise
   * @throws IllegalArgumentException if the variable is not found in the site
   */
  public boolean lockVariable(Lock lock, String variable) {
    if (!_variables.containsKey(variable)) {
      throw new IllegalArgumentException("Error: variabld #" + variable + " not found in the Site #" + _sid);
    }

    Variable var = _variables.get(variable);
    List<Lock> locks = _lockTable.get(variable);
    locks.add(lock);
    _lockTable.put(variable, locks);

    return var.lock(lock);
  }

  // release the locks from an absorted trasaction in this site when dead lock detected, by Yuchang
  public void releaseLocks(Transaction t) {
    for(String var: _lockTable.keySet()) {
      for(Lock lock: _lockTable.get(var)) {
        if(lock._transactionId.equals(t._tid)) {
          _lockTable.get(var).remove(lock);
        }
      }
    }
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
