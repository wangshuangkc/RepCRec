import java.util.*;

/**
 * Site Object storing:
 * site Id, variable map, status (if failed), and
 * locks table with variable id as the key (each variables can hold several Read Locks but only one Write Lock)
 *
 * @author Shuang on 11/29/16.
 */
public class Site {
  private final int _sid;
  private boolean _failed = false;
  private Map<String, Variable> _variables = new HashMap<>();
  private Map<String, List<Lock>> _lockTable = new HashMap<>();

  public Site(int id) {
    _sid = id;
  }

  /**
   * Fail the site for test purpose, and the site is no longer accessible
   */
  public void fail() {
    _failed = true;
    _lockTable.clear();
  }

  /**
   * Recover the site, and the site is accessible
   */
  public void recover() {
    _failed = false;
    updateData();
  }

  private void updateData() {
    //todo catch up with variable data from other site
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
