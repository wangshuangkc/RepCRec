import java.util.*;

/**
 * Created by kc on 11/29/16.
 */
public class Site {
  private final int _sid;
  private boolean _failed = false;
  private Map<Integer, Integer> _variables = new HashMap<>();
  private Map<Integer, ArrayList<Lock>> _lockTable = new HashMap<>();

  public Site(int number) {
    _sid = number;
  }

  public void fail() {
    _failed = true;
    _lockTable.clear();
  }

  public void recover() {
    _failed = false;
  }

  public boolean isFailed() {
    return _failed;
  }

  public void addVariable(int variable) {
    if (!_variables.containsKey(variable)) {
      _variables.put(variable, 0);
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
