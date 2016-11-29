import java.util.*;

/**
 * Created by kc on 11/29/16.
 */
public class Site {
  private final int _number;
  private boolean _failed;
  private Map<Integer, Integer> _lockTable;
  private Map<Integer, Integer> _variables;

  public Site(int number) {
    _number = number;
    _failed = false;
    _lockTable = new HashMap<>();
    _variables = new HashMap<>();
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

  public void addVariable(int variable, int value) {
    _variables.put(variable, value);
  }

  public int readVariable(int variable) {
    checkLock(variable);
    return _variables.get(variable);
  }

  public int writeVariable(int variable, int value) {
    checkLock(variable);
    _variables.put(variable, value);
    return value;
  }
  private void checkLock(int variable) {
  }
}
