import java.util.*;

/**
 * Variable objects including information:
 * variable id, current value, last committed value and the current lock status
 *
 * @author Shuang on 11/29/16.
 */
public class Variable {
  public final String _vid;
  private int _value;
  private int _lastCommitted;
  private Map<Integer, Integer> _commits = new HashMap<>();
  private boolean _canRead = true;

  public Variable(String id) {
    _vid = id;
    _value = getInitValue(id);
    _commits.put(0, _value);
    _lastCommitted = _value;
  }

  private int getInitValue(String vid) {
    String s = vid.replace("x", "");
    try {
      int i = Integer.valueOf(s);
      return 10 * i;
    } catch (NumberFormatException e) {
      System.out.println("Error: invalid variable name " + vid
          + ". Expected example: x1");
      e.printStackTrace();
    }

    return -1;
  }

  /**
   * Read the value
   * May need to check the lock first
   * @return the value of the variable
   *
   * @author Shuang on 11/29/16
   */
  public int read() {
    return _value;
  }

  /**
   * Read the last committed value before the given timestamp, for Read-Only transaction
   * @param timestamp the given transaction timestamp
   * @return last committed value before the timestamp
   *
   * @author Shuang
   */
  public int readOnly(int timestamp) {
    List<Integer> timestamps = new ArrayList<>(_commits.keySet());
    Collections.sort(timestamps);
    int size = timestamps.size();
    if (timestamp > timestamps.get(size - 1)) {
      return _commits.get(timestamps.get(size - 1));
    }

    int low = 0;
    int high = size - 1;
    int mid;
    while (high - low > 1) {
      mid = (high - low) / 2 + low;
      if (timestamps.get(mid) >= timestamp) {
        high = mid;
      } else {
        low = mid;
      }
    }

    return _commits.get(timestamps.get(low));
  }

  /**
   * Write the value
   * @param value the copy value
   * @return the current value
   *
   * @author Shuang
   */
  public int write(int value) {
    _value = value;

    return read();
  }

  /**
   * Commit the current value
   * @return the last committed/current value
   *
   * @author Shuang
   */
  public int commit(int timestamp) {
    _commits.put(timestamp, _value);
    _lastCommitted = _value;

    return _value;
  }

  /**
   * When a transaction is aborted, revert the current value to the last committed
   *
   * @author Shuang
   */
  public void revert() {
    _value = readLastCommited();
  }

  /**
   * Return the last committed value before the present
   * @return the last committed value
   *
   * @author Shuang
   */
  public int readLastCommited() {
    return _lastCommitted;
  }

  /**
   * Block the variable to be read for Site recovery period
   *
   * @author Shuang
   */
  public void blockRead() {
    _canRead = false;
  }

  /**
   * Check if the variable can be read for Site recovery period
   * @return true if the variable is allowed to read, false otherwise
   *
   * @author Shuang
   */
  public boolean canRead() {
    return _canRead;
  }

  /**
   * Make the variable accessible for Read
   * For replicated variables, when the site recovered, they needs to wait one Write before accessible for Read
   *
   * @author Shuang
   */
  public void recoverVariable() {
    _canRead = true;
  }
}
