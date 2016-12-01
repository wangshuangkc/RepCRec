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
  private Lock _lock = null;
  private Map<Integer, Integer> _commits = new HashMap<>();

  public Variable(String id) {
    _vid = id;
    _value = 10 * _vid.charAt(1);
    _commits.put(0, _value);
  }

  /**
   * Read the value
   * May need to check the lock first
   * @return the value of the variable
   * @author Shuang on 11/29/16
   */
  public int read() {
    return _value;
  }

  /**
   * Read the last committed value before the given timestamp, for Read-Only transaction
   * @param timestamp the given transaction timestamp
   * @return last committed value
   * @throws IllegalArgumentException when the timestamp is earlier than 0
   */
  public int readOnly(int timestamp) {
    if (timestamp <= 0) {
      throw new IllegalArgumentException("Invalid timestamp: " + timestamp);
    }
    List<Integer> timestamps = new ArrayList<>(_commits.keySet());
    int size = timestamps.size();
    if (timestamp > timestamps.get(size - 1)) {
      return timestamps.get(size - 1);
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
   */
  public int write(int value) {
    _value = value;

    return read();
  }

  /**
   * Commit the current value
   * @return the last committed/current value
   */
  public int commit(int timestamp) {
    _commits.put(timestamp, _value);

    return _value;
  }

  /**
   * Lock the variable
   * @param lock Read or Write lock
   */
  public boolean lock(Lock lock) {
    if (_lock == null) {
      _lock = lock;
      return true;
    }

    return _lock.canShare(lock);
  }
}
