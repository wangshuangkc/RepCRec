/**
 * Variable objects including information:
 * variable id, current value, last committed value and the current lock status
 *
 * @author Shuang on 11/29/16.
 */
public class Variable {
  public final int _vid;
  private int _value = 0;
  private int _lastCommitedValue = 0;
  private Lock _lock = null;

  public Variable(int id) {
    _vid = id;
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
   * Write the value
   * @param value the new value
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
  public int commit() {
    _lastCommitedValue = _value;

    return _lastCommitedValue;
  }

  /**
   * Read the last commited value
   * @return the last commited value
   */
  public int readCommittedValue() {
    return _lastCommitedValue;
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
