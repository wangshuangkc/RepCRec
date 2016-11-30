import java.util.Objects;

/**
 * Lock object storing:
 * lock type (Read or Write), holder transaction Id, variable id
 * @author  on 11/29/16.
 */
public class Lock {
  private final LockType _type;
  private final int _transactionId;
  private final int _variableId;
  private boolean shared;

  public Lock(LockType type, int transaction, int variable) {
    _type = type;
    _transactionId = transaction;
    _variableId = variable;
    shared = _type == LockType.RL;
  }

  /**
   * Check if the variable has a shared lock and can be assigned another lock
   * @param lock another lock attempting to access this
   * @return true if both locks are Read Lock, false if either is Write Lock.
   */
  public boolean canShare(Lock lock) {
    return shared && lock.shared;
  }

  @Override
  public boolean equals(Object ob) {
    if (ob == this) {
      return true;
    }

    if (!(ob instanceof Lock)) {
      return false;
    }

    Lock l = (Lock) ob;
    return _type == l._type
        && _transactionId == l._transactionId
        && _variableId == l._variableId;
  }

  @Override
  public int hashCode() {
    return Objects.hash(_type, _transactionId, _variableId);
  }
}

enum LockType {
  RL("READ_LOCK"),
  WL("WRITE_LOCK");

  private final String _type;

  LockType(String type) {
    _type = type;
  }

  @Override
  public String toString() {
    return _type;
  }
}