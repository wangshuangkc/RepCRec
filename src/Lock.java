import java.util.Objects;

/**
 * Lock object storing:
 * lock type (Read or Write), holder transaction Id, variable id
 * @author Shuang on 11/29/16.
 */
public class Lock {
  final LockType _type;
  final String _transactionId;
  final String _variableId;

  public Lock(LockType type, String transactionId, String variableId) {
    _type = type;
    _transactionId = transactionId;
    _variableId = variableId;
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

/**
 * Lock type: Read Lock, Write Lock
 *
 * @author Shuang
 */
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