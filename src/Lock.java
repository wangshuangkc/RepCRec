import java.util.Objects;

/**
 * Created by kc on 11/29/16.
 */
public class Lock {
  private final LockType _type;
  private final int _transactionId;
  private final int _variableId;

  public Lock(LockType type, int transaction, int variable) {
    _type = type;
    _transactionId = transaction;
    _variableId = variable;
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