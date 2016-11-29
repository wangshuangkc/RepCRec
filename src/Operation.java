/**
 * Created by kc on 11/29/16.
 */
public abstract class Operation {
  private final int _variable;
  private int _value;

  public Operation(int variable, int value) {
    _variable = variable;
    _value = value;
  }

  public Operation(int variable) {
    _variable = variable;
  }
}
