/**
 * Created by kc on 11/29/16.
 */
public class Read extends Operation {
  public Read(int _variable) {
    super(_variable);
  }

  private int selectSite(int variable) {
    if (variable % 2 == 0) {
      return -1;
    }
    return 1 + variable % 10;
  }
}
