import java.util.ArrayList;
import java.util.List;

/**
 * Created by kc on 12/1/16.
 */
public class DBSystem {
  private static final int NUM_SITE = 10;
  private static final int NUM_VARIABLE = 20;
  final List<Site> _sites;
  final TransactionManager _tm = new TransactionManager(this);

  public DBSystem() {
    _sites = setupSites();
  }

  public void failSite(int sid) {
    Site s = _sites.get(sid);
    s.fail();
    _tm.failSite(s);
  }


  private List<Site> setupSites() {
    List<Variable> variables = setupVariables();

    List<Site> sites = new ArrayList<>();
    for (int i = 1;  i <= NUM_SITE; i++) {
      Site s = new Site(i);
      for (int j = 2; j <= variables.size(); j += 2) {
        s.addVariable(variables.get(j));
      }
      sites.add(s);
    }

    for (int i = 1; i <= variables.size(); i += 2) {
      int sid = 1 + i % NUM_SITE;
      sites.get(sid).addVariable(variables.get(i));
    }

    return sites;
  }

  private List<Variable> setupVariables() {
    List<Variable> vars = new ArrayList<>();
    for (int i = 1; i <= NUM_VARIABLE; i++) {
      String id = "x" + i;
      Variable v = new Variable(id);
      vars.add(v);
    }

    return vars;
  }
}
