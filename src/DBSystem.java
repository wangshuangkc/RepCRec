import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.io.*;
import java.util.Map;

/**
 * Created by kc on 12/1/16.
 */
public class DBSystem {
  static final int NUM_SITE = 10;
  static final int NUM_VARIABLE = 20;
  private int _timestamp = 0;
  final List<Site> _sites;
  final TransactionManager _tm = new TransactionManager(this);
  public final List<String> operations = new ArrayList<>();

  public DBSystem() {
    _sites = setupSites();
  }

  private List<Site> setupSites() {
    List<Variable> variables = setupVariables();

    List<Site> sites = new ArrayList<>();
    for (int i = 1;  i <= NUM_SITE; i++) {
      Site s = new Site(i);
      sites.add(s);
    }

    for (int i = 1; i <= variables.size(); i++) {
      if (i % 2 == 1) {
        int sid = 1 + i % NUM_SITE;
        sites.get(sid - 1).addVariable(copyVariable(variables.get(i - 1)));
      } else {
        for (Site s : sites) {
          s.addVariable(copyVariable(variables.get(i - 1)));
        }
      }
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

  private Variable copyVariable(Variable v) {
    return new Variable(v._vid);
  }

  /**
   * Read operations from the input script
   * @param fileName, input file name including path
   *
   * @author Yuchang
   */
  public void readInputFile(String fileName) throws IOException {
    FileReader fileReader = new FileReader(fileName);
    BufferedReader bufferedReader = new BufferedReader(fileReader);
    String line = null;
    while((line = bufferedReader.readLine()) != null) {
      //System.out.println(line);
      operations.add(line);
    }
    bufferedReader.close();
    runDB();
  }

  private void runDB() {
    for(String ope: operations) {
      //parse the String operation
      if(ope.contains("begin")) {
        String tid = ope.substring(ope.indexOf("(")+1, ope.indexOf(")"));
        System.out.println("begin transaction id is " + tid);
        boolean readOnly = false;
        if(ope.contains("beginRO")) readOnly = true;
        _tm.begin(tid, _timestamp++, readOnly);
      } else if(ope.contains("R")) {
        int split = ope.indexOf(",");
        String tid = ope.substring(ope.indexOf("(")+1, split);
        String vid = ope.substring(split+1, ope.indexOf(")"));
        System.out.println("read transaction id is " + tid);
        System.out.println("read variable id is " + tid);
        _tm.read(tid, vid);
      } else if(ope.contains("W")) {
        int first = ope.indexOf(",");
        String tid = ope.substring(ope.indexOf("(")+1, first);
        int second = ope.indexOf(",", first);
        String vid = ope.substring(first+1, second);
        int val = Integer.parseInt(ope.substring(second+1, ope.indexOf(")")));
        _tm.write(tid, vid, val);
      } else if(ope.contains("dump")) {
        //_tm.dump(...)
      } else if(ope.contains("fail")) {
        int sid = Integer.parseInt(ope.substring(ope.indexOf("(")+1, ope.indexOf(")")));
        failSite(sid);
      } else if(ope.contains("recover")) {
        int sid = Integer.parseInt(ope.substring(ope.indexOf("(")+1, ope.indexOf(")")));
        recoverSite(sid);
      } else if(ope.contains("end")) {
        String tid = ope.substring(ope.indexOf("(")+1, ope.indexOf(")"));
        _tm.end(tid, _timestamp);
      }

    }
  }


  /**
   * Fail the site when get fail(sid) from the input
   * @param sid
   *
   * @author Shuang
   */
  public void failSite(int sid) {
    Site s = _sites.get(sid);
    s.fail();
  }

  /**
   * Recover the site when get recover(sid) from the input
   * @param sid
   *
   * @author Shuang
   */
  public void recoverSite(int sid) {
    Site s = _sites.get(sid);
    s.recover();
  }

  public void dump() {
    StringBuffer sb = new StringBuffer();
    for (int i = 1; i <= NUM_VARIABLE; i++) {
      Map<Integer, List<Integer>> values = new HashMap<>();
      String vid = "x" + i;
      if (i % 2 == 1) {
        int sid = 1 + i % NUM_SITE;
        Site s = _sites.get(sid - 1);
        int value = s.getVariable(vid).readLastCommited();
        String out = vid + ": " + value + " at site " + sid + "|\n";
        sb.append(out);
      } else {
        for (Site s : _sites) {
          int value = s.getVariable(vid).readLastCommited();
          if (!values.containsKey(value)) {
            values.put(value, new ArrayList<Integer>());
          }
          List<Integer> siteOfValue = values.get(value);
          siteOfValue.add(s._sid);
        }
        sb.append(vid + ": ");
        for (int v : values.keySet()) {
          if (values.size() == 1) {
            sb.append(v + " at all sites");
          } else {
            sb.append(v + " at site ");
            for (int s : values.get(v)) {
              sb.append(s + " ");
            }
          }
          sb.append("| ");
        }
        sb.append("\n");
      }
    }
    System.out.println(sb.toString());
  }
}
