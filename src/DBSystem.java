import java.util.ArrayList;
import java.util.List;
import java.io.*;

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
        tid = tid.replaceAll("\\s+","");
        System.out.println("begin transaction id is " + tid);
        boolean readOnly = false;
        if(ope.contains("beginRO")) readOnly = true;
        _tm.begin(tid, _timestamp++, readOnly);
      } else if(ope.contains("R")) {
        int split = ope.indexOf(",");
        String tid = ope.substring(ope.indexOf("(")+1, split);
        tid = tid.replaceAll("\\s+","");
        String vid = ope.substring(split+1, ope.indexOf(")"));
        vid = vid.replaceAll("\\s+","");
        _tm.read(tid, vid);
        System.out.println("read transaction id is " + tid);
        System.out.println("read variable id is " + vid);
      } else if(ope.contains("W")) {
        int first = ope.indexOf(",");
        String tid = ope.substring(ope.indexOf("(")+1, first);
        tid = tid.replaceAll("\\s+","");
        int second = ope.indexOf(",", first+1);
        String vid = ope.substring(first+1, second);
        vid = vid.replaceAll("\\s+","");
        String num = ope.substring(second+1, ope.indexOf(")")).replace("\\s+","");
        int val = Integer.valueOf(num);
        _tm.write(tid, vid, val);
        System.out.println("write transaction id is " + tid);
        System.out.println("write variable " + vid + " with value " + val);
      } else if(ope.contains("dump")) {
        //_tm.dump(...)
      } else if(ope.contains("fail")) {
        String num = ope.substring(ope.indexOf("(")+1, ope.indexOf(")")).replace("\\s+","");
        int sid = Integer.valueOf(num);
        failSite(sid);
      } else if(ope.contains("recover")) {
        String num = ope.substring(ope.indexOf("(")+1, ope.indexOf(")")).replace("\\s+","");
        int sid = Integer.valueOf(num);
        recoverSite(sid);
      } else if(ope.contains("end")) {
        String tid = ope.substring(ope.indexOf("(")+1, ope.indexOf(")"));
        tid = tid.replaceAll("\\s+","");
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
}
