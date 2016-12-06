import java.util.*;
import java.io.*;

/**
 * DBSystem object for storing sites, raise TM events, and display status
 *
 * @author Shuang
 */
public class DBSystem {
  static final int NUM_SITE = 10;
  static final int NUM_VARIABLE = 20;
  int _timestamp = 0;
  final List<Site> _sites;
  final TransactionManager _tm = new TransactionManager(this);
  private boolean verbose = true;

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
   * Read each event from the input script, and operate the event
   * @param fileName, input file name including path
   *
   * @author Yuchang, Shuang
   */
  public void run(String fileName) {
    try {
      FileReader fileReader = new FileReader(fileName);
      BufferedReader br = new BufferedReader(fileReader);
      printVerbose("Read input from " + fileName);
      String line;
      while ((line = br.readLine()) != null) {
        String[] events = line.split(";");
        for (String event : events) {
          if (event == null || event.trim().isEmpty() || event.startsWith("//")) {
            continue;
          }
          if (event.contains("//")) {
            event = event.substring(0, event.indexOf("//"));
          }
          _timestamp++;
          runCommand(event);
        }
      }
      br.close();
    } catch (Exception e) {
      System.out.println("Error: " + e.getMessage());
    }
  }

  private void runCommand(String command) {
    String ope = command.replaceAll("\\s+", "");
    printVerbose(ope);
    if(ope.contains("begin")) {
      String tid = ope.substring(ope.indexOf("(")+1, ope.indexOf(")"));
      boolean readOnly = false;
      if(ope.contains("beginRO")) readOnly = true;
      _tm.begin(tid, _timestamp, readOnly);
    } else if(ope.contains("R")) {
      int split = ope.indexOf(",");
      String tid = ope.substring(ope.indexOf("(")+1, split);
      String vid = ope.substring(split+1, ope.indexOf(")"));
      _tm.read(tid, vid);
    } else if(ope.contains("W")) {
      int first = ope.indexOf(",");
      String tid = ope.substring(ope.indexOf("(")+1, first);
      int second = ope.indexOf(",", first + 1);
      String vid = ope.substring(first+1, second);
      int val = Integer.parseInt(ope.substring(second+1, ope.indexOf(")")));
      _tm.write(tid, vid, val);
    } else if(ope.contains("dump")) {
      if (ope.contains("()")) {
        dump();
      } else if (ope.contains("x")) {
        String vid = ope.substring(ope.indexOf("x") + 1, ope.indexOf(")"));
        dump(vid);
      } else {
        String sid = ope.substring(ope.indexOf("(") + 1, ope.indexOf(")"));
        dump(Integer.valueOf(sid));
      }
    } else if(ope.contains("fail")) {
      int sid = Integer.parseInt(ope.substring(ope.indexOf("(")+1, ope.indexOf(")")));
      failSite(sid, _timestamp);
    } else if(ope.contains("recover")) {
      int sid = Integer.parseInt(ope.substring(ope.indexOf("(")+1, ope.indexOf(")")));
      recoverSite(sid);
    } else if(ope.contains("end")) {
      String tid = ope.substring(ope.indexOf("(")+1, ope.indexOf(")"));
      _tm.commitTransaction(tid, _timestamp);
    }
  }

  /**
   * Fail the site when get fail(sid) from the input
   * @param sid
   *
   * @author Shuang
   */
  public void failSite(int sid, int timestamp) {
    System.out.println("site " + sid + " fails");

    Site s = _sites.get(sid - 1);
    Set<String> abortedTids = new HashSet<>();
    for (Map.Entry<String, List<Lock>> locks : s._lockTable.entrySet()) {
      for (Lock l : locks.getValue()) {
        String tid = l._transactionId;
        abortedTids.add(tid);
      }
    }
    for (String tid : abortedTids) {
      _tm.abortTransaction(tid);
      System.out.println("abort " + tid + " because site " + sid + " fails");
    }

    s.fail(timestamp);
  }

  /**
   * Recover the site when get recover(sid) from the input
   * @param sid
   *
   * @author Shuang
   */
  public void recoverSite(int sid) {
    Site s = _sites.get(sid - 1);
    s.recover();
    System.out.println("site " + sid + " recovers");
  }

  /**
   * Display all values of all copies of all at all sites
   *
   * @author Shuang
   */
  public void dump() {
    for (int i = 1; i <= NUM_VARIABLE; i++) {
      String vid = "x" + i;
      dump(vid);
    }
  }

  /**
   * Print the committed values of all copies of all variables at site i
   * @param sid the site id
   *
   * @author Shuang
   */
  public void dump(int sid) {
    Site s = _sites.get(sid - 1);
    StringBuffer sb = new StringBuffer("Site " + sid + ":\n");
    List<String> vids = s.getAllVariableIds();
    for (String vid : vids) {
      int value = s.readVariable(vid, true);
      sb.append(vid + ": " + value + "\n");
    }
    System.out.println(sb.toString());
  }

  /**
   * Print the committed values of all copies of the given variable at all sites
   * @param vid the variable id
   *
   * @author Shuang
   */
  public void dump(String vid) {
    int vidx = Integer.valueOf(vid.substring(1));
    if (vidx % 2 == 1) {
      int sid = 1 + vidx % NUM_SITE;
      Site s = _sites.get(sid - 1);
      int value = s.readVariable(vid, true);
      System.out.println(vid + ": " + value + " at site " + s._sid);
    } else {
      Map<Integer, List<Integer>> values = new HashMap<>();
      for (Site s : _sites) {
        int value = s.readVariable(vid, true);
        if (!values.containsKey(value)) {
          values.put(value, new ArrayList<Integer>());
        }
        List<Integer> siteOfValue = values.get(value);
        siteOfValue.add(s._sid);
      }
      StringBuffer sb;
      for (int v : values.keySet()) {
        if (values.size() == 1) {
          System.out.println(vid + ": " + v + " at all sites");
        } else {
          sb = new StringBuffer(vid + ": ");
          sb.append(v + " at site ");
          for (int s : values.get(v)) {
            sb.append(s + " ");
          }
          System.out.println(sb.toString().trim());
        }
      }
    }
  }

  /**
   * Print state for check in versbose mode
   * @param message the state info
   *
   * @author Shuang
   */
  public void printVerbose(String message) {
    if (verbose) {
      System.out.println("# " + message);
    }
  }

  public static void main(String[] args) {
    DBSystem dbs = new DBSystem();

    //String fileName = getInput();
    if (args.length < 1) {
      System.out.println("Error: no valid input file");
      System.exit(1);
    }

    dbs.run(args[0]);
  }

  private static String getInput() {
    Scanner input = new Scanner(System.in);
    String inputFile = null;
    try {
      File f;
      do {
        System.out.println("Enter input filename: ");
        inputFile = input.next();
        f = new File(inputFile);
      } while (!f.exists());
      input.close();
    } catch (Exception e) {
      System.out.println(e.getMessage());
    }

    return inputFile;
  }
}
