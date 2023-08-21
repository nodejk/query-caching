import common.CalciteConfiguration;
import test.QueryReader;
import utils.Configuration;

import java.io.PrintStream;
import java.util.List;

public class Main {

    public static final int MODE_SEQ = 0;
    public static final int MODE_HYB = 1;
    public static final int MODE_BAT = 2;
    public static final int MODE_MVR = 3;

    public static double lowerDerVal = 0;

    public static final List<Integer> CACHE_SIZES = List.of(4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096);
    public static final List<String> DERIVABILITIES = List.of("0", "10", "20", "25", "35", "40", "45", "60", "75", "78", "83", "88", "90");

    /*
    The execution is managed by command line arguments
        [mode] [cache size] [derivability] [query type]

        mode: 0 for sequential test, 1 for hybrid test, 2 for batch test, 3 for mvr test
        cache size: [0 10] == [4MB 4GB]
        derivability: [0 15]
        query type: [0, 1, 2, 3, 4, 5]
     */

    public static void main(String[] args) throws Exception {
        String cacheType = args[0];
        String mode = args[1];
        Integer cacheSize = Integer.parseInt(args[2]);
        String queryType = args[3];

        Integer derivibility = Integer.parseInt(args[4]);
        String dimensionType = args[5];

        Configuration config = new Configuration(
                cacheType,
                mode,
                cacheSize,
                queryType,
                derivibility,
                dimensionType
        );

        CalciteConfiguration calciteConfiguration = CalciteConfiguration.initialize();

        QueryReader.dir = config.derivability.toString();
        QueryReader.folderName = config.cacheType.toString();

        Tester tester = new Tester(calciteConfiguration, config);
//
        tester.testMain(config.mode, config.queryType);
//        hideLoggerWarnings();
//
//        System.out.println("########################################################################################");
//        System.out.println("########################################################################################");
//        System.out.println("########################################################################################");
//
////        args = new String[] {"2", "0", "0"};
//
//        int modeArg = Integer.parseInt(args[0]);
//        int cacheSizeArg = Integer.parseInt(args[1]);
//        int derivabilityArg = Integer.parseInt(args[2]);
//        int queryTypeArg = Integer.parseInt(args[3]);
//
//        if (derivabilityArg <= 4) {
//            lowerDerVal = derivabilityArg == 0 ? 1
//            : derivabilityArg == 1 ? 0.94
//            : derivabilityArg == 2 ? 0.85
//            : derivabilityArg == 3 ? 0.8
//            : 0.5;
//
//            derivabilityArg = 5;
//        }
//
//        Configuration config = Configuration.initialize();


//        String mode = modeArg == 0 ? "SEQ"
//                : modeArg == 1 ? "HYB"
//                : modeArg == 2 ? "BAT"
//                : "MVR";
//        String queryType = queryTypeArg == 0 ? "ALL"
//                : queryTypeArg == 1 ? "Simple Filter"
//                : queryTypeArg == 2 ? "Complex filter"
//                : queryTypeArg == 3 ? "Filter join"
//                : queryTypeArg == 4 ? "Filter aggregate"
//                : "Filter join aggregate";
//        int size = CACHE_SIZES.get(cacheSizeArg);
//        String der = DERIVABILITIES.get(derivabilityArg);
//
//        System.out.printf(
//                "Starting with mode: %s, cache size: %dMB, derivability: %s, query type: %s\n", mode, size,
//                DERIVABILITIES.get(derivabilityArg),
//                queryType
//        );
//        if (lowerDerVal > 0) {
//            System.out.println("LOW DER: " + lowerDerVal);
//        }
//
//        QueryReader.dir = der;




//        tester.normalExecTest();
//        tester.testFindDerivablePercentage();
//        tester.testMVSubstitution();
//        tester.testDerivabilityPerf();
//        tester.testBatch2();
//        tester.testMultipleExecutions();
//        tester.printQuerySizes();
    }

    public static void hideLoggerWarnings() {
        PrintStream filterOut = new PrintStream(System.err) {
            public void println(String l) {
                if (!l.startsWith("SLF4J")) {
                    super.println(l);
                }
            }
        };
        System.setErr(filterOut);
    }

}
