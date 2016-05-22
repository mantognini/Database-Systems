import java.io.*;
import java.util.*;

/**
 *  This is a more intuitive test suit for testing your
 *  Multi-version Snapshot Isolation implementation.
 *
 *  You can easily create new schedules, by giving the
 *  specification, and determining the correct results.
 *
 * @author Mohammad Dashti (mohammad.dashti@epfl.ch)
 *
 * @author Marco Antognini
 *   -- significant change were made to have a ROBUST testing suite
 *
 */
public class OMVCCTest2 {

    private static boolean ENABLE_COMMAND_LOGGING = true;
    private static ByteArrayOutputStream buffer = new ByteArrayOutputStream();
    private static PrintStream log = new PrintStream(buffer);

    public static void main(String[] args) {
        // # of test to execute
        // For automatic validation, it is not possible to execute all tests at once
        // You can get the TEST# from args and execute all tests using a shell-script
        int TEST = 1;
        if(args.length > 0) {
            TEST = Integer.parseInt(args[0]);
        }

        try {
            switch (TEST) {
                case 1: test1(); break;
                case 2: test2(); break;
                case 3: test3(); break;
                case 4: test4(); break;
                case 5: test5(); break;
                case 6: test6(); break;
                case 7: test7(); break;
                case 8: test8(); break;
                case 9: test9(); break;
                case 10: test10(); break;
                case 11: test11(); break;
                case 12: test12(); break;
                case 13: test13(); break;
                case 14: test14(); break;
                case 15: test15(); break;
                default: throw new AssertionError("Unknown test " + TEST);
            }
            // System.out.print(buffer.toString());
        } catch (Exception e) {
            System.out.print(buffer.toString());
            e.printStackTrace();
            System.exit(-1);
        }
    }

    /**
     * Tests WR conflict (reading uncommitted data)
     */
    private static void test1() {
        log.println("----------- Test 1 -----------");
        /* Example schedule:
            T1: W(3),W(1),  C
            T2:                R(1),W(1),               R(3),W(3),  A
            T3:                          R(1),W(1),  C
            T4:                                                        R(1),R(3),  C
        */
        int[][][] schedule = new int[][][]{
            // t      1    2    3    4    5    6    7    8    9    10   11   12  13    14
            /*T1:*/ {W(3),W(1),__C_                                                       },
            /*T2:*/ {____,____,____,R(1),W(1),____,____,____,R(3),W(3),__C_               },
            /*T3:*/ {____,____,____,____,____,R(1),W(1),__C_                              },
            /*T4:*/ {____,____,____,____,____,____,____,____,____,____,____,R(1),R(3),__C_}
        };
        // T(1):B
        // T(1):W(3,4)
        // T(1):W(1,6)
        // T(1):C
        // T(2):B
        // T(2):R(1) => 6
        // T(2):W(1,12)
        // T(3):B
        // T(3):R(1) => 6
        // T(3):W(1,16)
        //     T(3) could not write a version for key = 1 because there is another uncommitted version written by T(2)
        // T(2):R(3) => 4
        // T(2):W(3,22)
        // T(2):C
        // T(4):B
        // T(4):R(1) => 12
        // T(4):R(3) => 22
        // T(4):C
        int maxLen = analyzeSchedule(schedule);
        printSchedule(schedule);
        Object[][][] expectedResults = new Object[schedule.length][maxLen][];
        expectedResults[T(1)][STEP(1)] = VALID;
        expectedResults[T(1)][STEP(2)] = VALID;
        expectedResults[T(1)][STEP(3)] = VALID;
        expectedResults[T(2)][STEP(4)] = RESULT(AT_STEP(2));
        expectedResults[T(2)][STEP(5)] = VALID;
        expectedResults[T(3)][STEP(6)] = RESULT(AT_STEP(2));
        expectedResults[T(3)][STEP(7)] = ROLLBACK;
        // no step(8) since rollback
        expectedResults[T(2)][STEP(9)] = RESULT(AT_STEP(1));
        expectedResults[T(2)][STEP(10)] = VALID;
        expectedResults[T(2)][STEP(11)] = VALID;
        expectedResults[T(4)][STEP(12)] = RESULT(AT_STEP(5));
        expectedResults[T(4)][STEP(13)] = RESULT(AT_STEP(10));
        expectedResults[T(4)][STEP(14)] = VALID;
        executeSchedule(schedule, expectedResults, maxLen);
    }

    /**
     * Tests RW conflict (unrepeatable reads)
     */
    private static void test2(){
        log.println("----------- Test 2 -----------");
        /* Example schedule:
            T1: W(4),W(2),  C
            T2:                R(2),               R(2),W(4),  C
            T3:                     R(2),W(2),  C
            T4:                                                   R(2),R(4),  C
        */
        int[][][] schedule = new int[][][]{
            // t      1    2    3    4    5    6    7    8    9    10   11   12  13
            /*T1:*/ {W(4),W(2),__C_                                                  },
            /*T2:*/ {____,____,____,R(2),____,____,R(2),W(4),__C_                    },
            /*T3:*/ {____,____,____,____,R(2),W(2),____,____,____,__C_               },
            /*T4:*/ {____,____,____,____,____,____,____,____,____,____,R(2),R(4),__C_}
        };
        // T(1):B
        // T(1):W(4,4)
        // T(1):W(2,6)
        // T(1):C
        // T(2):B
        // T(2):R(2) => 6
        // T(3):B
        // T(3):R(2) => 6
        // T(3):W(2,14)
        // T(2):R(2) => 6
        // T(2):W(4,18)
        // T(2):C
        // T(3):C
        // T(4):B
        // T(4):R(2) => 14
        // T(4):R(4) => 18
        // T(4):C
        int maxLen = analyzeSchedule(schedule);
        printSchedule(schedule);
        Object[][][] expectedResults = new Object[schedule.length][maxLen][];
        expectedResults[T(1)][STEP(1)] = VALID;
        expectedResults[T(1)][STEP(2)] = VALID;
        expectedResults[T(1)][STEP(3)] = VALID;
        expectedResults[T(2)][STEP(4)] = RESULT(AT_STEP(2));
        expectedResults[T(3)][STEP(5)] = RESULT(AT_STEP(2));
        expectedResults[T(3)][STEP(6)] = VALID;
        expectedResults[T(2)][STEP(7)] = RESULT(AT_STEP(2));
        expectedResults[T(2)][STEP(8)] = VALID;
        expectedResults[T(2)][STEP(9)] = VALID;
        expectedResults[T(3)][STEP(10)] = VALID;
        expectedResults[T(4)][STEP(11)] = RESULT(AT_STEP(6));
        expectedResults[T(4)][STEP(12)] = RESULT(AT_STEP(8));
        expectedResults[T(4)][STEP(13)] = VALID;
        executeSchedule(schedule, expectedResults, maxLen);
    }

    /**
     * Tests WW conflict (overwriting uncommitted data)
     */
    private static void test3() {
        log.println("----------- Test 3 -----------");
        /* Example schedule:
            T1: W(2),                    W(3),  C
            T2:      W(2),W(3),  C
            T3:                     R(2),          W(3),  C ,
            T4:                                              R(2),R(3),  C
        */
        int[][][] schedule = new int[][][]{
            // t      1    2    3    4    5    6    7    8    9    10   11   12
            /*T1:*/ {W(2),____,____,____,____,W(3),__C_                         },
            /*T2:*/ {____,W(2),W(3),__C_                                        },
            /*T3:*/ {____,____,____,____,W(1),____,____,W(3),__C_,____,____,____},
            /*T4:*/ {____,____,____,____,____,____,____,____,____,R(2),R(3),__C_}
        };
        // T(1):B
        // T(1):W(2,4)
        // T(2):B
        // T(2):W(2,6)
        //     T(2) could not write a version for key = 2 because there is another uncommitted version written by T(1)
        // T(3):B
        // T(3):W(1,12)
        // T(1):W(3,14)
        // T(1):C
        // T(3):W(3,18)
        //     T(3) could not write a version for key = 3 because there is a newer committed version.
        // T(4):B
        // T(4):R(2) => 4
        // T(4):R(3) => 14
        // T(4):C
        int maxLen = analyzeSchedule(schedule);
        printSchedule(schedule);
        Object[][][] expectedResults = new Object[schedule.length][maxLen][];
        expectedResults[T(1)][STEP(1)] = VALID;
        expectedResults[T(2)][STEP(2)] = ROLLBACK;
        expectedResults[T(3)][STEP(5)] = VALID;
        expectedResults[T(1)][STEP(6)] = VALID;
        expectedResults[T(1)][STEP(7)] = VALID;
        expectedResults[T(3)][STEP(8)] = ROLLBACK;
        expectedResults[T(4)][STEP(10)] = RESULT(AT_STEP(1));
        expectedResults[T(4)][STEP(11)] = RESULT(AT_STEP(6));
        expectedResults[T(4)][STEP(12)] = VALID;
        executeSchedule(schedule, expectedResults, maxLen);
    }

    private static void test4(){
        log.println("----------- Test 4 -----------");
        /* Example schedule:
            T1: W(2),W(3),W(9),  C
            T2:                     R(9),W(2),W(9),               R(2),W(9),  C ,
            T3:                                    R(9),W(3),W(9),                           C
            T4:                                                                  R(2),R(3),       C
        */
        int[][][] schedule = new int[][][]{
            // t      1    2    3    4    5    6    7    8    9    10   11   12   13   14   15   16   17   18   19
            /*T1:*/ {W(2),W(3),W(9),__C_                                                                           },
            /*T2:*/ {____,____,____,____,R(9),W(9),W(2),____,____,____,R(2),W(9),__C_                              },
            /*T3:*/ {____,____,____,____,____,____,____,R(9),M(4),W(3),____,____,____,__C_                         },
            /*T4:*/ {____,____,____,____,____,____,____,____,____,____,____,____,____,____,R(2),R(3),W(3),R(3),__C_}
        };
        // T(1):B
        // T(1):W(2,4)
        // T(1):W(3,6)
        // T(1):W(9,8)
        // T(1):C
        // T(2):B
        // T(2):R(9) => 8
        // T(2):W(9,14)
        // T(2):W(2,16)
        // T(3):B
        // T(3):R(9) => 8
        // T(3):M(4) => [4, 8]
        // T(3):W(3,22)
        // T(2):R(2) => 16
        // T(2):W(9,26)
        // T(2):C
        // T(3):C
        //     T(3) did not pass the validation.
        // T(4):B
        // T(4):R(2) => 16
        // T(4):R(3) => 6
        // T(4):W(3,36)
        // T(4):R(3) => 36
        // T(4):C
        int maxLen = analyzeSchedule(schedule);
        printSchedule(schedule);
        Object[][][] expectedResults = new Object[schedule.length][maxLen][];
        expectedResults[T(1)][STEP(1)] = VALID;
        expectedResults[T(1)][STEP(2)] = VALID;
        expectedResults[T(1)][STEP(3)] = VALID;
        expectedResults[T(1)][STEP(4)] = VALID;
        expectedResults[T(2)][STEP(5)] = RESULT(AT_STEP(3));
        expectedResults[T(2)][STEP(6)] = VALID;
        expectedResults[T(2)][STEP(7)] = VALID;
        expectedResults[T(3)][STEP(8)] = RESULT(AT_STEP(3));
        expectedResults[T(3)][STEP(9)] = RESULT(AT_STEP(1),AT_STEP(3));
        expectedResults[T(3)][STEP(10)] = VALID;
        expectedResults[T(2)][STEP(11)] = RESULT(AT_STEP(7));
        expectedResults[T(2)][STEP(12)] = VALID;
        expectedResults[T(2)][STEP(13)] = VALID;
        expectedResults[T(3)][STEP(14)] = ROLLBACK;
        expectedResults[T(4)][STEP(15)] = RESULT(AT_STEP(7));
        expectedResults[T(4)][STEP(16)] = RESULT(AT_STEP(2));
        expectedResults[T(4)][STEP(17)] = VALID;
        expectedResults[T(4)][STEP(18)] = RESULT(AT_STEP(17));
        expectedResults[T(4)][STEP(19)] = VALID;
        executeSchedule(schedule, expectedResults, maxLen);
    }

    private static void test5(){
        log.println("----------- Test 5 -----------");

        int[][][] schedule = new int[][][]{
        //        1    2    3    4     5    6    7   8    9    10   11  12   13   14   15   16
        /*T1:*/ {W(3),W(1),____,W(2),____,__C_                                                  },
        /*T2:*/ {____,____,____,____,W(9),____,____,____ ,____,____,W(9),__C_                    },
        /*T3:*/ {____,____,____,____,____,____,____,____ ,W(8),__C_                              },
        /*T4:*/ {____,____,____,____,____,____,____,____ ,____,____,____,____,____,R(1),R(3),__C_},
        /*T5:*/ {____,____,____,____,____,____,M(2),W(10),____,____,____,____,__C_              },
        };

        int maxLen = analyzeSchedule(schedule);
        printSchedule(schedule);
        Object[][][] expectedResults = new Object[schedule.length][maxLen][];
        expectedResults[T(1)][STEP(1)] = VALID;
        expectedResults[T(1)][STEP(2)] = VALID;
        expectedResults[T(1)][STEP(4)] = VALID;
        expectedResults[T(2)][STEP(5)] = VALID;
        expectedResults[T(1)][STEP(6)] = VALID;
        expectedResults[T(5)][STEP(7)] = RESULT(AT_STEP(1),AT_STEP(2),AT_STEP(4));
        expectedResults[T(5)][STEP(8)] = VALID;
        expectedResults[T(3)][STEP(9)] = VALID;
        expectedResults[T(3)][STEP(10)] = VALID;
        expectedResults[T(2)][STEP(11)] = VALID;
        expectedResults[T(2)][STEP(12)] = VALID;
        expectedResults[T(5)][STEP(13)] = ROLLBACK;
        expectedResults[T(4)][STEP(14)] = RESULT(AT_STEP(2));
        expectedResults[T(4)][STEP(15)] = RESULT(AT_STEP(1));
        expectedResults[T(4)][STEP(16)] = VALID;
        executeSchedule(schedule, expectedResults, maxLen);
    }

    private static void test6() {
        log.println("----------- Test 6 -----------");

        int[][][] schedule = new int[][][]{
            // t      1    2    3    4    5    6    7    8    9    10
            /*T1:*/ {W(1),W(2),__C_                                   },
            /*T2:*/ {____,____,____,M(2),M(4),____,____,____,W(3),__C_},
            /*T3:*/ {____,____,____,____,____,W(4),W(5),__C_          },
        };

        int maxLen = analyzeSchedule(schedule);
        printSchedule(schedule);
        Object[][][] expectedResults = new Object[schedule.length][maxLen][];

        expectedResults[T(1)][STEP(1)] = VALID;
        expectedResults[T(1)][STEP(2)] = VALID;
        expectedResults[T(1)][STEP(3)] = VALID;
        expectedResults[T(2)][STEP(4)] = RESULT(AT_STEP(1),AT_STEP(2));
        expectedResults[T(2)][STEP(5)] = RESULT(AT_STEP(1));
        expectedResults[T(3)][STEP(6)] = VALID;
        expectedResults[T(3)][STEP(7)] = VALID;
        expectedResults[T(3)][STEP(8)] = VALID;
        expectedResults[T(2)][STEP(9)] = VALID;
        expectedResults[T(2)][STEP(10)] = ROLLBACK;

        executeSchedule(schedule, expectedResults, maxLen);
    }

    private static void test7() {
        log.println("----------- Test 7 -----------");

        int[][][] schedule = new int[][][]{
            // t      1    2    3    4    5    6    7    8    9    10
            /*T1:*/ {W(1),W(2),__C_                                   },
            /*T2:*/ {____,____,____,M(9),____,____,____,____,W(3),__C_},
            /*T3:*/ {____,____,____,____,W(4),____,____,__C_          },
            /*T4:*/ {____,____,____,____,____,W(5),__C_               },
        };

        int maxLen = analyzeSchedule(schedule);
        printSchedule(schedule);
        Object[][][] expectedResults = new Object[schedule.length][maxLen][];

        expectedResults[T(1)][STEP(1)] = VALID; // 4
        expectedResults[T(1)][STEP(2)] = VALID; // 6
        expectedResults[T(1)][STEP(3)] = VALID;
        expectedResults[T(2)][STEP(4)] = RESULT();
        expectedResults[T(3)][STEP(5)] = VALID; // 12
        expectedResults[T(4)][STEP(6)] = VALID; // 14
        expectedResults[T(4)][STEP(7)] = VALID;
        expectedResults[T(3)][STEP(8)] = VALID;
        expectedResults[T(2)][STEP(9)] = VALID; // 20
        expectedResults[T(2)][STEP(10)] = VALID;

        executeSchedule(schedule, expectedResults, maxLen);
    }

    private static void test8() {
        log.println("----------- Test 8 -----------");

        int[][][] schedule = new int[][][]{
            // t      1    2    3    4    5    6    7    8    9    10   11
            /*T1:*/ {W(1),W(2),__C_                                        },
            /*T2:*/ {____,____,____,M(9),____,____,____,____,M(2),W(3),__C_},
            /*T3:*/ {____,____,____,____,W(4),____,____,__C_               },
            /*T4:*/ {____,____,____,____,____,W(5),__C_                    },
        };

        int maxLen = analyzeSchedule(schedule);
        printSchedule(schedule);
        Object[][][] expectedResults = new Object[schedule.length][maxLen][];

        expectedResults[T(1)][STEP(1)] = VALID; // 4
        expectedResults[T(1)][STEP(2)] = VALID; // 6
        expectedResults[T(1)][STEP(3)] = VALID;
        expectedResults[T(2)][STEP(4)] = RESULT();
        expectedResults[T(3)][STEP(5)] = VALID; // 12
        expectedResults[T(4)][STEP(6)] = VALID; // 14
        expectedResults[T(4)][STEP(7)] = VALID;
        expectedResults[T(3)][STEP(8)] = VALID;
        expectedResults[T(2)][STEP(9)] = RESULT(AT_STEP(1),AT_STEP(2));
        expectedResults[T(2)][STEP(10)] = VALID; // 22
        expectedResults[T(2)][STEP(11)] = ROLLBACK;

        executeSchedule(schedule, expectedResults, maxLen);
    }

    private static void test9() {
        log.println("----------- Test 9 -----------");

        int[][][] schedule = new int[][][]{
            // t      1    2    3    4    5    6    7    8    9    10   11
            /*T1:*/ {W(1),W(2),__C_                                        },
            /*T2:*/ {____,____,____,M(9),____,____,____,____,M(9),W(3),__C_},
            /*T3:*/ {____,____,____,____,W(4),____,____,__C_               },
            /*T4:*/ {____,____,____,____,____,W(5),__C_                    },
        };

        int maxLen = analyzeSchedule(schedule);
        printSchedule(schedule);
        Object[][][] expectedResults = new Object[schedule.length][maxLen][];

        expectedResults[T(1)][STEP(1)] = VALID; // 4
        expectedResults[T(1)][STEP(2)] = VALID; // 6
        expectedResults[T(1)][STEP(3)] = VALID;
        expectedResults[T(2)][STEP(4)] = RESULT();
        expectedResults[T(3)][STEP(5)] = VALID; // 12
        expectedResults[T(4)][STEP(6)] = VALID; // 14
        expectedResults[T(4)][STEP(7)] = VALID;
        expectedResults[T(3)][STEP(8)] = VALID;
        expectedResults[T(2)][STEP(9)] = RESULT();
        expectedResults[T(2)][STEP(10)] = VALID; // 22
        expectedResults[T(2)][STEP(11)] = VALID;

        executeSchedule(schedule, expectedResults, maxLen);
    }

    private static void test10() {
        log.println("----------- Test 10 -----------");

        int[][][] schedule = new int[][][]{
            // t         1       2   3       4      5     6      7       8        9      10   11   12     13      14     15    16    17      18    19
            /*T1:*/ {  ____   ,____,M(2),W2(33,33),__C_                                                                                                },
            /*T2:*/ {W2(1,100),__C_                                                                                                                    },
            /*T3:*/ {  ____   ,____,____,   ____  ,____,M(7),  ____  ,  ____  ,  ____  ,____,____,____,W2(99,99),__C_                                  },
            /*T4:*/ {  ____   ,____,____,   ____  ,____,____,W2(4,13),  ____  ,  ____  ,__C_                                                           },
            /*T5:*/ {  ____   ,____,____,   ____  ,____,____,  ____  ,W2(5,15),  ____  ,____,__C_                                                      },
            /*T6:*/ {  ____   ,____,____,   ____  ,____,____,  ____  ,  ____  ,W2(6,71),____,____,__C_                                                 },
            /*T7:*/ {  ____   ,____,____,   ____  ,____,____,  ____  ,  ____  ,  ____  ,____,____,____,   ____  ,____,W2(1,99),____,__C_               },
            /*T8:*/ {  ____   ,____,____,   ____  ,____,____,  ____  ,  ____  ,  ____  ,____,____,____,   ____  ,____,  ____  ,M(2),____,W2(99,99),__C_}
        };

        int maxLen = analyzeSchedule(schedule);
        printSchedule(schedule);
        Object[][][] expectedResults = new Object[schedule.length][maxLen][];

        expectedResults[T(2)][STEP(1)] = VALID;
        expectedResults[T(2)][STEP(2)] = VALID;
        expectedResults[T(1)][STEP(3)] = RESULT(100);
        expectedResults[T(1)][STEP(4)] = VALID;
        expectedResults[T(1)][STEP(5)] = VALID;
        expectedResults[T(3)][STEP(6)] = RESULT();
        expectedResults[T(4)][STEP(7)] = VALID;
        expectedResults[T(5)][STEP(8)] = VALID;
        expectedResults[T(6)][STEP(9)] = VALID;
        expectedResults[T(4)][STEP(10)] = VALID;
        expectedResults[T(5)][STEP(11)] = VALID;
        expectedResults[T(6)][STEP(12)] = VALID;
        expectedResults[T(3)][STEP(13)] = VALID;
        expectedResults[T(3)][STEP(14)] = VALID;
        expectedResults[T(7)][STEP(15)] = VALID;
        expectedResults[T(8)][STEP(16)] = RESULT(100);
        expectedResults[T(7)][STEP(17)] = VALID;
        expectedResults[T(8)][STEP(18)] = VALID;
        expectedResults[T(8)][STEP(19)] = ROLLBACK;

        executeSchedule(schedule, expectedResults, maxLen);
    }

    private static void test11() {
        log.println("----------- Test 11 -----------");

        int[][][] schedule = new int[][][]{
            // t        1       2     3       4      5     6    7
            /*T1:*/ {W2(1,100),__C_                                },
            /*T2:*/ {  ____   ,____,W2(1,99),____,  ____ ,__C_     },
            /*T3:*/ {  ____   ,____,  ____  ,M(2),W2(2,0),____,__C_},
        };

        int maxLen = analyzeSchedule(schedule);
        printSchedule(schedule);
        Object[][][] expectedResults = new Object[schedule.length][maxLen][];

        expectedResults[T(1)][STEP(1)] = VALID;
        expectedResults[T(1)][STEP(2)] = VALID;
        expectedResults[T(2)][STEP(3)] = VALID;
        expectedResults[T(3)][STEP(4)] = RESULT(100);
        expectedResults[T(3)][STEP(5)] = VALID;
        expectedResults[T(2)][STEP(6)] = VALID;
        expectedResults[T(3)][STEP(7)] = ROLLBACK;

        executeSchedule(schedule, expectedResults, maxLen);
    }

    private static void test12() {
        log.println("----------- Test 12 -----------");

        int[][][] schedule = new int[][][]{
            // t         1       2    3      4      5     6     7
            /*T1:*/ {W2(1,100),__C_                                },
            /*T2:*/ {  ____   ,____,M(2),  ____  ,____,W2(2,1),__C_},
            /*T3:*/ {  ____   ,____,____,W2(1,99),__C_             },
        };

        int maxLen = analyzeSchedule(schedule);
        printSchedule(schedule);
        Object[][][] expectedResults = new Object[schedule.length][maxLen][];

        expectedResults[T(1)][STEP(1)] = VALID;
        expectedResults[T(1)][STEP(2)] = VALID;
        expectedResults[T(2)][STEP(3)] = RESULT(100);
        expectedResults[T(3)][STEP(4)] = VALID;
        expectedResults[T(3)][STEP(5)] = VALID;
        expectedResults[T(2)][STEP(6)] = VALID;
        expectedResults[T(2)][STEP(7)] = ROLLBACK;

        executeSchedule(schedule, expectedResults, maxLen);
    }

    private static void test13() {
        log.println("----------- Test 13 -----------");

        int[][][] schedule = new int[][][]{
            // t         1       2    3      4      5     6     7       8     9
            /*T1:*/ {W2(1,100),__C_                                              },
            /*T2:*/ {  ____   ,____,M(2),  ____  ,____,  ____  ,____,W2(2,0),__C_},
            /*T3:*/ {  ____   ,____,____,W2(1,98),__C_                           },
            /*T4:*/ {  ____   ,____,____,  ____  ,____,W2(1,97),__C_             },
        };

        int maxLen = analyzeSchedule(schedule);
        printSchedule(schedule);
        Object[][][] expectedResults = new Object[schedule.length][maxLen][];

        expectedResults[T(1)][STEP(1)] = VALID;
        expectedResults[T(1)][STEP(2)] = VALID;
        expectedResults[T(2)][STEP(3)] = RESULT(100);
        expectedResults[T(3)][STEP(4)] = VALID;
        expectedResults[T(3)][STEP(5)] = VALID;
        expectedResults[T(4)][STEP(6)] = VALID;
        expectedResults[T(4)][STEP(7)] = VALID;
        expectedResults[T(2)][STEP(8)] = VALID;
        expectedResults[T(2)][STEP(9)] = ROLLBACK;

        executeSchedule(schedule, expectedResults, maxLen);
    }

    private static void test14() {
        log.println("----------- Test 14 -----------");

        int[][][] schedule = new int[][][]{
            // t         1       2    3      4      5     6     7       8     9
            /*T1:*/ {W2(1,101),__C_                                              },
            /*T2:*/ {  ____   ,____,M(2),  ____  ,____,  ____  ,____,W2(2,0),__C_},
            /*T3:*/ {  ____   ,____,____,W2(1,98),__C_                           },
            /*T4:*/ {  ____   ,____,____,  ____  ,____,W2(1,97),__C_             },
        };

        int maxLen = analyzeSchedule(schedule);
        printSchedule(schedule);
        Object[][][] expectedResults = new Object[schedule.length][maxLen][];

        expectedResults[T(1)][STEP(1)] = VALID;
        expectedResults[T(1)][STEP(2)] = VALID;
        expectedResults[T(2)][STEP(3)] = RESULT();
        expectedResults[T(3)][STEP(4)] = VALID;
        expectedResults[T(3)][STEP(5)] = VALID;
        expectedResults[T(4)][STEP(6)] = VALID;
        expectedResults[T(4)][STEP(7)] = VALID;
        expectedResults[T(2)][STEP(8)] = VALID;
        expectedResults[T(2)][STEP(9)] = ROLLBACK;

        executeSchedule(schedule, expectedResults, maxLen);
    }

    private static void test15() {
        log.println("----------- Test 15 -----------");

        int[][][] schedule = new int[][][]{
            // t         1       2    3      4      5     6     7       8     9
            /*T1:*/ {W2(1,101),__C_                                              },
            /*T2:*/ {  ____   ,____,M(2),  ____  ,____,  ____  ,____,W2(2,0),__C_},
            /*T3:*/ {  ____   ,____,____,W2(1,99),__C_                           },
            /*T4:*/ {  ____   ,____,____,  ____  ,____,W2(1,98),__C_             },
        };

        int maxLen = analyzeSchedule(schedule);
        printSchedule(schedule);
        Object[][][] expectedResults = new Object[schedule.length][maxLen][];

        expectedResults[T(1)][STEP(1)] = VALID;
        expectedResults[T(1)][STEP(2)] = VALID;
        expectedResults[T(2)][STEP(3)] = RESULT();
        expectedResults[T(3)][STEP(4)] = VALID;
        expectedResults[T(3)][STEP(5)] = VALID;
        expectedResults[T(4)][STEP(6)] = VALID;
        expectedResults[T(4)][STEP(7)] = VALID;
        expectedResults[T(2)][STEP(8)] = VALID;
        expectedResults[T(2)][STEP(9)] = ROLLBACK;

        executeSchedule(schedule, expectedResults, maxLen);
    }




    /**
     * This method is for executing a schedule.
     *
     * @param schedule is a 3D array containing one transaction
     *                 in each row, and in each cell is one operation
     * @param expectedResults is the array of expected result in each
     *                 READ operation. For:
     *                  - READ: the cell contains the STEP# (zero-based)
     *                          in the schedule that WRITTEN
     *                          the value that should be read here.
     * @param maxLen is the maximum length of schedule
     */
    private static void executeSchedule(int[][][] schedule, Object[][][] expectedResults, int maxLen) {
        Map<Integer, Long> xactLabelToXact = new HashMap<Integer, Long>();
        Set<Integer> ignoredXactLabels = new HashSet<Integer>();

        for(int step=0; step<maxLen; step++) {
            for(int i=0; i<schedule.length; i++) {
                if(step < schedule[i].length && schedule[i][step] != null) {
                    int[] xactOps = schedule[i][step];
                    int xactLabel = i+1;
                    if(ignoredXactLabels.contains(xactLabel)) break;

                    long xact = 0L;
                    try {
                        if(xactLabelToXact.containsKey(xactLabel)) {
                            xact = xactLabelToXact.get(xactLabel);
                        } else {
                            logCommand(String.format("T(%d):B", xactLabel));
                            xact = OMVCC.begin();
                            xactLabelToXact.put(xactLabel, xact);
                        }
                        if(xactOps.length == 1) {
                            switch(xactOps[0]) {
                                case COMMIT: {
                                    logCommand(String.format("T(%d):C", xactLabel));

                                    if (expectedResults[T(xactLabel)][step] == null)
                                        throw new AssertionError("expected commit status for "
                                                                 + "T(" + xactLabel + ") in step " + (step+1));

                                    Object expectedRes = expectedResults[T(xactLabel)][step];

                                    if (expectedRes != VALID && expectedRes != ROLLBACK)
                                        throw new AssertionError("expected correct commit status "
                                                                 + "for T(" + xactLabel + ") in step " + (step+1));

                                    expectedResults[T(xactLabel)][step] = null;

                                    try {
                                        OMVCC.commit(xact);
                                        if (expectedRes == ROLLBACK) throw new CommitFailedException(xactLabel, step, false);
                                        // else logCommand("commit succeeded as expected");
                                    } catch (CommitFailedException e) {
                                        throw e;
                                    } catch (Exception e) {
                                        if (expectedRes == VALID) throw new CommitFailedException(xactLabel, step, true);
                                        else {
                                            logCommand("commit failed as expected");
                                            throw e;
                                        }
                                    }
                                    break;
                                }
                                case ABORT: {
                                    logCommand(String.format("T(%d):R", xactLabel));
                                    OMVCC.rollback(xact);
                                    break;
                                }
                            }
                        } else {
                            int key = xactOps[1];
                            switch(xactOps[0]) {
                                case WRITE: {
                                    int value = getValue(step);
                                    logCommand(String.format("T(%d):W(%d,%d)", xactLabel, key, value));

                                    if (expectedResults[T(xactLabel)][step] == null)
                                        throw new AssertionError("expected write status for "
                                                                 + "T(" + xactLabel + ") in step " + (step+1));

                                    Object expectedRes = expectedResults[T(xactLabel)][step];

                                    if (expectedRes != VALID && expectedRes != ROLLBACK)
                                        throw new AssertionError("expected correct write status "
                                                                 + "for T(" + xactLabel + ") in step " + (step+1));

                                    expectedResults[T(xactLabel)][step] = null;

                                    try {
                                        OMVCC.write(xact, key, value);
                                        if (expectedRes == ROLLBACK) throw new BadWriteException(xactLabel, step, false);
                                        // else logCommand("write succeeded as expected");
                                    } catch (BadWriteException e) {
                                        throw e;
                                    } catch (Exception e) {
                                        if (expectedRes == VALID) {
                                            throw new BadWriteException(xactLabel, step, true);
                                        } else {
                                            logCommand("write failed as expected");
                                            throw e;
                                        }
                                    }
                                    break;
                                }
                                case WRITE2: {
                                    int value = xactOps[2];
                                    logCommand(String.format("T(%d):W(%d,%d)", xactLabel, key, value));

                                    if (expectedResults[T(xactLabel)][step] == null)
                                        throw new AssertionError("expected write status for "
                                                                 + "T(" + xactLabel + ") in step " + (step+1));

                                    Object expectedRes = expectedResults[T(xactLabel)][step];

                                    if (expectedRes != VALID && expectedRes != ROLLBACK)
                                        throw new AssertionError("expected correct write status "
                                                                 + "for T(" + xactLabel + ") in step " + (step+1));

                                    expectedResults[T(xactLabel)][step] = null;

                                    try {
                                        OMVCC.write(xact, key, value);
                                        if (expectedRes == ROLLBACK) throw new BadWriteException(xactLabel, step, false);
                                        // else logCommand("write succeeded as expected");
                                    } catch (BadWriteException e) {
                                        throw e;
                                    } catch (Exception e) {
                                        if (expectedRes == VALID) {
                                            throw new BadWriteException(xactLabel, step, true);
                                        } else {
                                            logCommand("write failed as expected");
                                            throw e;
                                        }
                                    }
                                    break;
                                }
                                case READ: {
                                    int readValue;
                                    try {
                                        readValue = OMVCC.read(xact, key);
                                        logCommand(String.format("T(%d):R(%d) => %d", xactLabel, key, readValue));
                                    } catch (Exception e) {
                                        logCommand(String.format("T(%d):R(%d) => --", xactLabel, key));
                                        throw e;
                                    }
                                    Object expectedRes = expectedResults[T(xactLabel)][step][0];
                                    if(expectedRes != null) {
                                        int expected = (Integer)expectedRes;
                                        if(readValue != expected) {
                                            throw new WrongResultException(xactLabel, step, xactOps, readValue, expected);
                                        }
                                        // marking the expected result as checked
                                        expectedResults[T(xactLabel)][step] = null;
                                    }
                                    break;
                                }
                                case MODQ: {
                                    List<Integer> readValue = OMVCC.modquery(xact, key);
                                    logCommand(String.format("T(%d):M(%d) => %s", xactLabel, key, readValue));
                                    Object[] expectedRes = expectedResults[T(xactLabel)][step];
                                    if(expectedRes != null) {
                                        List<Integer> expectedResInt = new ArrayList<Integer>();
                                        for(Object er : expectedRes) {
                                            expectedResInt.add((Integer)er);
                                        }
                                        Collections.sort(readValue);
                                        Collections.sort(expectedResInt);
                                        if(!readValue.equals(expectedResInt)) {
                                            throw new WrongResultException(xactLabel, step, xactOps, readValue, expectedResInt);
                                        }
                                        // marking the expected result as checked
                                        expectedResults[T(xactLabel)][step] = null;
                                    }
                                    break;
                                }
                            }
                        }
                    } catch (BadWriteException e) {
                        throw e;
                    } catch (CommitFailedException e) {
                        throw e;
                    } catch (WrongResultException e) {
                        throw e;
                    } catch (Exception e) {
                        ignoredXactLabels.add(xactLabel);
                        if(e.getMessage() != null)
                            log.println("    "+e.getMessage());
                        else
                            e.printStackTrace(log);
                    }
                    break;
                }
            }
        }

        // Check if some expected result was not checked
        for(int i=0; i < expectedResults.length; ++i) {
            for( int j=0; j < expectedResults[i].length; ++j) {
                if(expectedResults[i][j] != null) {
                    throw new ResultNotCheckedException(i+1, j);
                }
            }
        }
    }

    private static void logCommand(String cmd) {
        if(ENABLE_COMMAND_LOGGING) log.println(cmd);
    }

    /**
     * @param step is the STEP# in the schedule (zero-based)
     * @return the expected result of a READ operation in a schedule.
     */
    private static int getValue(int step) {
        return (step+2)*2;
    }

    private static void printSchedule(int[][][] schedule) {
        StringBuilder sb = new StringBuilder();
        for(int i=0; i<schedule.length; i++) {
            sb.append("T").append(i+1).append(": ");
            for(int j=0; j<schedule[i].length; j++) {
                int[] xactOps = schedule[i][j];
                if(xactOps == null) {
                    sb.append("         ");
                } else if(xactOps.length == 1) {
                    sb.append("  ");
                    switch(xactOps[0]) {
                        case COMMIT: sb.append("  C "); break;
                        case ABORT: sb.append("  A "); break;
                    }
                    sb.append("  ");
                } else if (xactOps.length == 2) {
                    sb.append("  ");
                    switch(xactOps[0]) {
                        case WRITE: sb.append("W"); break;
                        case READ: sb.append("R"); break;
                        case MODQ: sb.append("M"); break;
                    }
                    sb.append("(").append(xactOps[1]).append(")  ");
                } else {
                    switch(xactOps[0]) {
                        case WRITE2: sb.append("W2"); break;
                    }
                    sb.append("(").append(xactOps[1]).append(",").append(xactOps[2]).append(")");
                }
                if(j+1<schedule[i].length && xactOps != null){
                    sb.append(",");
                }
            }
            sb.append("\n");
        }
        log.println("\n"+sb.toString());
    }

    /**
     * Analyzes and validates the given schedule.
     *
     * @return maximum number of steps in the
     *         transactions inside the given schedule
     */
    private static int analyzeSchedule(int[][][] schedule) {
        int maxLen = 0;
        for(int i=0; i<schedule.length; i++) {
            if(maxLen < schedule[i].length) {
                maxLen = schedule[i].length;
            }
            for(int j=0; j<schedule[i].length; j++) {
                int[] xactOps = schedule[i][j];
                if(xactOps == null) {
                    // no operation
                } else if(xactOps.length == 1 && (xactOps[0] == COMMIT || xactOps[0] == ABORT)) {
                    // commit or roll back
                } else if(xactOps.length == 2){
                    switch(xactOps[0]) {
                        case WRITE: /*write*/; break;
                        case READ: /*read*/; break;
                        case MODQ: /*mod query*/; break;
                        default: throw new RuntimeException("Unknown operation in schedule: T"+(i+1)+", Operation "+(j+1));
                    }
                } else if (xactOps.length == 3 && xactOps[0] == WRITE2) {
                    // ok
                } else {
                    throw new RuntimeException("Unknown operation in schedule: T"+(i+1)+", Operation "+(j+1));
                }
            }
        }
        return maxLen;
    }

    private final static int /*BEGIN = 1,*/ WRITE = 2, READ = 3, MODQ = 4, COMMIT = 5, ABORT = 6, WRITE2 = 7;
    private final static int[] /*__B_ = {BEGIN},*/ __C_ = {COMMIT}, __A_ = {ABORT}, ____ = null;

    //transaction
    private static int T(int i) {
        return i-1;
    }
    //step
    private static int STEP(int i) {
        return i-1;
    }
    private static int AT_STEP(int i) {
        return getValue(STEP(i));
    }

    // write/commit successfully
    public static Object[] VALID    = new Object[1]; // dummy value different than ROLLBACK
    // write/commit results in rollback
    public static Object[] ROLLBACK = new Object[2]; // dummy value different than VALID

    //result
    private static Object[] RESULT(int... arr) {
        Object[] resArr = new Object[arr.length];
        for(int i=0; i<arr.length; ++i) {
            resArr[i] = arr[i];
        }
        return resArr;
    }
    //write
    public static int[] W(int key) {
        return new int[]{WRITE,key};
    }
    public static int[] W2(int key, int value) {
        return new int[]{WRITE2,key,value};
    }
    //read
    public static int[] R(int key) {
        return new int[]{READ,key};
    }
    //read
    public static int[] M(int k) {
        return new int[]{MODQ,k};
    }

    static class WrongResultException extends RuntimeException {
        public WrongResultException(int xactLabel, int step, int[] operation, Object actual, Object expected) {
            super("Wrong result in T("+xactLabel+") in step " + (step+1) + " (Actual: " + actual+", Expected: " + expected + ")");
        }
    }

    static class ResultNotCheckedException extends RuntimeException {
        public ResultNotCheckedException(int xactLabel, int step) {
            super("The result in T("+xactLabel+") in step " + (step+1) + " is not checked.");
        }
    }

    static class BadWriteException extends RuntimeException {
        public BadWriteException(int xactLabel, int step, boolean expectedSuccess) {
            super("It was expected of T(" + xactLabel + ") in step " + (step+1)
                  + " to " + (expectedSuccess ? "succeed" : "fail") + " but it didn't.");
        }
    }

    static class CommitFailedException extends RuntimeException {
        public CommitFailedException(int xactLabel, int step, boolean expectedSuccess) {
            super("It was expected of T(" + xactLabel + ") in step " + (step+1)
                  + " to " + (expectedSuccess ? "succeed" : "fail") + " but it didn't.");
        }
    }
}
