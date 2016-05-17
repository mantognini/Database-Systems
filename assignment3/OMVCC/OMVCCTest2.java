import java.io.PrintStream;
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
 */
public class OMVCCTest2 {
	
	private static boolean ENABLE_COMMAND_LOGGING = true;
	private static PrintStream log = System.out;

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
			}
		} catch (Exception e) {
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
		expectedResults[T(2)][STEP(4)] = RESULT(STEP(2));
		expectedResults[T(3)][STEP(6)] = RESULT(STEP(2));
		expectedResults[T(2)][STEP(9)] = RESULT(STEP(1));
		expectedResults[T(4)][STEP(12)] = RESULT(STEP(5));
		expectedResults[T(4)][STEP(13)] = RESULT(STEP(10));
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
		expectedResults[T(2)][STEP(4)] = RESULT(STEP(2));
		expectedResults[T(3)][STEP(5)] = RESULT(STEP(2));
		expectedResults[T(2)][STEP(7)] = RESULT(STEP(2));
		expectedResults[T(4)][STEP(11)] = RESULT(STEP(6));
		expectedResults[T(4)][STEP(12)] = RESULT(STEP(8));
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
		expectedResults[T(4)][STEP(10)] = RESULT(STEP(1));
		expectedResults[T(4)][STEP(11)] = RESULT(STEP(6));
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
		expectedResults[T(2)][STEP(5)] = RESULT(STEP(3));
		expectedResults[T(3)][STEP(8)] = RESULT(STEP(3));
		expectedResults[T(3)][STEP(9)] = RESULT(STEP(1),STEP(3));
		expectedResults[T(2)][STEP(11)] = RESULT(STEP(7));
		expectedResults[T(4)][STEP(15)] = RESULT(STEP(7));
		expectedResults[T(4)][STEP(16)] = RESULT(STEP(2));
		expectedResults[T(4)][STEP(18)] = RESULT(STEP(17));
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
									OMVCC.commit(xact);
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
									OMVCC.write(xact, key, value);
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
										int expected = getValue((Integer)expectedRes);
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
											expectedResInt.add(getValue((Integer)er));
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
					} catch (WrongResultException e) {
						throw e;
					} catch (Exception e) {
						ignoredXactLabels.add(xactLabel);
						if(e.getMessage() != null)
							log.println("    "+e.getMessage());
						else
							e.printStackTrace();
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
					sb.append("     ");
				} else if(xactOps.length == 1) {
					switch(xactOps[0]) {
						case COMMIT: sb.append("  C "); break;
						case ABORT: sb.append("  A "); break;
					}
				} else {
					switch(xactOps[0]) {
						case WRITE: sb.append("W"); break;
						case READ: sb.append("R"); break;
						case MODQ: sb.append("M"); break;
					}
					sb.append("(").append(xactOps[1]).append(")");
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
				} else {
					throw new RuntimeException("Unknown operation in schedule: T"+(i+1)+", Operation "+(j+1));
				}
			}
		}
		return maxLen;
	}
	
	private final static int /*BEGIN = 1,*/ WRITE = 2, READ = 3, MODQ = 4, COMMIT = 5, ABORT = 6;
	private final static int[] /*__B_ = {BEGIN},*/ __C_ = {COMMIT}, __A_ = {ABORT}, ____ = null;

	//transaction
	private static int T(int i) {
		return i-1;
	}
	//step
	private static int STEP(int i) {
		return i-1;
	}
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
}
