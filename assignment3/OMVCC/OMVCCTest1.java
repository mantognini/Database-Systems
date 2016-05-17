/**
 *  This is an example test file. Try others to debug your OMVCC impl.
 *  
 * @author Christoph Koch (christoph.koch@epfl.ch)
 *
 */
public class OMVCCTest1 {
	// This is an example test file. Try others to debug your system!!!
	
	public static void main(String[] args) {
		try {
			/* Example schedule:
			 T1: I(1) C
			 T2:        R(1) W(1)           R(1) W(1) C
			 T3:                  R(1) W(1)             C
			*/
			long t1 = OMVCC.begin();
			OMVCC.write(t1, 1, 13); // create obj1 and initialize with 13
			OMVCC.commit(t1);
			long t2 = OMVCC.begin();
			long t3 = OMVCC.begin();
			OMVCC.write(t2, 1, OMVCC.read(t2, 1) * 2); // double value of obj1
			OMVCC.write(t3, 2, OMVCC.read(t3, 1) + 4); // create obj2 by adding 4 to the value of obj1

			OMVCC.write(t2, 1, OMVCC.read(t2, 1) * 2); // double value of obj1

			OMVCC.commit(t3);
			OMVCC.commit(t2);
			long t4 = OMVCC.begin();
			assert OMVCC.read(t4, 1) == 52;
			assert OMVCC.read(t4, 2) == 17;
			OMVCC.commit(t4);

			System.out.println("Success in OMVCCTest1!");
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("Failure in OMVCCTest1!");
		}
	}
}
