package node;

public class StartUp {

	public static void main(String[] args) {
		new Thread(new Server()).start();
	}

}
