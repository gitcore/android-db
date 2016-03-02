package sunset.gitcore.database;

public interface Log {
	void log(Object message);

	Log NONE = new Log() {
		@Override
		public void log(Object message) {
            System.out.println(message);
		}
	};
}
