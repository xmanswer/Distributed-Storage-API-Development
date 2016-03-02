public class StoreValue {
	private long timestamp;
	private String value;

	public StoreValue(long time, String val) {
		timestamp = time;
		value = val;
	}
	
	public long getTimestamp() {
		return timestamp;
	}

	public String getValue() {
		return value;
	}
}
