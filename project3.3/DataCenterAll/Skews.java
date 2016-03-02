public class Skews {
	public static long handleSkew(long timestamp, int region) {
		long adjustedTimestamp = 0;
                switch (region) {
                case Constants.US_EAST:
                        adjustedTimestamp = timestamp;
                        break;
                case Constants.US_WEST:
                        adjustedTimestamp = timestamp - 200;
                        break;
                case Constants.SINGAPORE:
                        adjustedTimestamp = timestamp - 600;
                        break;
                default:
                        break;
                }
		return adjustedTimestamp;
	}
}
