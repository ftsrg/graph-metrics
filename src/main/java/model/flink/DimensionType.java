package model.flink;

/**
 * Enum class for dimension types.
 * 
 * @author Lehel
 *
 */
public enum DimensionType {
	VERTICES("vertices"), TARGET("target"), INCOMING("incoming"), OUTGOING("outgoing"), REGIONS("regions");

	private String label;

	/**
	 * Constructor.
	 * 
	 * @param label
	 *            - string value of the enum
	 */
	DimensionType(String label) {
		this.label = label;
	}

	/**
	 * Returns the string value of the enum.
	 * 
	 * @return a string
	 */
	public String getLabel() {
		return label;
	}
}
