package model.flink;

public enum DimensionType {
	VERTICES("vertices"), TARGET("target"), INCOMING("incoming"),
	OUTGOING("outgoing"), REGIONS("regions");
	
	private String label;
	
	DimensionType(String label) {
		this.label = label;
	}
	
	public String getLabel() {
		return label;
	}
}
