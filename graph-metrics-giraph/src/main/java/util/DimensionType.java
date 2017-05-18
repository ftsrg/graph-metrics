package util;



public enum DimensionType {
	
	CONTAINER_OF("containerOf"), HAS_COMMENT("hasComment"), HAS_CREATOR("hasCreator"),
	HAS_INTEREST("hasInterest"), HAS_MEMBER("hasMember"), HAS_MODERATOR("hasModerator"),
	HAS_ORGANISATION("hasOrganisation"), HAS_PERSON("hasPerson"), HAS_POST("hasPost"),
	HAS_TAG("hasTag"), IS_LOCATED("isLocatedIn"), IS_PART_OF("isPartOf"), KNOWS("knows"),
	LIKES("likes"), REPLY_OF("replyOf"), STUDY_AT("studyAt"), SUB_CLASS_OF("subClassOf"),
	URL("url"), WORK_AT("workAt");

		
	private String label;

	DimensionType(String label) {
		this.label = label;
	}

	public String getLabel() {
		return label;
	}
	
    public static DimensionType getEnumByName(String name){
    	for(DimensionType dimension : DimensionType.values()){
            if(name.equals(dimension.getLabel())) {
            	return dimension;
            }
        }
        return null;
    }
}
