case class RawLogEntry(
	event: String,
	entityType: String,
	timestamp: Long,
	productID: String,
	listProduct: List[String],
	properties: Properties
	)