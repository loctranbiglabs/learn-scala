case class RawLogEntry(
	event: String,
	entityType: String,
	timestamp: Long,
	productId: String,
	listProduct: List[String],
	properties: Properties
	)