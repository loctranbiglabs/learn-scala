case class EventLogEntry(
				event: String,
				entityType: String,
				productId: String,
				sessionId: String,
				timestamp: String,
				listProductId: String*
)