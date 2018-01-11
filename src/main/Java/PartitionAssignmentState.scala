import org.apache.kafka.common.Node

class PartitionAssignmentState(group: String, coordinator: Option[Node], topic: Option[String],
                               partition: Option[Int], offset: Option[Long], lag: Option[Long],
                               consumerId: Option[String], host: Option[String],
                               clientId: Option[String], logEndOffset: Option[Long])
