# Workers for priority-queue routing specs.
class PriorityP0Worker
  include KafkaBatch::Worker
  kafka_topic "kafka_batch.jobs.p0"

  def perform(_payload); end
end

class PriorityP1Worker
  include KafkaBatch::Worker
  kafka_topic "kafka_batch.jobs.p1"

  def perform(_payload); end
end
