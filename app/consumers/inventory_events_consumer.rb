# frozen_string_literal: true

# Receives messages from the Kafka topic, converts them into jobs
# for processing
class InventoryEventsConsumer < ApplicationConsumer
  subscribes_to Settings.platform_kafka_inventory_topic

  include ReportParsing

  def process(message)
    super(message)

    if service == 'compliance'
      produce(parse_report, topic: Settings.platform_kafka_validation_topic)
    end

    case @msg_value['type']
    when 'delete'
      DeleteHost.perform_async(@msg_value)
    when 'updated'
      InventoryHostUpdatedJob.perform_async(@msg_value)
    end
  end
end
