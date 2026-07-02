# frozen_string_literal: true

module Kafka
  # Service for marking a KafkaSystem as soft-deleted (tombstoned) in the systems table
  class SystemRemover
    def initialize(message, logger)
      @message = message
      @logger = logger
      @id = @message.dig('id')
      @org_id = @message.dig('org_id')
    end

    def remove_system
      # rubocop:disable Layout/LineLength
      # rubocop:disable Rails/SkipsModelValidations
      KafkaSystem.where(id: @id)
                 .where('updated IS NULL OR updated < ?', delete_timestamp)
                 .update_all(deleted_at: delete_timestamp)
      # rubocop:enable Rails/SkipsModelValidations
      # rubocop:enable Layout/LineLength
    end

    private

    def delete_timestamp
      timestamp_str = @message.dig('updated') || @message.dig('timestamp')
      Time.zone.parse(timestamp_str)
    rescue StandardError
      Time.current
    end
  end
end
