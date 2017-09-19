gem 'google-cloud-pubsub', '~> 0.27.0'
require 'google/cloud/pubsub'
require 'thread'
require 'action_cable/subscription_adapter/inline'

module ActionCable
  module SubscriptionAdapter
    class GooglePubsub < Base # :nodoc:
      PubSubMessage = Struct.new :channel, :message

      # Overwrite this factory method for redis connections if you want to use a different Redis library than Redis.
      # This is needed, for example, when using Makara proxies for distributed Redis.
      cattr_accessor(:google_pubsub_connector) { ->(config) {
        Google::Cloud::Pubsub.new(project: config[:project], keyfile: config[:keyfile])
      } }

      def initialize(*)
        super
        @listener = nil
        #@redis_connection_for_broadcasts = nil
        @google_pubsub_connection_for_broadcasts = nil
      end

      # broadcast payload through channel
      def broadcast(channel, payload)
        #redis_connection_for_broadcasts.publish(channel, payload)
        # Todo: Maybe this can be pubsub publish for a specific channel. There still needs to be an accept payload.
        message = PubSubMessage.new channel, payload

        google_pubsub_connection_for_broadcasts.publish Marshal.dump(message)
      end

      # Todo: Calls Listener::add_subscriber which maps a websocket connection to the subscriber_map.
      def subscribe(channel, callback, success_callback = nil)
        listener.add_subscriber(channel, callback, success_callback)
      end

      # Todo: Calls Listener::remove_subscriber which removes a websocket connection from the subscriber_map.
      def unsubscribe(channel, callback)
        listener.remove_subscriber(channel, callback)
      end

      def shutdown
        @listener.shutdown if @listener
      end

      # def redis_connection_for_subscriptions
      #   redis_connection
      # end

      def google_pubsub_connection_for_subscription
        conn = google_pubsub_connection
        conn.subscription @server.config.cable[:subscription]
      end

      private
        def listener
          @listener || @server.mutex.synchronize { @listener ||= Listener.new(self, @server.event_loop) }
        end

        # def redis_connection_for_broadcasts
        #   @redis_connection_for_broadcasts || @server.mutex.synchronize do
        #     @redis_connection_for_broadcasts ||= redis_connection
        #   end
        # end

        def google_pubsub_connection_for_broadcasts
          @google_pubsub_connection_for_broadcasts || @server.mutex.synchronize do
            @google_pubsub_connection_for_broadcasts ||= google_pubsub_connection.topic @server.config.cable[:topic]
          end
        end

        # def redis_connection
        #   self.class.redis_connector.call(@server.config.cable)
        # end

        def google_pubsub_connection
          # WTF IS THIS???
          # Does this call line 11 -> cattr_accessor? I think so...
          self.class.google_pubsub_connector.call(@server.config.cable)
        end

        class Listener < SubscriberMap
          def initialize(adapter, event_loop)
            super()

            @adapter = adapter
            @event_loop = event_loop

            @subscribe_callbacks = Hash.new { |h, k| h[k] = [] }
            @subscription_lock = Mutex.new

            @raw_client = nil

            @when_connected = []

            @thread = nil
          end

          # Todo: What is conn and what does this method do?
          # conn == pubsub client
          def listen(conn)
            p conn
            subscriber = conn.listen do |msg|
              msg.ack!
              puts "Received message: #{msg.inspect}"
              # Security Risk!
              pubsub_message = Marshal.load msg.message.data

              broadcast pubsub_message.channel, pubsub_message.message
            end

            subscriber.start
            # conn.without_reconnect do
            #   original_client = conn.client

            #   conn.subscribe('_action_cable_internal') do |on|
            #     on.subscribe do |chan, subscriptions|
            #       @subscription_lock.synchronize do
            #         if subscriptions == 1
            #           @raw_client = original_client

            #           until @when_connected.empty?
            #             @when_connected.shift.call
            #           end
            #         end

            #         if callbacks = @subscribe_callbacks[chan]
            #           next_callback = callbacks.shift
            #           @event_loop.post(&next_callback) if next_callback
            #           @subscribe_callbacks.delete(chan) if callbacks.empty?
            #         end
            #       end
            #     end

            #     on.message do |chan, message|
            #       broadcast(chan, message)
            #     end

            #     on.unsubscribe do |chan, subscriptions|
            #       if subscriptions == 0
            #         @subscription_lock.synchronize do
            #           @raw_client = nil
            #         end
            #       end
            #     end
            #   end
            # end
          end

          def shutdown
            @subscription_lock.synchronize do
              return if @thread.nil?

              when_connected do
                send_command('unsubscribe')
                @raw_client = nil
              end
            end

            Thread.pass while @thread.alive?
          end

          def add_channel(channel, on_success)
            @subscription_lock.synchronize do
              ensure_listener_running
              @subscribe_callbacks[channel] << on_success
              when_connected { send_command('subscribe', channel) }
            end
          end

          def remove_channel(channel)
            @subscription_lock.synchronize do
              when_connected { send_command('unsubscribe', channel) }
            end
          end

          def invoke_callback(*)
            @event_loop.post { super }
          end

          private
            def ensure_listener_running
              @thread ||= Thread.new do
                Thread.current.abort_on_exception = true

                conn = @adapter.google_pubsub_connection_for_subscription
                listen conn
              end
            end

            def when_connected(&block)
              if @raw_client
                block.call
              else
                @when_connected << block
              end
            end

            def send_command(*command)
              @raw_client.write(command)

              very_raw_connection =
                @raw_client.connection.instance_variable_defined?(:@connection) &&
                @raw_client.connection.instance_variable_get(:@connection)

              if very_raw_connection && very_raw_connection.respond_to?(:flush)
                very_raw_connection.flush
              end
            end
        end
    end
  end
end
