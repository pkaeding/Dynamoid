# encoding: utf-8
module Dynamoid #:nodoc:
  module Indexes

    class GlobalSecondaryIndex
      attr_accessor :source, :name, :hash_key, :range_key, :options
      alias_method :range_key?, :range_key

      # Create a new global secondary index.
      #
      # @param [Class] source the source class for the index
      # @param [Symbol] name the name of the index
      #
      # @since 0.8.0      
      def initialize(source, name, options = {})
        @source = source
        
        if options[:range_key]
          @range_key = options[:range_key]
        end
        @options = options

        @hash_key = name
        @name = sort([hash_key, range_key])
        
        raise Dynamoid::Errors::InvalidField, 'A key specified for an index is not a field' unless keys.all?{|n| source.attributes.include?(n)}
      end

      def is_real_index
        true
      end

      def table_name
        source.table_name
      end

      # Given either an object or a list of attributes, generate a hash key and a range key for the index. Optionally pass in 
      # true to changed_attributes for a list of all the object's dirty attributes in convenient index form (for deleting stale 
      # information from the indexes).
      #
      # @param [Object] attrs either an object that responds to :attributes, or a hash of attributes
      #
      # @return [Hash] a hash with the keys :hash_value and :range_value
      #
      # @since 0.8.0
      def values(attrs, changed_attributes = false)
        if changed_attributes
          hash = {}
          attrs.changes.each {|k, v| hash[k.to_sym] = (v.first || v.last)}
          attrs = hash
        end
        attrs = attrs.send(:attributes) if attrs.respond_to?(:attributes)
        {}.tap do |hash|
          hash[:hash_value] = attrs[hash_key]
          hash[:range_value] = attrs[range_key]
        end
      end

      # Returns the projected fields (other than the keys values and the id) for the index
      #
      # @since 0.8.0
      def projection
        options[:projection] || []
      end

      # Returns the read_capacity for this table.
      #
      # @since 0.8.0
      def read_capacity
        options[:read_capacity] || Dynamoid::Config.read_capacity
      end

      # Returns the write_capacity for this table.
      #
      # @since 0.8.0
      def write_capacity
        options[:write_capacity] || Dynamoid::Config.write_capacity
      end

      # Return the array of keys this index uses for its table.
      #
      # @since 0.8.0      
      def keys
        ([hash_key] + Array(range_key)).uniq
      end

      # Return the name for this index.
      #
      # @since 0.8.0
      def index_name
        "index_#{name.collect(&:to_s).collect(&:pluralize).join('_and_')}"
      end
      
      # Sort objects into alphabetical strings, used for composing index names correctly (since we always assume they're alphabetical).
      #
      # @example find all users by first and last name
      #   sort([:gamma, :alpha, :beta, :omega]) # => [:alpha, :beta, :gamma, :omega]
      #
      # @since 0.2.0         
      def sort(objs)
        Array(objs).flatten.compact.uniq.collect(&:to_s).sort.collect(&:to_sym)
      end

      # A hash describing the parameters of this index, to be used to create the index upon table creation
      #
      # @since 0.8.0
      def to_hash
        key_schema = [
            {
              :attribute_name => @hash_key.to_s,
              :key_type => 'HASH'
            }
          ]
        if range_key?
          key_schema << {
            :attribute_name => @range_key.to_s,
            :key_type => 'RANGE'
          }
        end
        projection = 
          if self.projection.empty?
            { :projection_type => 'KEYS_ONLY' }
          else
            { 
              :projection_type => 'INCLUDE', 
              :non_key_attributes => self.projection.map(&:to_s)
            }
          end

        {
          :index_name => self.index_name,
          :key_schema => key_schema,
          :projection => projection,
          :provisioned_throughput => {
            :read_capacity_units => self.read_capacity,
            :write_capacity_units => self.write_capacity
          }
        }
      end
    end
  end
end