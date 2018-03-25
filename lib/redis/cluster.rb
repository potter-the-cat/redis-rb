require_relative 'cluster/key_slot_converter'

class Redis
  # Redis Cluster client
  #
  # @see https://github.com/antirez/redis-rb-cluster POC implementation
  # @see https://redis.io/topics/cluster-spec Redis Cluster specification
  # @see https://redis.io/topics/cluster-tutorial Redis Cluster tutorial
  #
  # Copyright (C) 2013 Salvatore Sanfilippo <antirez@gmail.com>
  class Cluster
    KEYLESS_COMMANDS = %i[info multi exec slaveof config shutdown].freeze
    RETRY_COUNT = 16
    RETRY_WAIT_SEC = 0.1

    # Create a new client instance.
    #
    # @example How to use of simple
    #   nodes = (7000..7005).map { |port| "redis://127.0.0.1:#{port}" }
    #   redis = Redis::Cluster.new(nodes)
    #   redis.set(:hogehoge, 1)
    #
    # @example How to use with options
    #   nodes = (7000..7005).map { |port| "redis://127.0.0.1:#{port}" }
    #   redis = Redis::Cluster.new(nodes, timeout: 1)
    #   redis.set(:hogehoge, 1)
    #
    # @param node_addrs [Array<String, Hash>] the list of node addresses
    #   to contact
    # @param options [Hash] same as the `Redis` constractor
    #
    # @return [Redis::Cluster] a new client instance
    def initialize(node_addrs, options = {})
      raise ArgumentError, 'Redis Cluster node config must be Array' unless node_addrs.is_a?(Array)

      startup_nodes = build_clients_per_node(node_addrs, options)
      available_slots = fetch_available_slots_per_node(startup_nodes.values)

      raise CannotConnectError, 'Could not connect to any nodes' if available_slots.nil?

      available_node_addrs = extract_available_node_addrs(available_slots)
      @available_nodes = build_clients_per_node(available_node_addrs, options)
      @slot_node_key_maps = build_slot_node_key_maps(available_slots)
    end

    # Sends `CLUSTER *` command to random node and returns its reply.
    #
    # @see https://redis.io/commands#cluster Reference of cluster command
    #
    # @param command [String, Symbol] the subcommand of cluster command
    #   e.g. `:slots`, `:nodes`, `:slaves`, `:info`
    #
    # @return [Object] depends on the subcommand
    def cluster(command, *args, &block)
      response = try_cmd(find_node, :cluster, command, *args, &block)
      case command.to_s.downcase
      when 'slots' then cluster_slots(response)
      when 'nodes' then cluster_nodes(response)
      when 'slaves' then cluster_slaves(response)
      when 'info' then cluster_info(response)
      else response
      end
    end

    # Sends `ASKING` command to random node and returns its reply.
    #
    # @see https://redis.io/topics/cluster-spec#ask-redirection ASK redirection
    #
    # @return [String] `'OK'`
    def asking
      try_cmd(find_node, :synchronize) { |client| client.call(%i[asking]) }
    end

    private

    # Delegates to a instance of random node client and returns its reply.
    #
    # @param method_name [String] the method name
    # @param include_private [true, false] specify true if private methods needed
    #
    # @return [true, false] depends on implementation of the `Redis`
    def respond_to_missing?(method_name, include_private = false)
      find_node.respond_to?(method_name, include_private)
    end

    # Delegates to a instance of random node client and returns its reply.
    #
    # @param method_name [String, Symbol] the method name e.g. `:set`, `:get`
    #
    # @return [Object] depends on implementation of the `Redis`
    def method_missing(method_name, *args, &block)
      key = extract_key(method_name, *args)
      slot = key.empty? ? nil : KeySlotConverter.convert(key)
      node = find_node(slot)
      return try_cmd(node, method_name, *args, &block) if node.respond_to?(method_name)

      super
    end

    # Finds and returns a instance of node client.
    #
    # @param slot [nil, Integer] the slot number
    #
    # @return [Redis] a instance of node client related to the slot number
    # @return [Redis] a instance of random node client if the slot number is nil
    # @return [nil] nil if client not cached slot information
    def find_node(slot = nil)
      return nil unless instance_variable_defined?(:@available_nodes)
      return @available_nodes.values.sample if slot.nil? || !@slot_node_key_maps.key?(slot)

      node_key = @slot_node_key_maps[slot]
      @available_nodes.fetch(node_key)
    end

    # Sends the command and returns its reply. Redirections may occur.
    #
    # @see https://redis.io/topics/cluster-spec#redirection-and-resharding
    #   Redirection and resharding
    #
    # @param node [Redis] the instance of node client
    # @param command [String, Symbol] the command of redis
    # @param ttl [Integer] the limit of count for retry or redirection
    #
    # @return [Object] depends on the command
    def try_cmd(node, command, *args, ttl: RETRY_COUNT, &block)
      ttl -= 1
      node.send(command, *args, &block)
    rescue TimeoutError, CannotConnectError, Errno::ECONNREFUSED, Errno::EACCES => err
      raise err if ttl <= 0
      sleep(RETRY_WAIT_SEC)
      node = find_node || node
      retry
    rescue CommandError => err
      if err.message.start_with?('MOVED')
        redirection_node(err.message).send(command, *args, &block)
      elsif err.message.start_with?('ASK')
        raise err if ttl <= 0
        asking
        retry
      else
        raise err
      end
    end

    # Parse redirection error message,
    #   and returns a instance of destination node client,
    #   and updates slot-node mapping cache (side effect!).
    #
    # @param err_msg [String] the message of MOVED redirection error
    #   e.g. `'MOVED 3999 127.0.0.1:6381'`
    #
    # @return [Redis] a instance of destination node client
    def redirection_node(err_msg)
      _, slot, node_key = err_msg.split(' ')
      @slot_node_key_maps[slot.to_i] = node_key
      find_node(slot.to_i)
    end

    # Creates client instances per node.
    #
    # @example When string included array specified
    #   build_clients_per_node(['redis://127.0.0.1:6379'])
    #   #=> { '127.0.0.1:6379' => Redis.new(url: 'redis://127.0.0.1:6379') }
    #
    # @example When hash included array specified
    #   build_clients_per_node([{ host: '127.0.0.1', port: 6379 }])
    #   #=> { '127.0.0.1:6379' => Redis.new(host: '127.0.0.1', port: 6379) }
    #
    # @example With options
    #   build_clients_per_node(['redis://127.0.0.1:6379'], driver: :hiredis)
    #   #=> { '127.0.0.1:6379' => Redis.new(url: 'redis://127.0.0.1:6379', driver: :hiredis) }
    #
    # @param node_addrs [Array<String, Hash>] the list of node addresses
    #   to contact
    # @param options [Hash] same as the `Redis` constractor
    #
    # @return [Hash{String => Redis}] client instances per `'ip:port'`
    def build_clients_per_node(node_addrs, options)
      node_addrs.map do |addr|
        option = to_client_option(addr)
        [to_node_key(option), Redis.new(options.merge(option))]
      end.to_h
    end

    # Converts node address into client options.
    #
    # @example When string value specified
    #   to_client_option('redis://127.0.0.1:6379')
    #   #=> { url: 'redis://127.0.0.1:6379' }
    #
    # @example When hash value specified
    #   to_client_option(host: '127.0.0.1', port: 6379)
    #   #=> { host: '127.0.0.1', port: 6379 }
    #
    # @param addr [String, Hash] the node address
    #   e.g. `'redis://127.0.0.1:6379'`, `{ host: '127.0.0.1', port: 6379 }`
    #
    # @return [Hash{Symbol => String, Integer}] converted options
    #
    # @raise [ArgumentError] if addr is not a `String` or `Hash`
    def to_client_option(addr)
      if addr.is_a?(String)
        { url: addr }
      elsif addr.is_a?(Hash)
        addr = addr.map { |k, v| [k.to_sym, v] }.to_h
        { host: addr.fetch(:host), port: addr.fetch(:port) }
      else
        raise ArgumentError, 'Redis Cluster node addr must includes String or Hash'
      end
    end

    # Converts client option into key of node address.
    #
    # @example When :url key included hash specified
    #   to_node_key(url: 'redis://127.0.0.1:6379')
    #   #=> '127.0.0.1:6379'
    #
    # @example When :url key included hash specified
    #   to_node_key(host: '127.0.0.1', port: 6379)
    #   #=> '127.0.0.1:6379'
    #
    # @param option [Hash{Symbol => String, Integer}] the client option
    #   e.g. `{ url: 'redis://127.0.0.1:6379' }`,
    #   `{ host: '127.0.0.1', port: 6379 }`
    #
    # @return [String] a node key of address e.g. `'127.0.0.1:6379'`
    def to_node_key(option)
      return option[:url].gsub(%r{rediss?://}, '') if option.key?(:url)

      "#{option[:host]}:#{option[:port]}"
    end

    # Fetch cluster slot info on available node.
    #
    # @example Basic example
    #   fetch_available_slots_per_node([Redis.new(url: 'redis://127.0.0.1:6379')])
    #   #=> { '127.0.0.1:6379' => (0..12345) }
    #
    # @param startup_nodes [Array<Redis>] the list of start-up node clients
    #
    # @return [Hash{String => Range}] slot ranges per key of node address
    def fetch_available_slots_per_node(startup_nodes)
      slot_info = nil

      startup_nodes.each do |node|
        begin
          slot_info = fetch_slot_info(node, ttl: 1)
        rescue CannotConnectError, CommandError
          next
        end

        break
      end

      slot_info
    end

    # Try fetch cluster slot info and converts it into slot range data per node.
    #
    # @example Basic example
    #   fetch_slot_info(Redis.new(url: 'redis://127.0.0.1:6379'))
    #   #=> { '127.0.0.1:6379' => (0..12345) }
    #
    # @param node [Redis] the instance of node client
    # @param [Integer] ttl the limit of count for retry or redirection
    #
    # @return [Hash{String => Range}] slot ranges per key of node address
    def fetch_slot_info(node, ttl: RETRY_COUNT)
      try_cmd(node, :cluster, :slots, ttl: ttl).map do |slot_info|
        first_slot, last_slot = slot_info[0..1]
        ip, port = slot_info[2]
        ["#{ip}:#{port}", (first_slot..last_slot)]
      end.to_h
    end

    # Extracts node addresses from slot info.
    #
    # @example Basic example
    #   extract_available_node_addrs('127.0.0.1:6379' => (0..12345))
    #   #=> [{ host: '127.0.0.1', port: 6379 }]
    #
    # @param available_slots [Hash{String => Range}] the cluster slot info
    #
    # @return [Array<Hash>] available node addresses
    def extract_available_node_addrs(available_slots)
      available_slots
        .keys
        .map { |k| k.split(':') }
        .map { |k| { host: k[0], port: k[1] } }
    end

    # Creates cache of slot-node mapping.
    #
    # @example Basic example
    #   build_slot_node_key_maps('127.0.0.1:6379' => (0..2))
    #   #=> { 0 => '127.0.0.1:6379', 1 => '127.0.0.1:6379', 2 => '127.0.0.1:6379' }
    #
    # @param available_slots [Hash{String => Range}] the cluster slot info
    #
    # @return [Hash{Integer => String}] cache of slot-node mapping
    def build_slot_node_key_maps(available_slots)
      available_slots.each_with_object({}) do |(node_key, slots), m|
        slots.each { |slot| m[slot] = node_key }
      end
    end

    # Extracts command key from arguments.
    #
    # @example When normal key specified
    #   extract_key(:get, 'hogehoge')
    #   #=> 'hogehoge'
    #
    # @example When hash tag included key specified
    #   extract_key(:get, 'boo{foo}woo')
    #   #=> 'foo'
    #
    # @example When key less command specified
    #   extract_key(:info, 'fugafuga')
    #   #=> ''
    #
    # @see https://redis.io/topics/cluster-spec#keys-hash-tags Keys hash tags
    #
    # @param command [String] the command of the redis
    #
    # @return [String] a key of the redis command
    # @return [String] a blank if keyless command specified
    # @return [String] a hash tag if hash tag specified
    def extract_key(command, *args)
      command = command.to_s.downcase.to_sym
      return '' if KEYLESS_COMMANDS.include?(command)

      key = args.first.to_s
      hash_tag = extract_hash_tag(key)
      hash_tag.empty? ? key : hash_tag
    end

    # Extracts hash tag from key.
    #
    # @see https://redis.io/topics/cluster-spec#keys-hash-tags Keys hash tags
    #
    # @param key [String] the key of the redis command
    #
    # @return [String] hash tag or blank
    def extract_hash_tag(key)
      s = key.index('{')
      e = key.index('}', s.to_i + 1)

      return '' if s.nil? || e.nil?

      key[s + 1..e - 1]
    end

    # Deserialize a node info.
    #
    # @example Basic example
    #   deserialize_node_info('07c37dfeb235213a872192d90877d0cd55635b91 127.0.0.1:6379 myself,master - 0 1426238317239 4 connected 0-12345')
    #   #=> { node_id: '07c37dfeb235213a872192d90877d0cd55635b91',
    #         ip_port: '127.0.0.1:6379',
    #         flags: ['myself', 'master'],
    #         master_node_id: '-',
    #         ping_sent: '0',
    #         pong_recv: '1426238317239',
    #         config_epoch: '4',
    #         link_state: 'connected',
    #         slots: (0..12345) }
    #
    # @param str [String] the raw data of node info
    #
    # @return [Hash{Symbol => String, Range, nil}] a node info
    def deserialize_node_info(str)
      arr = str.split(' ')
      {
        node_id: arr[0],
        ip_port: arr[1],
        flags: arr[2].split(','),
        master_node_id: arr[3],
        ping_sent: arr[4],
        pong_recv: arr[5],
        config_epoch: arr[6],
        link_state: arr[7],
        slots: arr[8].nil? ? nil : Range.new(*arr[8].split('-'))
      }
    end

    # Parse `CLUSTER SLOTS` command response raw data.
    #
    # @example Basic example
    #   cluster_slots([0, 12345, ['127.0.0.1', 6379, '09dbe9720cda62f7865eabc5fd8857c5d2678366'], ['127.0.0.1', 6380, '821d8ca00d7ccf931ed3ffc7e3db0599d2271abf']])
    #   #=> { start_slot: 0,
    #         end_slot: 12345,
    #         master: { ip: '127.0.0.1', port: 6379, node_id: '09dbe9720cda62f7865eabc5fd8857c5d2678366' },
    #         replicas: [{ ip: '127.0.0.1', port: 6380, node_id: '821d8ca00d7ccf931ed3ffc7e3db0599d2271abf' }] }
    #
    # @param response [Array<Array>] the raw data of slots info
    #
    # @return [Array<Hash>] parsed data
    def cluster_slots(response)
      response.map do |res|
        first_slot, last_slot = res[0..1]
        master = { ip: res[2][0], port: res[2][1], node_id: res[2][2] }
        replicas = res[3..-1].map { |r| { ip: r[0], port: r[1], node_id: r[2] } }
        { start_slot: first_slot, end_slot: last_slot, master: master, replicas: replicas }
      end
    end

    # Parse `CLUSTER NODES` command response raw data.
    #
    # @example Basic example
    #   cluster_nodes('07c37dfeb235213a872192d90877d0cd55635b91 127.0.0.1:6379 myself,master - 0 1426238317239 4 connected 0-12345')
    #   #=> [{ node_id: '07c37dfeb235213a872192d90877d0cd55635b91',
    #          ip_port: '127.0.0.1:6379',
    #          flags: ['myself', 'master'],
    #          master_node_id: '-',
    #          ping_sent: '0',
    #          pong_recv: '1426238317239',
    #          config_epoch: '4',
    #          link_state: 'connected',
    #          slots: (0..12345) }]
    #
    # @param response [String] the raw data of nodes info
    #
    # @return [Array<Hash>] parsed data
    def cluster_nodes(response)
      response
        .split(/[\r\n]+/)
        .map { |str| deserialize_node_info(str) }
    end

    # Parse `CLUSTER SLAVES` command response raw data.
    #
    # @example Basic example
    #   cluster_slaves(['07c37dfeb235213a872192d90877d0cd55635b91 127.0.0.1:6379 myself,master - 0 1426238317239 4 connected 0-12345'])
    #   #=> [{ node_id: '07c37dfeb235213a872192d90877d0cd55635b91',
    #          ip_port: '127.0.0.1:6379',
    #          flags: ['myself', 'master'],
    #          master_node_id: '-',
    #          ping_sent: '0',
    #          pong_recv: '1426238317239',
    #          config_epoch: '4',
    #          link_state: 'connected',
    #          slots: (0..12345) }]
    #
    # @param response [Array<String>] the raw data of slaves info
    #
    # @return [Array<Hash>] parsed data
    def cluster_slaves(response)
      response.map { |str| deserialize_node_info(str) }
    end

    # Parse `CLUSTER INFO` command response raw data.
    #
    # @example Basic example
    #   cluster_info('cluster_state:ok\r\ncluster_size:3')
    #   #=> { cluster_state: 'ok', cluster_size: '3' }
    #
    # @param response [String] the raw data of cluster info
    #
    # @return [Hash{Symbol => String}] parsed data
    def cluster_info(response)
      response
        .split(/[\r\n]+/)
        .map { |str| str.split(':') }
        .map { |arr| [arr.first.to_sym, arr[1]] }
        .to_h
    end
  end
end