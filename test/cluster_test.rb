require_relative 'helper'

# ruby -w -Itest test/cluster_test.rb
class TestCluster < Test::Unit::TestCase
  include Helper::Cluster

  def test_extract_key
    nodes = (7000..7005).map { |port| "redis://127.0.0.1:#{port}" }

    redis = Redis::Cluster.new(nodes)

    assert_equal 'dogs:1', redis.send(:extract_key, :get, 'dogs:1')
    assert_equal 'user1000', redis.send(:extract_key, :get, '{user1000}.following')
    assert_equal 'user1000', redis.send(:extract_key, :get, '{user1000}.followers')
    assert_equal 'foo{}{bar}', redis.send(:extract_key, :get, 'foo{}{bar}')
    assert_equal '{bar', redis.send(:extract_key, :get, 'foo{{bar}}zap')
    assert_equal 'bar', redis.send(:extract_key, :get, 'foo{bar}{zap}')
    assert_equal '', redis.send(:extract_key, :get, '')
    assert_equal '', redis.send(:extract_key, :get, nil)
    assert_equal '', redis.send(:extract_key, :get)
    assert_equal '', redis.send(:extract_key, :info)
    assert_equal '', redis.send(:extract_key, :multi)
    assert_equal '', redis.send(:extract_key, :exec)
    assert_equal '', redis.send(:extract_key, :slaveof)
    assert_equal '', redis.send(:extract_key, :config)
    assert_equal '', redis.send(:extract_key, :shutdown)
  end

  def test_cluster_slots
    nodes = (7000..7005).map { |port| "redis://127.0.0.1:#{port}" }

    redis = Redis::Cluster.new(nodes)
    slots = redis.cluster(:slots)
    sample_slot = slots.first

    assert_equal 3, slots.length
    assert_equal true, sample_slot.key?(:start_slot)
    assert_equal true, sample_slot.key?(:end_slot)
    assert_equal true, sample_slot.key?(:master)
    assert_equal true, sample_slot.fetch(:master).key?(:ip)
    assert_equal true, sample_slot.fetch(:master).key?(:port)
    assert_equal true, sample_slot.fetch(:master).key?(:node_id)
    assert_equal true, sample_slot.key?(:replicas)
    assert_equal true, sample_slot.fetch(:replicas).is_a?(Array)
    assert_equal true, sample_slot.fetch(:replicas).first.key?(:ip)
    assert_equal true, sample_slot.fetch(:replicas).first.key?(:port)
    assert_equal true, sample_slot.fetch(:replicas).first.key?(:node_id)
  end

  def test_cluster_keyslot
    nodes = (7000..7005).map { |port| "redis://127.0.0.1:#{port}" }

    redis = Redis::Cluster.new(nodes)

    assert_equal Redis::Cluster::KeySlotConverter.convert('hogehoge'), redis.cluster(:keyslot, 'hogehoge')
    assert_equal Redis::Cluster::KeySlotConverter.convert('12345'), redis.cluster(:keyslot, '12345')
    assert_equal Redis::Cluster::KeySlotConverter.convert('foo'), redis.cluster(:keyslot, 'boo{foo}woo')
    assert_equal Redis::Cluster::KeySlotConverter.convert('antirez.is.cool'), redis.cluster(:keyslot, 'antirez.is.cool')
    assert_equal Redis::Cluster::KeySlotConverter.convert(''), redis.cluster(:keyslot, '')
  end

  def test_cluster_nodes
    nodes = (7000..7005).map { |port| "redis://127.0.0.1:#{port}" }

    redis = Redis::Cluster.new(nodes)
    cluster_nodes = redis.cluster(:nodes)
    sample_node = cluster_nodes.first

    assert_equal 6, cluster_nodes.length
    assert_equal true, sample_node.key?(:node_id)
    assert_equal true, sample_node.key?(:ip_port)
    assert_equal true, sample_node.key?(:flags)
    assert_equal true, sample_node.key?(:master_node_id)
    assert_equal true, sample_node.key?(:ping_sent)
    assert_equal true, sample_node.key?(:pong_recv)
    assert_equal true, sample_node.key?(:config_epoch)
    assert_equal true, sample_node.key?(:link_state)
    assert_equal true, sample_node.key?(:slots)
  end

  def test_cluster_slaves
    nodes = (7000..7005).map { |port| "redis://127.0.0.1:#{port}" }

    redis = Redis::Cluster.new(nodes)
    cluster_nodes = redis.cluster(:nodes)

    sample_master_node_id = cluster_nodes.find { |n| n.fetch(:master_node_id) == '-' }.fetch(:node_id)
    sample_slave_node_id = cluster_nodes.find { |n| n.fetch(:master_node_id) != '-' }.fetch(:node_id)

    assert_equal 'slave', redis.cluster(:slaves, sample_master_node_id).first.fetch(:flags).first
    assert_raise(Redis::CommandError, 'ERR The specified node is not a master') do
      redis.cluster(:slaves, sample_slave_node_id)
    end
  end

  def test_cluster_info
    nodes = (7000..7005).map { |port| "redis://127.0.0.1:#{port}" }

    redis = Redis::Cluster.new(nodes)
    info = redis.cluster(:info)

    assert_equal '3', info.fetch(:cluster_size)
  end

  def test_asking
    nodes = (7000..7005).map { |port| "redis://127.0.0.1:#{port}" }

    redis = Redis::Cluster.new(nodes)

    assert_equal 'OK', redis.asking
  end

  def test_client_works_even_if_so_many_unavailable_nodes_specified
    nodes = (6001..7005).map { |port| "redis://127.0.0.1:#{port}" }

    redis = Redis::Cluster.new(nodes)
    assert_equal 'PONG', redis.ping
  end

  def test_client_accepts_valid_node_configs
    nodes = ['redis://127.0.0.1:7000',
             'redis://127.0.0.1:7001',
             { host: '127.0.0.1', port: '7002' },
             { 'host' => '127.0.0.1', port: 7003 },
             'redis://127.0.0.1:7004',
             'redis://127.0.0.1:7005']

    assert_nothing_raised do
      Redis::Cluster.new(nodes)
    end
  end

  def test_well_known_commands_work
    nodes = (7000..7005).map { |port| "redis://127.0.0.1:#{port}" }

    redis = Redis::Cluster.new(nodes)

    100.times { |i| redis.set(i.to_s, "hogehoge#{i}") }
    100.times { |i| assert_equal "hogehoge#{i}", redis.get(i.to_s) }
    assert_equal '1', redis.info['cluster_enabled']
  end

  def test_hash_tags_work_on_multi_key_commands
    nodes = (7000..7005).map { |port| "redis://127.0.0.1:#{port}" }

    redis = Redis::Cluster.new(nodes)

    assert_raise(Redis::CommandError, "CROSSSLOT Keys in request don't hash to the same slot") do
      redis.mset('Presidents.of.USA:1', 'George Washington',
                 'Presidents.of.USA:2', 'John Adams',
                 'Presidents.of.USA:3', 'Thomas Jefferson')
    end

    assert_raise(Redis::CommandError, "CROSSSLOT Keys in request don't hash to the same slot") do
      redis.mget('Presidents.of.USA:1', 'Presidents.of.USA:2',
                 'Presidents.of.USA:3')
    end

    assert_nothing_raised do
      redis.mset('{Presidents.of.USA}:1', 'George Washington',
                 '{Presidents.of.USA}:2', 'John Adams',
                 '{Presidents.of.USA}:3', 'Thomas Jefferson')
    end

    assert_equal(['George Washington', 'John Adams', 'Thomas Jefferson'],
                 redis.mget('{Presidents.of.USA}:1', '{Presidents.of.USA}:2',
                            '{Presidents.of.USA}:3'))
  end

  def test_pipelining_with_a_hash_tag
    nodes = (7000..7005).map { |port| "redis://127.0.0.1:#{port}" }
    redis = Redis::Cluster.new(nodes)

    p1 = p2 = p3 = p4 = p5 = p6 = nil

    redis.pipelined do |r|
      r.set('{Presidents.of.USA}:1', 'George Washington')
      r.set('{Presidents.of.USA}:2', 'John Adams')
      r.set('{Presidents.of.USA}:3', 'Thomas Jefferson')
      r.set('{Presidents.of.USA}:4', 'James Madison')
      r.set('{Presidents.of.USA}:5', 'James Monroe')
      r.set('{Presidents.of.USA}:6', 'John Quincy Adams')

      p1 = r.get('{Presidents.of.USA}:1')
      p2 = r.get('{Presidents.of.USA}:2')
      p3 = r.get('{Presidents.of.USA}:3')
      p4 = r.get('{Presidents.of.USA}:4')
      p5 = r.get('{Presidents.of.USA}:5')
      p6 = r.get('{Presidents.of.USA}:6')
    end

    [p1, p2, p3, p4, p5, p6].each do |actual|
      assert_true actual.is_a?(Redis::Future)
    end

    assert_equal('George Washington', p1.value)
    assert_equal('John Adams',        p2.value)
    assert_equal('Thomas Jefferson',  p3.value)
    assert_equal('James Madison',     p4.value)
    assert_equal('James Monroe',      p5.value)
    assert_equal('John Quincy Adams', p6.value)
  end

  def test_pipelining_without_hash_tags
    nodes = (7000..7005).map { |port| "redis://127.0.0.1:#{port}" }
    redis = Redis::Cluster.new(nodes)

    a = b = c = d = e = f = nil

    redis.pipelined do
      redis.set(:a, 1)
      redis.set(:b, 2)
      redis.set(:c, 3)
      redis.set(:d, 4)
      redis.set(:e, 5)
      redis.set(:f, 6)

      a = redis.get(:a)
      b = redis.get(:b)
      c = redis.get(:c)
      d = redis.get(:d)
      e = redis.get(:e)
      f = redis.get(:f)
    end

    [a, b, c, d, e, f].each_with_index do |actual, i|
      expected = (i + 1).to_s

      if actual.is_a?(Redis::Future)
        assert_equal(expected, actual.value)
      else
        assert_equal(expected, actual)
      end
    end
  end

  def test_client_respond_to_commands
    nodes = (7000..7005).map { |port| "redis://127.0.0.1:#{port}" }

    redis = Redis::Cluster.new(nodes)

    assert_equal true, redis.respond_to?(:set)
    assert_equal true, redis.respond_to?('set')
    assert_equal true, redis.respond_to?(:get)
    assert_equal true, redis.respond_to?('get')
    assert_equal true, redis.respond_to?(:cluster)
    assert_equal true, redis.respond_to?(:asking)
    assert_equal false, redis.respond_to?(:unknown_method)
  end

  def test_unknown_command_does_not_work
    nodes = (7000..7005).map { |port| "redis://127.0.0.1:#{port}" }

    redis = Redis::Cluster.new(nodes)

    assert_raise(NoMethodError) do
      redis.not_yet_implemented_command('boo', 'foo')
    end
  end

  def test_client_accepts_valid_options
    nodes = (7000..7005).map { |port| "redis://127.0.0.1:#{port}" }

    assert_nothing_raised do
      Redis::Cluster.new(nodes, timeout: 1.0)
    end
  end

  def test_client_ignores_invalid_options
    nodes = (7000..7005).map { |port| "redis://127.0.0.1:#{port}" }

    assert_nothing_raised do
      Redis::Cluster.new(nodes, invalid_option: true)
    end
  end

  def test_client_does_not_accept_db_specified_url
    assert_raise(Redis::CannotConnectError, 'Could not connect to any nodes') do
      Redis::Cluster.new(['redis://127.0.0.1:7000/1/namespace'])
    end

    assert_raise(Redis::CannotConnectError, 'Could not connect to any nodes') do
      Redis::Cluster.new([{ host: '127.0.0.1', port: '7000' }], db: 1)
    end
  end

  def test_client_does_not_accept_unconnectable_node_url_only
    nodes = ['redis://127.0.0.1:7006']

    assert_raise(Redis::CannotConnectError, 'Could not connect to any nodes') do
      Redis::Cluster.new(nodes)
    end
  end

  def test_client_accepts_unconnectable_node_url_included
    nodes = ['redis://127.0.0.1:7000', 'redis://127.0.0.1:7006']

    assert_nothing_raised(Redis::CannotConnectError, 'Could not connect to any nodes') do
      Redis::Cluster.new(nodes)
    end
  end

  def test_client_does_not_accept_http_scheme_url
    nodes = ['http://127.0.0.1:80']

    assert_raise(ArgumentError, "invalid uri scheme 'http'") do
      Redis::Cluster.new(nodes)
    end
  end

  def test_client_does_not_accept_blank_included_config
    nodes = ['']

    assert_raise(ArgumentError, "invalid uri scheme ''") do
      Redis::Cluster.new(nodes)
    end
  end

  def test_client_does_not_accept_bool_included_config
    nodes = [true]

    assert_raise(ArgumentError, "invalid uri scheme ''") do
      Redis::Cluster.new(nodes)
    end
  end

  def test_client_does_not_accept_nil_included_config
    nodes = [nil]

    assert_raise(ArgumentError, "invalid uri scheme ''") do
      Redis::Cluster.new(nodes)
    end
  end

  def test_client_does_not_accept_array_included_config
    nodes = [[]]

    assert_raise(ArgumentError, "invalid uri scheme ''") do
      Redis::Cluster.new(nodes)
    end
  end

  def test_client_does_not_accept_empty_hash_included_config
    nodes = [{}]

    assert_raise(KeyError, 'key not found: :host') do
      Redis::Cluster.new(nodes)
    end
  end

  def test_client_does_not_accept_object_included_config
    nodes = [Object.new]

    assert_raise(ArgumentError, 'Redis Cluster node config must includes String or Hash') do
      Redis::Cluster.new(nodes)
    end
  end

  def test_client_does_not_accept_not_array_config
    nodes = :not_array

    assert_raise(ArgumentError, 'Redis Cluster node config must be Array') do
      Redis::Cluster.new(nodes)
    end
  end
end