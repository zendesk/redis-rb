# frozen_string_literal: true

require "helper"

# ruby -w -Itest test/cluster_client_pinning_test.rb
class TestClusterClientPinning < Minitest::Test
  include Helper::Cluster

  def test_pinning_locks_to_node
    res = []
    redis.with(key: '{slot}') do |conn|
      10.times do
        res << conn.cluster('myid')
      end
    end

    refute_nil(res.first)
    assert_equal([res.first], res.uniq)
  end

  def test_pinning_cannot_cross_slots
    assert_raises(Redis::Cluster::TransactionConsistencyError) do
      redis.with(key: '{slot}') do |conn|
        conn.get('{otherslot}key')
      end
    end
  end

  def test_pinning_pipeline
    redis.set('{slot}counter', 0)
    redis.with(key: '{slot}') do |conn|
      got = conn.pipelined do |pipe|
        5.times { pipe.incr('{slot}counter') }
      end
      assert_equal(5, conn.get('{slot}counter').to_i)
      assert_equal([1, 2, 3, 4, 5], got)
      got = conn.pipelined do |pipe|
        5.times { pipe.incr('{slot}counter') }
      end
      assert_equal(10, conn.get('{slot}counter').to_i)
      assert_equal([6, 7, 8, 9, 10], got)
    end
  end

  def test_pinning_nested_pipeline
    redis.with(key: '{slot}') do |conn|
      conn.pipelined do |p1|
        p1.set('{slot}foo', 's1')
        p1.pipelined do |p2|
          p2.set('{slot}bar', 's2')
        end
      end
    end
    assert_equal "s1", redis.get("{slot}foo")
    assert_equal "s2", redis.get("{slot}bar")
  end

  def test_pinning_with_multi
    redis2 = build_another_client
    redis.with(key: '{slot}') do |conn|
      conn.set('{slot}key', 0)
      conn.multi do |txn|
        txn.incr('{slot}key')
        assert_equal(0, redis2.get('{slot}key').to_i)
        txn.incr('{slot}key')
        assert_equal(0, redis2.get('{slot}key').to_i)
      end
      assert_equal(2, redis2.get('{slot}key').to_i)
    end
  end

  def test_pinning_with_multi_and_watch_success
    got = nil
    redis.with(key: '{slot}') do |conn|
      conn.mset('{slot}key1', 'val1', '{slot}key2', 'val2')
      conn.watch('{slot}key1', '{slot}key2') do
        old_val1, old_val2 = conn.mget('{slot}key1', '{slot}key2')
        got = conn.multi do |txn|
          txn.set('{slot}key1', old_val2)
          txn.set('{slot}key2', old_val1)
        end
      end
    end
    assert_equal(['OK', 'OK'], got)
    assert_equal(['val2', 'val1'], redis.mget('{slot}key1', '{slot}key2'))
  end

  def test_pinning_with_multi_and_watch_abort
    redis2 = build_another_client
    got = nil
    redis.with(key: '{slot}') do |conn|
      conn.mset('{slot}key1', 'val1', '{slot}key2', 'val2')
      conn.watch('{slot}key1', '{slot}key2') do
        old_val1, old_val2 = conn.mget('{slot}key1', '{slot}key2')

        redis2.set('{slot}key1', 'invalidated_val1')

        got = conn.multi do |txn|
          txn.mset('{slot}key1', old_val2, '{slot}key2', old_val1)
        end
      end
    end

    assert_nil(got)
    assert_equal(['invalidated_val1', 'val2'], redis.mget('{slot}key1', '{slot}key2'))
  end

  def test_pinning_with_multi_and_watch_raise
    ex = Class.new(StandardError)
    redis2 = build_another_client
    got = nil

    redis.with(key: '{slot}') do |conn|
      # Watch something, and then raise
      assert_raises(ex) do
        conn.watch('{slot}key1') do
          conn.multi do |_txn|
            raise ex, "boom"
          end
        end
      end

      # Key1 should no longer be watched.
      # Prove it by modifying it, and trying a multi with a _different_ key.
      # The txn will fail if we were still watching key1
      got = conn.multi do |txn|
        redis2.set('{slot}key1', 'hello')
        txn.set('{slot}key2', 'valuez')
      end
    end

    assert_equal(['OK'], got)
    assert_equal('valuez', redis.get('{slot}key2'))
  end

  def test_pinning_multi_in_pipeline
    multi_future = nil
    foo_future = nil
    bar_future = nil
    response = nil
    redis.with(key: '{slot}') do |conn|
      response = conn.pipelined do |pipe|
        multi_future = pipe.multi do |txn|
          txn.set('{slot}foo', 's1')
          foo_future = txn.get('{slot}foo')
        end
        pipe.multi do |txn|
          txn.set('{slot}bar', 's2')
          bar_future = txn.get('{slot}bar')
        end
      end
    end

    assert_equal(['OK', 's1'], multi_future.value)
    assert_equal('s1', foo_future.value)
    assert_equal('s2', bar_future.value)
    assert_equal(['OK', 'QUEUED', 'QUEUED', ['OK', 's1'], 'OK', 'QUEUED', 'QUEUED', ['OK', 's2']], response)
  end

  def test_pinning_manual_unwatch
    redis2 = build_another_client
    got = nil
    redis.with(key: '{slot}') do |conn|
      conn.watch('{slot}key1')
      conn.set('{slot}key1', 'value1')
      redis2.set('{slot}key1', 'value_from_redis2')
      conn.unwatch

      # This would not commit if unwatch was inneffective.
      got = conn.multi do |txn|
        txn.set('{slot}key2', 'value2')
      end
    end

    assert_equal(['OK'], got)
    assert_equal('value2', redis.get('{slot}key2'))
  end
end
