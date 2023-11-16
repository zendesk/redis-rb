# frozen_string_literal: true

require "helper"

# ruby -w -Itest test/cluster_client_transactions_test.rb
class TestClusterClientTransactions < Minitest::Test
  include Helper::Cluster

  def test_cluster_client_does_support_transaction_by_single_key
    actual = redis.multi do |r|
      r.set('counter', '0')
      r.incr('counter')
      r.incr('counter')
    end

    assert_equal(['OK', 1, 2], actual)
    assert_equal('2', redis.get('counter'))
  end

  def test_cluster_client_does_support_transaction_by_hashtag
    actual = redis.multi do |r|
      r.mset('{key}1', 1, '{key}2', 2)
      r.mset('{key}3', 3, '{key}4', 4)
    end

    assert_equal(%w[OK OK], actual)
    assert_equal(%w[1 2 3 4], redis.mget('{key}1', '{key}2', '{key}3', '{key}4'))
  end

  def test_cluster_client_does_not_support_transaction_by_multiple_keys
    assert_raises(Redis::Cluster::TransactionConsistencyError) do
      redis.multi do |r|
        r.set('key1', 1)
        r.set('key2', 2)
        r.set('key3', 3)
        r.set('key4', 4)
      end
    end

    assert_raises(Redis::Cluster::TransactionConsistencyError) do
      redis.multi do |r|
        r.mset('key1', 1, 'key2', 2)
        r.mset('key3', 3, 'key4', 4)
      end
    end

    (1..4).each do |i|
      assert_nil(redis.get("key#{i}"))
    end
  end

  def test_cluster_client_can_watch_and_multi
    redis.mset('{slot}key1', 'value1', '{slot}key2', 'value2')
    actual = redis.watch(['{slot}key1', '{slot}key2']) do
      old_value1, old_value2 = redis.mget('{slot}key1', '{slot}key2')
      redis.multi do |r|
        r.set('{slot}key1', old_value2)
        r.set('{slot}key2', old_value1)
      end
    end

    assert_equal(%w[OK OK], actual)
    assert_equal(%w[value2 value1], redis.mget('{slot}key1', '{slot}key2'))
  end

  def test_cluster_client_unwatches_on_error_in_watch
    the_exception = Class.new(StandardError)
    assert_raises(the_exception) do
      redis.watch(['{slot}key1', '{slot}key2']) do
        raise the_exception
      end
    end

    # The client is now usable for other slots.
    assert_equal('OK', redis.set('{otherslot}key3', 'othervalue'))
  end

  def test_cluster_client_unwatches_on_error_in_multi
    the_exception = Class.new(StandardError)
    $commands.clear
    redis.set('{slot}key1', 'something')
    assert_raises(the_exception) do
      redis.watch(['{slot}key1', '{slot}key2']) do
        redis.multi do |txn|
          txn.set('{slot}key1', 'something_else')
          raise the_exception
        end
      end
    end

    assert_equal('something', redis.get('{slot}key1'))
    # The client is now usable for other slots.
    assert_equal('OK', redis.set('{otherslot}key3', 'othervalue'))
  end

  def test_cluster_client_can_unwatch
    redis.watch(['{slot}key1', '{slot}key2']) do
      redis.unwatch
    end
    # The client is now usable for other slots.
    assert_equal('OK', redis.set('{otherslot}key3', 'othervalue'))
  end

  def test_cluster_client_is_pinned_in_watch_block
    assert_raises(Redis::Cluster::TransactionConsistencyError) do
      redis.watch(['{slot}key1']) do
        redis.get('{otherslot}key3')
      end
    end
  end

  def test_cluster_client_watch_modified_in_thread
    redis.mset('{slot}key1', 'value1', '{slot}key2', 'value2')
    actual = redis.watch(['{slot}key1', '{slot}key2']) do
      old_value1, old_value2 = redis.mget('{slot}key1', '{slot}key2')

      Thread.new { build_another_client.set('{slot}key1', 'from_another_thread') }.join

      redis.multi do |r|
        r.set('{slot}key1', old_value2)
        r.set('{slot}key2', old_value1)
      end
    end

    assert_nil(actual)
    assert_equal(%w[from_another_thread value2], redis.mget('{slot}key1', '{slot}key2'))
  end
end
