defmodule Anoma.Node.Examples.EShardSupervisor do
  @moduledoc """
  I contain examples demonstrating the ShardSupervisor functionality.
  """

  alias Anoma.Node.Examples.ENode
  alias Anoma.Node.Registry
  alias Anoma.Node.Transaction.Shard
  alias Anoma.Node.Transaction.ShardRouter

  import ExUnit.Assertions

  @doc """
  I test starting a node with a shard configuration, verifying that the
  ShardSupervisor starts the correct Shard processes and ShardRouter,
  and that the router correctly maps keys to shard names.
  """
  @spec test_shard_supervisor_startup_and_routing() :: :ok
  def test_shard_supervisor_startup_and_routing() do
    node_id = "shard_sup_test_node"

    # 1. Define Schema and Start Node
    schema = [{"a", 5}, "b", {"c", 7}]
    shard_config = [strategy: :one_per_key, schema: schema]
    opts = [node_id: node_id, transaction: [shards: shard_config]]

    enode = ENode.start_node(opts)
    assert %ENode{node_id: ^node_id} = enode

    # 2. Verify ShardRouter Exists
    pid_router = Registry.whereis(node_id, ShardRouter)

    assert is_pid(pid_router),
           "ShardRouter for node #{node_id} should be registered and alive."

    pid_shard_a = Registry.whereis(node_id, Shard, :a)
    pid_shard_b = Registry.whereis(node_id, Shard, :b)
    pid_shard_c = Registry.whereis(node_id, Shard, :c)

    assert is_pid(pid_shard_a), "Shard 'a' should be registered and alive."
    assert is_pid(pid_shard_b), "Shard 'b' should be registered and alive."
    assert is_pid(pid_shard_c), "Shard 'c' should be registered and alive."

    # 4. Verify Initial State within Shards (don't use stateful Shard.read)
    state_a = :sys.get_state(pid_shard_a)
    state_b = :sys.get_state(pid_shard_b)
    state_c = :sys.get_state(pid_shard_c)

    # Check initial value at height -1
    assert state_a.kv["a"][-1].value == 5, "Shard 'a' initial value mismatch"
    assert state_b.kv == %{}, "Shard 'b' should have an empty initial kv map"
    assert state_c.kv["c"][-1].value == 7, "Shard 'c' initial value mismatch"

    # 5. Query ShardRouter using the specific router's via tuple
    assert ShardRouter.get_shard_label(node_id, "a") ==
             {:ok, :a},
           "Router lookup for 'a' failed"

    assert ShardRouter.get_shard_label(node_id, "b") ==
             {:ok, :b},
           "Router lookup for 'b' failed"

    assert ShardRouter.get_shard_label(node_id, "c") ==
             {:ok, :c},
           "Router lookup for 'c' failed"

    assert ShardRouter.get_shard_label(node_id, "d") == :error,
           "Router lookup for unknown key 'd' should return :error"

    # 6. Cleanup
    :ok = ENode.stop_node(enode)
    :ok
  end
end
