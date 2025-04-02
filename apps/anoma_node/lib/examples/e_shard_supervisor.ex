defmodule Anoma.Node.Examples.EShardSupervisor do
  @moduledoc """
  I contain examples demonstrating the ShardSupervisor functionality.
  """

  alias Anoma.Node.Examples.ENode
  alias Anoma.Node.Registry
  alias Anoma.Node.Transaction.Shard
  alias Anoma.Node.Transaction.ShardRouter

  # ShardSupervisor isn't called directly, but good to alias if referencing types
  # alias Anoma.Node.Transaction.ShardSupervisor

  import ExUnit.Assertions

  # Use the same registry name constant defined in supervisors

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

    # Allow supervisors a moment to start children
    Process.sleep(100)

    # 2. Verify ShardRouter Exists
    via_router = Registry.via(node_id, ShardRouter)
    pid_router = Registry.whereis(node_id, ShardRouter)

    assert is_pid(pid_router),
           "ShardRouter for node #{node_id} should be registered and alive."

    # 3. Verify Shard Processes Exist using (key, Module) lookup
    via_shard_a = Registry.via(node_id, Shard, :a)
    via_shard_b = Registry.via(node_id, Shard, :b)
    via_shard_c = Registry.via(node_id, Shard, :c)

    pid_shard_a = Registry.whereis(node_id, Shard, :a)
    pid_shard_b = Registry.whereis(node_id, Shard, :b)
    pid_shard_c = Registry.whereis(node_id, Shard, :c)

    assert is_pid(pid_shard_a), "Shard 'a' should be registered and alive."
    assert is_pid(pid_shard_b), "Shard 'b' should be registered and alive."
    assert is_pid(pid_shard_c), "Shard 'c' should be registered and alive."

    # 4. Verify Initial State within Shards (using :sys.get_state for test)
    state_a = :sys.get_state(pid_shard_a)
    state_b = :sys.get_state(pid_shard_b)
    state_c = :sys.get_state(pid_shard_c)

    # Check initial value at height -1
    assert state_a.kv["a"][-1].value == 5, "Shard 'a' initial value mismatch"
    assert state_b.kv == %{}, "Shard 'b' should have an empty initial kv map"
    assert state_c.kv["c"][-1].value == 7, "Shard 'c' initial value mismatch"

    # 5. Query ShardRouter using the specific router's via tuple
    assert GenServer.call(via_router, {:get_shard_name, "a"}) ==
             {:ok, via_shard_a},
           "Router lookup for 'a' failed"

    assert GenServer.call(via_router, {:get_shard_name, "b"}) ==
             {:ok, via_shard_b},
           "Router lookup for 'b' failed"

    assert GenServer.call(via_router, {:get_shard_name, "c"}) ==
             {:ok, via_shard_c},
           "Router lookup for 'c' failed"

    assert GenServer.call(via_router, {:get_shard_name, "d"}) == :error,
           "Router lookup for unknown key 'd' should return :error"

    # 6. Cleanup
    :ok = ENode.stop_node(enode)
    :ok
  end
end
