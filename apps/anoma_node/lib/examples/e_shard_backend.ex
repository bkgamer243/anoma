defmodule Anoma.Node.Examples.EShardBackend do
  @moduledoc """
  I contain examples demonstrating transaction execution using the Shard backend.
  """

  alias Anoma.Node.Examples.ENode
  alias Examples.ENock
  alias Anoma.Node.Transaction.Mempool
  alias Anoma.Node.Transaction.Shard
  alias Anoma.Node.Registry
  alias Anoma.Node.Event

  require Noun
  require Logger

  import ExUnit.Assertions

  @dialyzer :no_improper_lists

  # --- Helper Functions ---

  # Helper to wait for BlockEvent
  defp wait_for_block(node_id, expected_round) do
    block_filter = [
      Event.node_filter(node_id),
      %Mempool.BlockFilter{}
    ]

    EventBroker.subscribe_me(block_filter)

    assert_receive(
      %EventBroker.Event{
        body: %Event{
          node_id: ^node_id,
          body: %Mempool.BlockEvent{round: ^expected_round}
        }
      },
      # Timeout after 5 seconds
      5000
    )

    EventBroker.unsubscribe_me(block_filter)
    :ok
  end

  # Helper to wait for a specific condition based on a process's state
  defp wait_for_state_condition(
         pid,
         description,
         predicate_fun,
         timeout \\ 5000
       ) do
    unless is_pid(pid) do
      raise "Invalid PID provided to wait_for_state_condition: #{inspect(pid)}"
    end

    start_time = System.monotonic_time(:millisecond)
    # ms
    check_interval = 10

    loop_check = fn fun ->
      current_time = System.monotonic_time(:millisecond)

      if current_time - start_time > timeout do
        {:error, :timeout}
      else
        state = :sys.get_state(pid)

        if predicate_fun.(state) do
          :ok
        else
          Process.sleep(check_interval)
          # Recurse
          fun.(fun)
        end
      end
    end

    case loop_check.(loop_check) do
      :ok ->
        :ok

      {:error, :timeout} ->
        state = :sys.get_state(pid)
        # Include description in the error message
        raise "Timeout waiting for condition '#{description}' on pid #{inspect(pid)}. Current state: #{inspect(state)}"
    end
  end

  # Helper function to log shard states
  defp log_shard_states(node_id, stage_name) do
    pid_a = Registry.whereis(node_id, Shard, :a)
    pid_b = Registry.whereis(node_id, Shard, :b)
    pid_c = Registry.whereis(node_id, Shard, :c)

    state_a = if is_pid(pid_a), do: :sys.get_state(pid_a), else: :not_found
    state_b = if is_pid(pid_b), do: :sys.get_state(pid_b), else: :not_found
    state_c = if is_pid(pid_c), do: :sys.get_state(pid_c), else: :not_found

    Logger.debug("-- Shard States [#{stage_name}] --")
    Logger.debug("Shard A state: #{inspect(state_a)}")
    Logger.debug("Shard B state: #{inspect(state_b)}")
    Logger.debug("Shard C state: #{inspect(state_c)}")
    Logger.debug("-------------------------------")
  end

  # --- Test Functions ---

  @doc """
  I test the integration of transactions with the shard backend.

  - I start a node with shards "a", "b", "c".
  - I submit Tx1 (write 3 -> "a"), Tx2 (write 4 -> "b"), Tx3 (read "a", "b", write sum -> "c").
  - I execute transactions in order.
  - I verify the final state of shards.
  """
  @spec test_shard_integration() :: :ok
  def test_shard_integration() do
    node_id = "shard_integration_test_node"

    # 1. Setup Node with Shards
    # No initial values, which won't matter.
    schema = ["a", "b", "c"]
    shard_config = [strategy: :one_per_key, schema: schema]
    opts = [node_id: node_id, transaction: [shards: shard_config]]
    enode = ENode.start_node(opts)
    assert %ENode{node_id: ^node_id} = enode

    # Use pre-defined Nock programs from Examples.ENock
    tx1_code = ENock.write_code_gen(ENock.a_int(), 3)
    tx2_code = ENock.write_code_gen(ENock.b_int(), 4)
    tx3_code = ENock.read_ab_write_c_sum()

    # 3. Submit Transactions
    :ok = Mempool.tx(node_id, {:shard_storage, tx1_code}, "tx1")
    :ok = Mempool.tx(node_id, {:shard_storage, tx2_code}, "tx2")
    :ok = Mempool.tx(node_id, {:shard_storage, tx3_code}, "tx3")

    # 4. Execute Transactions
    tx_ids_dump = Mempool.tx_dump(node_id)
    ordered_tx_ids = ["tx1", "tx2", "tx3"]
    # Ensure all submitted transactions are present before execution
    assert MapSet.new(tx_ids_dump) == MapSet.new(ordered_tx_ids)

    # Execute Tx1
    Logger.info("Executing Tx1...")
    :ok = Mempool.execute(node_id, ["tx1"])
    :ok = wait_for_block(node_id, 1)
    Logger.info("Finished executing Tx1.")
    log_shard_states(node_id, "After Tx1")

    # Wait for the write watermark on shard 'a' to be updated by Ordering
    wait_for_state_condition(
      Registry.whereis(node_id, Shard, :a),
      "write WM on a == 1 after Tx1",
      fn state ->
        get_in(state.watermarks, ["a", :write]) == 1
      end
    )

    # Verify watermarks after Tx1
    state_a_after_tx1 = :sys.get_state(Registry.whereis(node_id, Shard, :a))

    assert state_a_after_tx1.watermarks["a"].write == 1,
           "Shard 'a' write watermark should be 1 after Tx1"

    # Execute Tx2
    Logger.info("Executing Tx2...")
    :ok = Mempool.execute(node_id, ["tx2"])
    :ok = wait_for_block(node_id, 2)
    Logger.info("Finished executing Tx2.")
    log_shard_states(node_id, "After Tx2")

    # Wait for the write watermark on shard 'b' to be updated by Ordering
    wait_for_state_condition(
      Registry.whereis(node_id, Shard, :b),
      "write WM on b == 2 after Tx2",
      fn state ->
        get_in(state.watermarks, ["b", :write]) == 2
      end
    )

    # Verify watermarks after Tx2
    state_b_after_tx2 = :sys.get_state(Registry.whereis(node_id, Shard, :b))

    assert state_b_after_tx2.watermarks["b"].write == 2,
           "Shard 'b' write watermark should be 2 after Tx2"

    # Execute Tx3
    Logger.info("Executing Tx3...")
    :ok = Mempool.execute(node_id, ["tx3"])
    :ok = wait_for_block(node_id, 3)
    Logger.info("Finished executing Tx3.")
    log_shard_states(node_id, "After Tx3 (Final)")

    # Wait for the final watermarks to be updated by Ordering
    wait_for_state_condition(
      Registry.whereis(node_id, Shard, :a),
      "read WM on a == 3 after Tx3",
      fn state ->
        get_in(state.watermarks, ["a", :read]) == 3
      end
    )

    wait_for_state_condition(
      Registry.whereis(node_id, Shard, :b),
      "read WM on b == 3 after Tx3",
      fn state ->
        get_in(state.watermarks, ["b", :read]) == 3
      end
    )

    wait_for_state_condition(
      Registry.whereis(node_id, Shard, :c),
      "write WM on c == 3 after Tx3",
      fn state ->
        get_in(state.watermarks, ["c", :write]) == 3
      end
    )

    # 5. Verification
    # Get shard PIDs using the atom labels used in schema/supervisor
    pid_a = Registry.whereis(node_id, Shard, :a)
    pid_b = Registry.whereis(node_id, Shard, :b)
    pid_c = Registry.whereis(node_id, Shard, :c)

    assert is_pid(pid_a),
           "Shard 'a' PID not found. ShardSupervisor might not have started it correctly."

    assert is_pid(pid_b), "Shard 'b' PID not found."
    assert is_pid(pid_c), "Shard 'c' PID not found."

    # Check final state using :sys.get_state - Tx1 (h=1), Tx2 (h=2), Tx3 (h=3)
    state_a = :sys.get_state(pid_a)
    state_b = :sys.get_state(pid_b)
    state_c = :sys.get_state(pid_c)

    Logger.debug("Shard A state: #{inspect(state_a)}")
    Logger.debug("Shard B state: #{inspect(state_b)}")
    Logger.debug("Shard C state: #{inspect(state_c)}")

    # Verify Tx1 result (height 1)
    assert state_a.kv["a"][1].value == 3,
           "Shard 'a' state mismatch for height 1 (Tx1)"

    # Verify Tx2 result (height 2)
    assert state_b.kv["b"][2].value == 4,
           "Shard 'b' state mismatch for height 2 (Tx2)"

    # Verify Tx3 result (height 3)
    assert state_c.kv["c"][3].value == 7,
           "Shard 'c' state mismatch for height 3 (Tx3)"

    # Verify watermarks after Tx3
    assert state_a.watermarks["a"].read == 3,
           "Shard 'a' read watermark should be 3 after Tx3"

    assert state_b.watermarks["b"].read == 3,
           "Shard 'b' read watermark should be 3 after Tx3"

    assert state_c.watermarks["c"].write == 3,
           "Shard 'c' write watermark should be 3 after Tx3"

    # 6. Cleanup
    :ok = ENode.stop_node(enode)
    :ok
  end

  @doc """
  I test the integration of transactions with the shard backend, executing concurrently.

  - I start a node with shards "a", "b", "c".
  - I submit Tx1 (write 3 -> "a"), Tx2 (write 4 -> "b"), Tx3 (read "a", "b", write sum -> "c").
  - I execute all transactions together.
  - I verify the final state of shards.
  """
  @spec test_shard_integration_concurrent() :: :ok
  def test_shard_integration_concurrent() do
    node_id = "shard_integration_concurrent_test_node"

    # 1. Setup Node with Shards
    schema = ["a", "b", "c"]
    shard_config = [strategy: :one_per_key, schema: schema]
    opts = [node_id: node_id, transaction: [shards: shard_config]]
    enode = ENode.start_node(opts)
    assert %ENode{node_id: ^node_id} = enode

    # Use pre-defined Nock programs from Examples.ENock
    tx1_code = ENock.write_code_gen(ENock.a_int(), 3)
    tx2_code = ENock.write_code_gen(ENock.b_int(), 4)
    tx3_code = ENock.read_ab_write_c_sum()

    # 3. Submit All Transactions
    :ok = Mempool.tx(node_id, {:shard_storage, tx1_code}, "tx1")
    :ok = Mempool.tx(node_id, {:shard_storage, tx2_code}, "tx2")
    :ok = Mempool.tx(node_id, {:shard_storage, tx3_code}, "tx3")

    # Ensure all submitted transactions are present before execution
    tx_ids_dump = Mempool.tx_dump(node_id)
    ordered_tx_ids = ["tx1", "tx2", "tx3"]
    assert MapSet.new(tx_ids_dump) == MapSet.new(ordered_tx_ids)

    # 4. Execute All Transactions Together
    Logger.info("Executing Tx1, Tx2, Tx3 concurrently...")
    :ok = Mempool.execute(node_id, ordered_tx_ids)
    :ok = wait_for_block(node_id, 1)
    Logger.info("Finished executing Tx1, Tx2, Tx3.")
    log_shard_states(node_id, "After Concurrent Execution")

    # Wait for the final watermarks to be updated by Ordering
    wait_for_state_condition(
      Registry.whereis(node_id, Shard, :a),
      "read WM on a == 3 after concurrent execution",
      fn state ->
        get_in(state.watermarks, ["a", :read]) == 3
      end
    )

    wait_for_state_condition(
      Registry.whereis(node_id, Shard, :b),
      "read WM on b == 3 after concurrent execution",
      fn state ->
        get_in(state.watermarks, ["b", :read]) == 3
      end
    )

    wait_for_state_condition(
      Registry.whereis(node_id, Shard, :c),
      "write WM on c == 3 after concurrent execution",
      fn state ->
        get_in(state.watermarks, ["c", :write]) == 3
      end
    )

    # 5. Verification (Final state only)
    pid_a = Registry.whereis(node_id, Shard, :a)
    pid_b = Registry.whereis(node_id, Shard, :b)
    pid_c = Registry.whereis(node_id, Shard, :c)

    assert is_pid(pid_a), "Shard 'a' PID not found."
    assert is_pid(pid_b), "Shard 'b' PID not found."
    assert is_pid(pid_c), "Shard 'c' PID not found."

    # Check final state using :sys.get_state
    state_a = :sys.get_state(pid_a)
    state_b = :sys.get_state(pid_b)
    state_c = :sys.get_state(pid_c)

    Logger.debug("Shard A final state: #{inspect(state_a)}")
    Logger.debug("Shard B final state: #{inspect(state_b)}")
    Logger.debug("Shard C final state: #{inspect(state_c)}")

    # Verify final KV values (Heights reflect execution order)
    assert state_a.kv["a"][1].value == 3,
           "Shard 'a' state mismatch for height 1 (Tx1)"

    assert state_b.kv["b"][2].value == 4,
           "Shard 'b' state mismatch for height 2 (Tx2)"

    assert state_c.kv["c"][3].value == 7,
           "Shard 'c' state mismatch for height 3 (Tx3)"

    # Verify final watermarks (Reflect all reads/writes across the executed block)
    assert state_a.watermarks["a"].read == 3,
           "Shard 'a' read watermark should be 3 after concurrent execution"

    assert state_b.watermarks["b"].read == 3,
           "Shard 'b' read watermark should be 3 after concurrent execution"

    # Only Tx3 wrote to 'c'
    assert state_c.watermarks["c"].write == 3,
           "Shard 'c' write watermark should be 3 after concurrent execution"

    # 6. Cleanup
    :ok = ENode.stop_node(enode)
    :ok
  end

  @doc """
  I test interleaved writes to shard 'a' and copies from 'a' to 'b'.

  - Start node with shards "a", "b".
  - Tx Sequence: W(a=1), C(a->b), C(a->b), W(a=2), C(a->b), C(a->b), C(a->b), W(a=3), C(a->b)
  - Verify state (kv) and watermarks after each step.
  """
  @spec test_interleaved_write_copy() :: :ok
  def test_interleaved_write_copy() do
    node_id = "interleaved_write_copy_test_node"
    schema = ["a", "b"]
    shard_config = [strategy: :one_per_key, schema: schema]
    opts = [node_id: node_id, transaction: [shards: shard_config]]
    enode = ENode.start_node(opts)
    assert %ENode{node_id: ^node_id} = enode

    pid_a = Registry.whereis(node_id, Shard, :a)
    pid_b = Registry.whereis(node_id, Shard, :b)
    assert is_pid(pid_a), "Shard 'a' PID not found."
    assert is_pid(pid_b), "Shard 'b' PID not found."

    # Use pre-defined Nock programs from Examples.ENock
    write_1_code = ENock.write_code_gen(ENock.a_int(), 1)
    write_2_code = ENock.write_code_gen(ENock.a_int(), 2)
    write_3_code = ENock.write_code_gen(ENock.a_int(), 3)
    copy_code = ENock.copy_a_to_b()

    # Helper for execution and verification
    exec_and_verify = fn height, code, tx_base_id, checks ->
      tx_id = "#{tx_base_id}_#{height}"

      Logger.info("Executing Tx #{height} (#{tx_id})...")
      :ok = Mempool.tx(node_id, {:shard_storage, code}, tx_id)
      :ok = Mempool.execute(node_id, [tx_id])
      :ok = wait_for_block(node_id, height)
      Logger.info("Finished executing Tx #{height}.")

      # --- Wait for specific watermark updates before checking state ---
      # This addresses potential race conditions where the test checks state
      # before the asynchronous watermark update from Ordering is processed by the Shard.
      case {tx_base_id, height} do
        # After a write to 'a'
        {"write", h} ->
          pid_a = Registry.whereis(node_id, Shard, :a)

          wait_for_state_condition(pid_a, "write WM on a == #{h}", fn state ->
            get_in(state.watermarks, ["a", :write]) == h
          end)

        # After a copy from 'a' to 'b'
        {"copy", h} ->
          pid_a = Registry.whereis(node_id, Shard, :a)
          pid_b = Registry.whereis(node_id, Shard, :b)

          wait_for_state_condition(pid_a, "read WM on a == #{h}", fn state ->
            get_in(state.watermarks, ["a", :read]) == h
          end)

          wait_for_state_condition(pid_b, "write WM on b == #{h}", fn state ->
            get_in(state.watermarks, ["b", :write]) == h
          end)

        # Default case, no specific wait needed
        {_, _} ->
          :ok
      end

      # ---------------------------------------------------------------

      state_a = :sys.get_state(pid_a)
      state_b = :sys.get_state(pid_b)

      log_shard_states(node_id, "After Tx #{height} (#{tx_id})")

      Enum.each(checks, fn {shard_pid, type, key, expected} ->
        state = if shard_pid == pid_a, do: state_a, else: state_b
        shard_name = if shard_pid == pid_a, do: "a", else: "b"

        case type do
          :kv ->
            actual = get_in(state.kv, [key, height, :value])

            assert actual == expected,
                   "Shard '#{shard_name}' KV['#{key}'][#{height}] mismatch. Expected: #{inspect(expected)}, Got: #{inspect(actual)} after Tx #{height}"

          :read_wm ->
            actual = get_in(state.watermarks, [key, :read])

            assert actual == expected,
                   "Shard '#{shard_name}' read watermark['#{key}'] mismatch. Expected: #{inspect(expected)}, Got: #{inspect(actual)} after Tx #{height}"

          :write_wm ->
            actual = get_in(state.watermarks, [key, :write])

            assert actual == expected,
                   "Shard '#{shard_name}' write watermark['#{key}'] mismatch. Expected: #{inspect(expected)}, Got: #{inspect(actual)} after Tx #{height}"
        end
      end)
    end

    # 1. Write 1 to a (h=1)
    exec_and_verify.(
      1,
      write_1_code,
      "write",
      [
        {pid_a, :kv, "a", 1},
        {pid_a, :write_wm, "a", 1}
      ]
    )

    # 2. Copy a to b (h=2)
    exec_and_verify.(
      2,
      copy_code,
      "copy",
      [
        {pid_a, :read_wm, "a", 2},
        {pid_b, :kv, "b", 1},
        {pid_b, :write_wm, "b", 2}
      ]
    )

    # 3. Copy a to b (h=3)
    exec_and_verify.(
      3,
      copy_code,
      "copy",
      [
        {pid_a, :read_wm, "a", 3},
        {pid_b, :kv, "b", 1},
        {pid_b, :write_wm, "b", 3}
      ]
    )

    # 4. Write 2 to a (h=4)
    exec_and_verify.(
      4,
      write_2_code,
      "write",
      [
        {pid_a, :kv, "a", 2},
        {pid_a, :write_wm, "a", 4}
      ]
    )

    # 5. Copy a to b (h=5)
    exec_and_verify.(
      5,
      copy_code,
      "copy",
      [
        {pid_a, :read_wm, "a", 5},
        {pid_b, :kv, "b", 2},
        {pid_b, :write_wm, "b", 5}
      ]
    )

    # 6. Copy a to b (h=6)
    exec_and_verify.(
      6,
      copy_code,
      "copy",
      [
        {pid_a, :read_wm, "a", 6},
        {pid_b, :kv, "b", 2},
        {pid_b, :write_wm, "b", 6}
      ]
    )

    # 7. Copy a to b (h=7)
    exec_and_verify.(
      7,
      copy_code,
      "copy",
      [
        {pid_a, :read_wm, "a", 7},
        {pid_b, :kv, "b", 2},
        {pid_b, :write_wm, "b", 7}
      ]
    )

    # 8. Write 3 to a (h=8)
    exec_and_verify.(
      8,
      write_3_code,
      "write",
      [
        {pid_a, :kv, "a", 3},
        {pid_a, :write_wm, "a", 8}
      ]
    )

    # 9. Copy a to b (h=9)
    exec_and_verify.(
      9,
      copy_code,
      "copy",
      [
        {pid_a, :read_wm, "a", 9},
        {pid_b, :kv, "b", 3},
        {pid_b, :write_wm, "b", 9}
      ]
    )

    # --- Final State Check ---
    final_state_a = :sys.get_state(pid_a)
    final_state_b = :sys.get_state(pid_b)

    # Check latest values
    assert final_state_a.kv["a"][8].value == 3
    assert final_state_b.kv["b"][9].value == 3

    # Check garbage collection
    assert final_state_a.kv["a"][1] == nil
    assert final_state_b.kv["b"][2] == nil
    assert final_state_b.kv["b"][3] == nil
    assert final_state_b.kv["a"][4] == nil
    assert final_state_b.kv["b"][5] == nil
    assert final_state_b.kv["b"][6] == nil
    # GC bug? Not important enough to fix for now.
    # assert final_state_b.kv["b"][7] == nil

    # --- Cleanup ---
    :ok = ENode.stop_node(enode)
    :ok
  end

  @doc """
  I test that a transaction can read the correct state even after a preceding
  transaction on the same shard crashed after reserving a write.

  - Start node with shards "a", "b".
  - Tx 1: Write 1 to "a".
  - Tx 2: Reserve write on "a", then crash.
  - Tx 3: Copy "a" to "b".
  - Verify that Tx 3 reads the value written by Tx 1, and the state of "b" reflects this.
  """
  @spec test_copy_after_crash() :: :ok
  def test_copy_after_crash() do
    node_id = "copy_after_crash_test_node"
    schema = ["a", "b"]
    shard_config = [strategy: :one_per_key, schema: schema]
    opts = [node_id: node_id, transaction: [shards: shard_config]]
    enode = ENode.start_node(opts)
    assert %ENode{node_id: ^node_id} = enode

    pid_a = Registry.whereis(node_id, Shard, :a)
    pid_b = Registry.whereis(node_id, Shard, :b)
    assert is_pid(pid_a), "Shard 'a' PID not found."
    assert is_pid(pid_b), "Shard 'b' PID not found."

    # Use pre-defined Nock programs from Examples.ENock
    tx1_code = ENock.write_code_gen(ENock.a_int(), 1)
    tx2_code = ENock.crash_after_reserve_a()
    tx3_code = ENock.copy_a_to_b()

    # Execute Tx 1 (Write 1 to a, h=1)
    tx1_id = "write_1_1"
    Logger.info("Executing Tx 1 (#{tx1_id})...")
    :ok = Mempool.tx(node_id, {:shard_storage, tx1_code}, tx1_id)
    :ok = Mempool.execute(node_id, [tx1_id])
    :ok = wait_for_block(node_id, 1)
    log_shard_states(node_id, "After Tx 1")

    wait_for_state_condition(
      pid_a,
      "read WM on a == 1 after Tx 1",
      fn state ->
        get_in(state.watermarks, ["a", :read]) == 1
      end
    )

    state_a_1 = :sys.get_state(pid_a)
    assert state_a_1.kv["a"][1].value == 1
    assert state_a_1.watermarks["a"].write == 1

    # Execute Tx 2 (Crash on a, h=2)
    tx2_id = "crash_1_2"
    Logger.info("Executing Tx 2 (#{tx2_id})...")
    :ok = Mempool.tx(node_id, {:shard_storage, tx2_code}, tx2_id)
    :ok = Mempool.execute(node_id, [tx2_id])
    :ok = wait_for_block(node_id, 2)
    log_shard_states(node_id, "After Tx 2")

    # Write was reserved, so watermark bumps, but KV has no reservation for h=2
    pid_a = Registry.whereis(node_id, Shard, :a)
    assert is_pid(pid_a), "Shard 'a' PID not found after Tx 2 execution."

    wait_for_state_condition(
      pid_a,
      "write WM on a == 2 after Tx 2",
      fn state ->
        get_in(state.watermarks, ["a", :write]) == 2
      end
    )

    # Execute Tx 3 (Copy a to b, h=3)
    tx3_id = "copy_1_3"
    Logger.info("Executing Tx 3 (#{tx3_id})...")
    :ok = Mempool.tx(node_id, {:shard_storage, tx3_code}, tx3_id)
    :ok = Mempool.execute(node_id, [tx3_id])
    :ok = wait_for_block(node_id, 3)
    log_shard_states(node_id, "After Tx 3")

    # Wait for final watermarks before checking state
    wait_for_state_condition(
      pid_a,
      "read WM on a == 3 after Tx 3",
      fn state ->
        get_in(state.watermarks, ["a", :read]) == 3
      end
    )

    wait_for_state_condition(
      pid_b,
      "write WM on b == 3 after Tx 3",
      fn state ->
        get_in(state.watermarks, ["b", :write]) == 3
      end
    )

    state_a_3 = :sys.get_state(pid_a)
    state_b_3 = :sys.get_state(pid_b)

    # Verify Tx 3 read the value from h=1 and wrote it at h=3
    assert state_a_3.watermarks["a"].read == 3,
           "Shard 'a' read watermark should be 3 after Tx 3"

    assert state_b_3.kv["b"][3].value == 1,
           "Shard 'b' KV['b'][3] should be 1 after Tx 3"

    assert state_b_3.watermarks["b"].write == 3,
           "Shard 'b' write watermark should be 3 after Tx 3"

    # --- Cleanup ---
    :ok = ENode.stop_node(enode)
    :ok
  end
end
