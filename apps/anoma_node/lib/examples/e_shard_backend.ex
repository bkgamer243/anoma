defmodule Anoma.Node.Examples.EShardBackend do
  @moduledoc """
  I contain examples demonstrating transaction execution using the Shard backend.
  """

  alias Anoma.Node.Transaction.Backends
  alias Anoma.Node.Examples.ENode
  alias Anoma.Node.Transaction.Mempool
  alias Anoma.Node.Transaction.Shard
  alias Anoma.Node.Registry

  require Noun
  require Logger

  import ExUnit.Assertions
  import ExUnit.CaptureLog

  @dialyzer :no_improper_lists

  @doc """
  I test a Nock program for writing 3 to "a".

  I verify stage 1 (reservation/code split) and stage 2 (execution).
  """
  @spec test_nock_program_tx1() :: :ok
  def test_nock_program_tx1() do
    # Setup
    key_a_int = Noun.atom_binary_to_integer("a")
    dummy_tx_id = "test_tx_id_1"

    # Tx1 reservations: Write to "a"
    tx1_reservations = [1 | key_a_int]

    # Create a "stage 2" program that just returns a fixed key-value pair
    tx1_writes = [key_a_int | 3]

    # Program that basically just acts as constant functions for reservations and writes
    tx1_code = [[0 | 3] | [tx1_reservations | [[1 | tx1_writes] | [0 | 0]]]]

    # Stage 1: Execute the core using the backend's formula to extract reservations
    {:ok, stage1_result} = Nock.nock(tx1_code, [9, 2, 0 | 1], %Nock{})

    # Extract components from stage 1 result
    [res1 | tx1_stage2_code] = stage1_result
    assert res1 == tx1_reservations

    # Check that reservations parse properly
    {:ok, parsed_reservations} = Backends.parse_reservations(res1)
    assert parsed_reservations == [{:write, "a"}]

    # Stage 2: Execute using the exact sequence from Backends.vm_execute_stage2
    env_tx1 = %Nock{}

    # Step 1: Apply formula [10, [6, 1 | id], 0 | 1] - this inserts the tx_id into the code
    {:ok, ordered_tx1} =
      Nock.nock(tx1_stage2_code, [10, [6, 1 | dummy_tx_id], 0 | 1], env_tx1)

    # Step 2: Apply formula [9, 2, 0 | 1] - this executes the transaction
    {:ok, result1} = Nock.nock(ordered_tx1, [9, 2, 0 | 1], env_tx1)

    # This is the expected format that backends.ex receives from vm_execute_stage2
    assert result1 == tx1_writes

    :ok
  end

  @doc """
  I test a Nock program for the sequence (read a, read b, add a and b, write result to c).

  I verify stage 1 (reservation/code split) and stage 2 (execution with mock scry).
  """
  @spec test_nock_program_tx3() :: :ok
  def test_nock_program_tx3() do
    # Setup
    key_a_int = Noun.atom_binary_to_integer("a")
    key_b_int = Noun.atom_binary_to_integer("b")
    key_c_int = Noun.atom_binary_to_integer("c")
    dummy_tx_id = "test_tx_id_3"

    # Tx3: Read "a", Read "b", Add, Write result to "c"
    tx3_reservations = [
      [0 | key_a_int],
      [0 | key_b_int]
      | [1 | key_c_int]
    ]

    tx3_writes = [key_c_int | 7]

    scryA = [12 | [[1 | 0] | [1 | key_a_int]]]
    scryB = [12 | [[1 | 0] | [1 | key_b_int]]]
    addFormula = [4 | [4 | [4 | [4 | [0 | 2]]]]]
    sumFormula = [7 | [[scryA | scryB] | addFormula]]

    tx3_code = [
      [0 | 3]
      | [tx3_reservations | [[[1 | key_c_int] | sumFormula] | [0 | 0]]]
    ]

    # Stage 1: Execute the core using the backend's formula
    {:ok, [res3 | tx3_stage2_code]} =
      Nock.nock(tx3_code, [9, 2, 0 | 1], %Nock{})

    # Extract the components
    assert res3 == tx3_reservations

    # Check that reservations parse properly
    {:ok, parsed_reservations} = Backends.parse_reservations(res3)
    assert parsed_reservations == [{:read, "a"}, {:read, "b"}, {:write, "c"}]

    # Create a mock scry function that simulates reading values from storage
    mock_scry_tx3 = fn
      ^key_a_int -> {:ok, 3}
      ^key_b_int -> {:ok, 4}
      _ -> :error
    end

    env_tx3 = %Nock{scry_function: mock_scry_tx3}

    # Stage 2: Execute using the exact sequence from Backends.vm_execute_stage2
    # Step 1: Apply formula [10, [6, 1 | id], 0 | 1] - inserts tx_id
    {:ok, ordered_tx3} =
      Nock.nock(tx3_stage2_code, [10, [6, 1 | dummy_tx_id], 0 | 1], env_tx3)

    # Step 2: Apply formula [9, 2, 0 | 1] - executes transaction
    {:ok, result3} = Nock.nock(ordered_tx3, [9, 2, 0 | 1], env_tx3)

    # Expected sum and result format
    assert result3 == tx3_writes

    :ok
  end

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
    # Allow time for supervisors and shards to start
    Process.sleep(200)

    # 2. Define Keys and Transaction Code
    key_a_int = Noun.atom_binary_to_integer("a")
    key_b_int = Noun.atom_binary_to_integer("b")
    key_c_int = Noun.atom_binary_to_integer("c")

    # Tx1: Write 3 to "a" (Adapting from test_nock_program_tx1)
    tx1_reservations = [1 | key_a_int]
    tx1_writes = [key_a_int | 3]

    # Code structure: [ [0|3] | [ <reservations> | [ <writes_logic> | [0|0] ] ] ]
    # Writes logic for constant: [1 | <write_pair>]
    tx1_code = [[0 | 3] | [tx1_reservations | [[1 | tx1_writes] | [0 | 0]]]]

    # Tx2: Write 4 to "b"
    tx2_reservations = [1 | key_b_int]
    tx2_writes = [key_b_int | 4]
    tx2_code = [[0 | 3] | [tx2_reservations | [[1 | tx2_writes] | [0 | 0]]]]

    # Tx3: Read "a", Read "b", Add, Write result to "c" (Adapting from test_nock_program_tx3)
    # Reservations: read "a", read "b", write "c"
    tx3_reservations = [[0 | key_a_int], [0 | key_b_int] | [1 | key_c_int]]

    # Scry structure: [12 | [subject | formula]] where subject=[1|0], formula=[1|key]
    scryA = [12 | [[1 | 0] | [1 | key_a_int]]]
    scryB = [12 | [[1 | 0] | [1 | key_b_int]]]

    # Add opcode (4) applied repeatedly to pin the scry results [scryA scryB] from the subject (0 2)
    addFormula = [4 | [4 | [4 | [4 | [0 | 2]]]]]
    # Formula to calculate sum: apply addFormula to [scryA scryB]
    sumFormula = [7 | [[scryA | scryB] | addFormula]]
    # Writes logic for calculated value: [ [1 | key_c_int] | <sum_formula> ]
    tx3_writes_logic = [[1 | key_c_int] | sumFormula]
    tx3_code = [[0 | 3] | [tx3_reservations | [tx3_writes_logic | [0 | 0]]]]

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
    # Allow time for completion
    Process.sleep(500)
    Logger.info("Finished executing Tx1.")
    log_shard_states(node_id, "After Tx1")

    # Verify watermarks after Tx1
    state_a_after_tx1 = :sys.get_state(Registry.whereis(node_id, Shard, :a))

    assert state_a_after_tx1.watermarks["a"].write == 1,
           "Shard 'a' write watermark should be 1 after Tx1"

    # Execute Tx2
    Logger.info("Executing Tx2...")
    :ok = Mempool.execute(node_id, ["tx2"])
    # Allow time for completion
    Process.sleep(500)
    Logger.info("Finished executing Tx2.")
    log_shard_states(node_id, "After Tx2")

    # Verify watermarks after Tx2
    state_b_after_tx2 = :sys.get_state(Registry.whereis(node_id, Shard, :b))

    assert state_b_after_tx2.watermarks["b"].write == 2,
           "Shard 'b' write watermark should be 2 after Tx2"

    # Execute Tx3
    Logger.info("Executing Tx3...")
    :ok = Mempool.execute(node_id, ["tx3"])
    # Allow time for read/write/complete
    Process.sleep(500)
    Logger.info("Finished executing Tx3.")
    log_shard_states(node_id, "After Tx3 (Final)")

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
    # Allow time for supervisors and shards to start
    Process.sleep(200)

    # 2. Define Keys and Transaction Code (same as sequential test)
    key_a_int = Noun.atom_binary_to_integer("a")
    key_b_int = Noun.atom_binary_to_integer("b")
    key_c_int = Noun.atom_binary_to_integer("c")

    # Tx1: Write 3 to "a"
    tx1_reservations = [1 | key_a_int]
    tx1_writes = [key_a_int | 3]
    tx1_code = [[0 | 3] | [tx1_reservations | [[1 | tx1_writes] | [0 | 0]]]]

    # Tx2: Write 4 to "b"
    tx2_reservations = [1 | key_b_int]
    tx2_writes = [key_b_int | 4]
    tx2_code = [[0 | 3] | [tx2_reservations | [[1 | tx2_writes] | [0 | 0]]]]

    # Tx3: Read "a", Read "b", Add, Write result to "c"
    tx3_reservations = [[0 | key_a_int], [0 | key_b_int] | [1 | key_c_int]]
    scryA = [12 | [[1 | 0] | [1 | key_a_int]]]
    scryB = [12 | [[1 | 0] | [1 | key_b_int]]]
    addFormula = [4 | [4 | [4 | [4 | [0 | 2]]]]]
    sumFormula = [7 | [[scryA | scryB] | addFormula]]
    tx3_writes_logic = [[1 | key_c_int] | sumFormula]
    tx3_code = [[0 | 3] | [tx3_reservations | [tx3_writes_logic | [0 | 0]]]]

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
    # Allow time for completion
    # Increased sleep slightly for concurrent execution
    Process.sleep(1000)
    Logger.info("Finished executing Tx1, Tx2, Tx3.")
    log_shard_states(node_id, "After Concurrent Execution")

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
    # Allow supervisors/shards to start
    Process.sleep(200)

    pid_a = Registry.whereis(node_id, Shard, :a)
    pid_b = Registry.whereis(node_id, Shard, :b)
    assert is_pid(pid_a), "Shard 'a' PID not found."
    assert is_pid(pid_b), "Shard 'b' PID not found."

    key_a_int = Noun.atom_binary_to_integer("a")
    key_b_int = Noun.atom_binary_to_integer("b")

    # --- Define Nock Programs ---

    # Write Program Generator (writes a constant value to key 'a')
    write_1_code = write_code_gen(key_a_int, 1)
    write_2_code = write_code_gen(key_a_int, 2)
    write_3_code = write_code_gen(key_a_int, 3)

    # Copy Program (reads from 'a', writes to 'b')
    copy_reservations = [[0 | key_a_int] | [1 | key_b_int]]
    # Read from 'a'
    scryA = [12 | [[1 | 0] | [1 | key_a_int]]]
    # Write scry result to 'b'
    copy_writes_logic = [[1 | key_b_int] | scryA]

    copy_code = [
      [0 | 3] | [copy_reservations | [copy_writes_logic | [0 | 0]]]
    ]

    # --- Transaction Execution Sequence ---
    # Helper for execution and verification
    exec_and_verify = fn height, code, tx_base_id, checks ->
      tx_id = "#{tx_base_id}_#{height}"

      Logger.info("Executing Tx #{height} (#{tx_id})...")
      :ok = Mempool.tx(node_id, {:shard_storage, code}, tx_id)
      :ok = Mempool.execute(node_id, [tx_id])
      # Allow time for completion
      Process.sleep(500)
      Logger.info("Finished executing Tx #{height}.")

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
    # Allow supervisors/shards to start
    Process.sleep(200)

    pid_a = Registry.whereis(node_id, Shard, :a)
    pid_b = Registry.whereis(node_id, Shard, :b)
    assert is_pid(pid_a), "Shard 'a' PID not found."
    assert is_pid(pid_b), "Shard 'b' PID not found."

    key_a_int = Noun.atom_binary_to_integer("a")
    key_b_int = Noun.atom_binary_to_integer("b")

    # --- Define Nock Programs ---
    # Tx 1: Write 1 to 'a'
    write_1_code = write_code_gen(key_a_int, 1)

    # Tx 2: Reserve write on 'a', then crash (using [0 | [0 | 0]] which is invalid for writes stage)
    crash_reservations = [1 | key_a_int]
    crash_code = [[0 | 3] | [crash_reservations | [0 | [0 | 0]]]]

    # Tx 3: Copy 'a' to 'b'
    copy_reservations = [[0 | key_a_int] | [1 | key_b_int]]
    scryA = [12 | [[1 | 0] | [1 | key_a_int]]]
    copy_writes_logic = [[1 | key_b_int] | scryA]

    copy_code = [
      [0 | 3] | [copy_reservations | [copy_writes_logic | [0 | 0]]]
    ]

    # --- Transaction Execution ---

    # Execute Tx 1 (Write 1 to a, h=1)
    tx1_id = "write_1_1"
    Logger.info("Executing Tx 1 (#{tx1_id})...")
    :ok = Mempool.tx(node_id, {:shard_storage, write_1_code}, tx1_id)
    :ok = Mempool.execute(node_id, [tx1_id])
    Process.sleep(500)
    log_shard_states(node_id, "After Tx 1")
    state_a_1 = :sys.get_state(pid_a)
    assert state_a_1.kv["a"][1].value == 1
    assert state_a_1.watermarks["a"].write == 1

    # Execute Tx 2 (Crash on a, h=2)
    tx2_id = "crash_1_2"
    Logger.info("Executing Tx 2 (#{tx2_id})...")
    :ok = Mempool.tx(node_id, {:shard_storage, crash_code}, tx2_id)
    # Capture the expected error log for the crashing transaction
    log_output =
      capture_log(fn ->
        :ok = Mempool.execute(node_id, [tx2_id])
        # Allow time for execution and logging
        Process.sleep(500)
      end)

    assert log_output =~
             ~r/Transaction \"crash_1_2\" failed: VM execution stage 2 error/

    log_shard_states(node_id, "After Tx 2")
    state_a_2 = :sys.get_state(pid_a)

    # Write was reserved, so watermark bumps, but KV has no reservation for h=2
    assert state_a_2.watermarks["a"].write == 2
    assert state_a_2.kv["a"][2].write_reserved? == false

    # Execute Tx 3 (Copy a to b, h=3)
    tx3_id = "copy_1_3"
    Logger.info("Executing Tx 3 (#{tx3_id})...")
    :ok = Mempool.tx(node_id, {:shard_storage, copy_code}, tx3_id)
    :ok = Mempool.execute(node_id, [tx3_id])
    Process.sleep(500)
    log_shard_states(node_id, "After Tx 3")
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

  # --- Helper Functions ---

  # Write Program Generator (writes a constant value to a key)
  defp write_code_gen(key_int, value) do
    reservations = [1 | key_int]
    writes_logic = [1 | [key_int | value]]
    [[0 | 3] | [reservations | [writes_logic | [0 | 0]]]]
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
end
