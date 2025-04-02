defmodule Anoma.Node.Examples.EShard do
  @moduledoc """
  I contain examples on how to interact with the Shard module.
  """

  alias Anoma.Node.Examples.ENode
  alias Anoma.Node.Registry
  alias Anoma.Node.Transaction.Shard

  import ExUnit.Assertions

  @doc """
  I start a Shard with a predefined initial state and verify that
  reading the initial state (at height 0) returns the correct values.
  """
  @spec start_and_test_initial_state() :: ENode.t()
  def start_and_test_initial_state() do
    enode = ENode.start_node()
    node_id = enode.node_id
    shard_id = :test_shard_1
    shard_via = Registry.via(node_id, Shard, shard_id)

    initial_kv = %{
      "a" => 5,
      "c" => 15
      # "b" is intentionally omitted
    }

    # Start the shard
    {:ok, shard_pid} =
      Shard.start_link(node_id: node_id, id: shard_id, initial_kv: initial_kv)

    # --- Simulate Watermark Advancement prior to acquiring locks ---
    send(shard_pid, {:write_watermark_advanced, "c", 5})

    # --- Acquire Locks First ---
    {:ok, %{read: read_ref_a}} = Shard.lock(shard_via, "a", 0, :read)
    {:ok, %{read: read_ref_b}} = Shard.lock(shard_via, "b", 5, :read)
    {:ok, %{read: read_ref_c}} = Shard.lock(shard_via, "c", 4, :read)

    # --- Simulate Watermark Advancement after acquiring locks ---
    # Use send/2 because Shard handles these via handle_info
    send(shard_pid, {:write_watermark_advanced, "a", 0})
    send(shard_pid, {:write_watermark_advanced, "b", 10})

    # --- Test Reads (Now that watermarks allow immediate resolution) ---

    # Test key "a"
    assert Shard.read(shard_via, "a", 0, read_ref_a) == {:ok, 5}

    # Test key "b" (should be absent)
    assert Shard.read(shard_via, "b", 5, read_ref_b) == :absent

    # Test key "c"
    assert Shard.read(shard_via, "c", 4, read_ref_c) == {:ok, 15}

    enode
  end

  @doc """
  I test the scenario where a read is requested before the watermark allows,
  then the watermark advances, and the read completes.
  """
  @spec test_queued_read() :: ENode.t()
  def test_queued_read() do
    enode = ENode.start_node()
    node_id = enode.node_id
    shard_id = :test_shard_queued
    shard_via = Registry.via(node_id, Shard, shard_id)

    initial_kv = %{
      "a" => 5
    }

    # Start the shard
    {:ok, shard_pid} =
      Shard.start_link(node_id: node_id, id: shard_id, initial_kv: initial_kv)

    key = "a"
    height = 7

    # 1. Acquire Lock
    {:ok, %{read: read_ref}} = Shard.lock(shard_via, key, height, :read)

    # 2. Start the read in a separate task (it will block)
    read_task =
      Task.async(fn ->
        Shard.read(shard_via, key, height, read_ref)
      end)

    # Give the task a tiny moment to start and make the call
    Process.sleep(50)

    # Check shard state (debug)
    # :sys.get_state(shard_pid) |> IO.inspect(label: "Shard state before WM update")

    # 3. Advance the watermark *after* the read call is blocked
    send(shard_pid, {:write_watermark_advanced, key, height + 1})

    # Check shard state (debug)
    # :sys.get_state(shard_pid) |> IO.inspect(label: "Shard state AFTER WM update")

    # 4. Await the result from the task (should unblock now)
    # Use a timeout to prevent hangs if something is wrong
    result = Task.await(read_task, 1000)

    # 5. Assert the result
    assert result == {:ok, 5}

    enode
  end

  @doc """
  I test a variation of queued read where a write lock is acquired and
  a write is performed *after* the read is queued but *before* the read resolves,
  affecting the read's outcome.
  """
  @spec test_queued_read_with_intermediate_write() :: ENode.t()
  def test_queued_read_with_intermediate_write() do
    enode = ENode.start_node()
    node_id = enode.node_id
    shard_id = :test_shard_queued_write
    shard_via = Registry.via(node_id, Shard, shard_id)

    key = "a"
    initial_value = 5
    read_height = 7
    # Height for the intermediate write
    write_height = 5
    # Value for the intermediate write
    write_value = 10

    initial_kv = %{
      key => initial_value
    }

    # Start the shard
    {:ok, shard_pid} =
      Shard.start_link(node_id: node_id, id: shard_id, initial_kv: initial_kv)

    # 1. Acquire Read Lock for the future read
    {:ok, %{read: read_ref}} = Shard.lock(shard_via, key, read_height, :read)

    # 2. Start the read in a separate task (it will block)
    read_task =
      Task.async(fn ->
        Shard.read(shard_via, key, read_height, read_ref)
      end)

    # Give the task a moment to start and block on the read call
    Process.sleep(50)

    # 3. Acquire Write Lock for an intermediate height BEFORE watermark advances
    {:ok, %{write: write_ref_intermediate}} =
      Shard.lock(shard_via, key, write_height, :write)

    # 4. Advance the watermark AFTER the read call is blocked, enabling read resolution
    # WM >= read_height
    send(shard_pid, {:write_watermark_advanced, key, read_height + 1})

    # 5. Perform the Write AFTER watermark advanced but potentially before read task resumes
    # This write should be visible to the resolving read at height 7.
    assert :ok ==
             Shard.write(
               shard_via,
               key,
               write_value,
               write_height,
               write_ref_intermediate
             )

    # 6. Await the result from the read task (should unblock due to WM)
    # Use a timeout to prevent hangs
    result = Task.await(read_task, 1000)

    # 7. Assert the result
    # The read at height 7 should resolve to the latest write strictly below 7.
    # The write at height 5 (value 10) occurred before the read resolved.
    # The initial value at -1 is 5.
    # Therefore, the latest write < 7 is the one at height 5.
    assert result == {:ok, write_value}

    enode
  end

  @doc """
  I test the scenario where a read is requested, but the watermark never
  advances, causing the read to time out (within the Task.await).
  """
  @spec test_read_timeout() :: ENode.t()
  def test_read_timeout() do
    enode = ENode.start_node()
    node_id = enode.node_id
    shard_id = :test_shard_timeout
    shard_via = Registry.via(node_id, Shard, shard_id)

    # Start the shard (initial state doesn't matter)
    {:ok, _shard_pid} =
      Shard.start_link(node_id: node_id, id: shard_id, initial_kv: %{})

    key = "a"
    height = 5

    # 1. Acquire Lock
    {:ok, %{read: read_ref}} = Shard.lock(shard_via, key, height, :read)

    # 2. Start the read in a separate task (it will block)
    read_task =
      Task.async(fn ->
        # Note: The GenServer.call within Shard.read uses :infinity,
        # so this task itself won't timeout internally. The timeout
        # comes from Task.await below.
        Shard.read(shard_via, key, height, read_ref)
      end)

    # 3. DO NOT advance the watermark

    # 4. Await the result with a short timeout
    # We expect this to exit with reason :timeout
    try do
      # 100ms timeout
      Task.await(read_task, 100)
      # If await succeeds, the test fails
      flunk("Task.await should have timed out and exited, but it returned.")
    catch
      :exit, reason ->
        # Explicitly check the exit reason tuple if needed,
        # but typically checking the first element is sufficient for timeouts.
        assert reason == :timeout or match?({:timeout, _}, reason)
    end

    # Ensure the task is shut down to avoid lingering processes
    if Process.alive?(read_task.pid),
      do: Task.shutdown(read_task, :brutal_kill)

    enode
  end

  @doc """
  I test a scenario with two pending reads at different heights.
  An intermediate watermark advance unblocks only the lower-height read,
  while the higher-height read eventually times out.
  """
  @spec test_partial_read_unblocking_with_timeout() :: ENode.t()
  def test_partial_read_unblocking_with_timeout() do
    enode = ENode.start_node()
    node_id = enode.node_id
    shard_id = :test_shard_partial_unblock
    shard_via = Registry.via(node_id, Shard, shard_id)
    key = "a"
    initial_value = 1

    read_height_ok = 5
    read_height_timeout = 15
    watermark_height = 10

    # Start the shard with an initial value
    {:ok, shard_pid} =
      Shard.start_link(
        node_id: node_id,
        id: shard_id,
        initial_kv: %{key => initial_value}
      )

    # 1. Acquire Locks
    {:ok, %{read: read_ref_ok}} =
      Shard.lock(shard_via, key, read_height_ok, :read)

    {:ok, %{read: read_ref_timeout}} =
      Shard.lock(shard_via, key, read_height_timeout, :read)

    # 2. Start Read Tasks (both will block initially)
    read_task_ok =
      Task.async(fn ->
        Shard.read(shard_via, key, read_height_ok, read_ref_ok)
      end)

    read_task_timeout =
      Task.async(fn ->
        Shard.read(shard_via, key, read_height_timeout, read_ref_timeout)
      end)

    # Give tasks time to start and block
    Process.sleep(50)

    # 3. Advance Watermark partially (enough for height 5, not for 15)
    send(shard_pid, {:write_watermark_advanced, key, watermark_height})

    # 4. Await the read that should succeed
    # Generous timeout
    result_ok = Task.await(read_task_ok, 1000)
    # Read at height 5 resolves based on latest write < 5, which is height -1
    assert result_ok == {:ok, initial_value}

    # 5. Await the read that should time out
    try do
      # Short timeout
      Task.await(read_task_timeout, 100)

      flunk(
        "Task for height #{read_height_timeout} should have timed out, but it returned."
      )
    catch
      :exit, reason ->
        assert reason == :timeout or match?({:timeout, _}, reason)
    end

    # Ensure the timed-out task is shut down
    if Process.alive?(read_task_timeout.pid),
      do: Task.shutdown(read_task_timeout, :brutal_kill)

    enode
  end

  @doc """
  I test a more complex scenario involving multiple writes, reads, and
  write watermark advancements.
  """
  @spec test_complex_write_and_read_scenario() :: ENode.t()
  def test_complex_write_and_read_scenario() do
    enode = ENode.start_node()
    node_id = enode.node_id
    shard_id = :test_shard_complex_writes
    shard_via = Registry.via(node_id, Shard, shard_id)
    key = "a"

    initial_kv = %{
      key => 3
    }

    # Start the shard
    {:ok, shard_pid} =
      Shard.start_link(node_id: node_id, id: shard_id, initial_kv: initial_kv)

    # --- Acquire Write Locks ---
    {:ok, %{write: write_ref_5}} = Shard.lock(shard_via, key, 5, :write)
    {:ok, %{write: write_ref_6}} = Shard.lock(shard_via, key, 6, :write)
    {:ok, %{write: write_ref_10}} = Shard.lock(shard_via, key, 10, :write)

    # --- Acquire Read Locks ---
    {:ok, %{read: read_ref_0}} = Shard.lock(shard_via, key, 0, :read)
    {:ok, %{read: read_ref_4}} = Shard.lock(shard_via, key, 4, :read)
    {:ok, %{read: read_ref_5}} = Shard.lock(shard_via, key, 5, :read)
    {:ok, %{read: read_ref_6}} = Shard.lock(shard_via, key, 6, :read)
    {:ok, %{read: read_ref_7}} = Shard.lock(shard_via, key, 7, :read)
    {:ok, %{read: read_ref_9}} = Shard.lock(shard_via, key, 9, :read)
    {:ok, %{read: read_ref_10}} = Shard.lock(shard_via, key, 10, :read)
    {:ok, %{read: read_ref_11}} = Shard.lock(shard_via, key, 11, :read)

    # --- Perform Writes ---
    assert :ok == Shard.write(shard_via, key, 7, 5, write_ref_5)
    assert :ok == Shard.write(shard_via, key, 2, 6, write_ref_6)
    assert :ok == Shard.write(shard_via, key, 8, 10, write_ref_10)

    # --- Simulate Watermark Advancements ---
    # Reads 0, 4, 5 depend on initial state (implied WM >= 0)
    # Effectively done by init
    send(shard_pid, {:write_watermark_advanced, key, 0})

    # Read 6 needs to see write at 5
    send(shard_pid, {:write_watermark_advanced, key, 6})

    # Reads 7, 9, 10 need to see write at 6
    # WM advances to max(current, new)
    send(shard_pid, {:write_watermark_advanced, key, 7})

    # Read 11 needs to see write at 10
    send(shard_pid, {:write_watermark_advanced, key, 11})

    # --- Test Reads ---
    # Read height h resolves based on latest write < h, provided WM >= h
    # Before any writes
    assert Shard.read(shard_via, key, 0, read_ref_0) == {:ok, 3}
    # Before write@5
    assert Shard.read(shard_via, key, 4, read_ref_4) == {:ok, 3}
    # Before write@5
    assert Shard.read(shard_via, key, 5, read_ref_5) == {:ok, 3}
    # Sees write@5
    assert Shard.read(shard_via, key, 6, read_ref_6) == {:ok, 7}
    # Sees write@6
    assert Shard.read(shard_via, key, 7, read_ref_7) == {:ok, 2}
    # Sees write@6
    assert Shard.read(shard_via, key, 9, read_ref_9) == {:ok, 2}
    # Sees write@6
    assert Shard.read(shard_via, key, 10, read_ref_10) == {:ok, 2}
    # Sees write@10
    assert Shard.read(shard_via, key, 11, read_ref_11) == {:ok, 8}

    enode
  end

  @doc """
  I test the internal state changes related to Garbage Collection (GC)
  and the state of entries after locks are released, using `:sys.get_state`
  for introspection.
  """
  @spec test_gc_and_lock_release_state() :: ENode.t()
  def test_gc_and_lock_release_state() do
    enode = ENode.start_node()
    node_id = enode.node_id
    shard_id = :test_shard_gc_lock_release
    shard_via = Registry.via(node_id, Shard, shard_id)
    key = "a"

    initial_kv = %{key => 3}

    # Start the shard
    {:ok, shard_pid} =
      Shard.start_link(node_id: node_id, id: shard_id, initial_kv: initial_kv)

    # --- Writes ---
    write_ops = %{
      9 => 5,
      15 => 12,
      30 => 16,
      32 => 8
    }

    Enum.each(write_ops, fn {h, v} ->
      {:ok, %{write: write_ref}} = Shard.lock(shard_via, key, h, :write)
      assert :ok == Shard.write(shard_via, key, v, h, write_ref)
    end)

    # --- Direct State Check (Post-Write) ---
    # Use :sys.get_state for internal inspection (testing only)
    state1 = :sys.get_state(shard_pid)
    kv1 = state1.kv[key]

    assert Map.get(kv1, -1).value == 3

    assert Map.get(kv1, 9).value == 5 and
             is_nil(Map.get(kv1, 9).write_lock_ref)

    assert Map.get(kv1, 15).value == 12 and
             is_nil(Map.get(kv1, 15).write_lock_ref)

    assert Map.get(kv1, 30).value == 16 and
             is_nil(Map.get(kv1, 30).write_lock_ref)

    assert Map.get(kv1, 32).value == 8 and
             is_nil(Map.get(kv1, 32).write_lock_ref)

    # -1, 9, 15, 30, 32
    assert map_size(kv1) == 5

    # --- Read Lock ---
    {:ok, %{read: read_ref_17}} = Shard.lock(shard_via, key, 17, :read)

    # Verify lock presence in state
    state2 = :sys.get_state(shard_pid)
    kv2 = state2.kv[key]
    assert kv2[17].read_lock_ref == read_ref_17
    assert is_nil(kv2[17].value)
    # Added entry for height 17
    assert map_size(kv2) == 6

    # --- Advance Read Watermark (GC Trigger) ---
    send(shard_pid, {:read_watermark_advanced, key, 33})
    # Allow time for message processing
    Process.sleep(50)

    # --- Direct State Check (Post-GC) ---
    state3 = :sys.get_state(shard_pid)
    kv3 = state3.kv[key]

    # Expected remaining heights:
    # - 15: Kept because it's needed for read lock at 17 (max_h < 17)
    # - 17: Kept because it holds the active read lock.
    # - 32: Kept because it's the latest entry <= the watermark 33.
    assert Map.has_key?(kv3, 15)
    # Check value consistency
    assert Map.get(kv3, 15).value == 12
    assert Map.has_key?(kv3, 17)
    # Lock still held
    assert kv3[17].read_lock_ref == read_ref_17
    assert Map.has_key?(kv3, 32)
    # Check value consistency
    assert Map.get(kv3, 32).value == 8
    assert map_size(kv3) == 3
    # Ensure others are gone
    refute Map.has_key?(kv3, -1)
    refute Map.has_key?(kv3, 9)
    refute Map.has_key?(kv3, 30)

    # --- Read Operation (at 17) ---
    # Advance WRITE watermark so read can resolve
    send(shard_pid, {:write_watermark_advanced, key, 18})
    # Perform the read
    assert Shard.read(shard_via, key, 17, read_ref_17) == {:ok, 12}

    # --- Direct State Check (Post-Read) ---
    state4 = :sys.get_state(shard_pid)
    kv4 = state4.kv[key]
    # Entry should still exist
    assert Map.has_key?(kv4, 17)
    # Lock should be released
    assert is_nil(kv4[17].read_lock_ref)
    assert is_nil(kv4[17].value)
    assert is_nil(kv4[17].write_lock_ref)
    # Size remains same, just lock released
    assert map_size(kv4) == 3

    # --- Advance Read Watermark Again (Clean up entry 17) ---
    send(shard_pid, {:read_watermark_advanced, key, 34})
    Process.sleep(50)

    # --- Direct State Check (Final) ---
    state5 = :sys.get_state(shard_pid)
    kv5 = state5.kv[key]

    # Expected remaining heights:
    # - 32: Kept because it's the latest entry <= the new watermark 34.
    # Entries 15 and 17 should now be GC'd.
    assert Map.has_key?(kv5, 32)
    assert Map.get(kv5, 32).value == 8
    assert map_size(kv5) == 1
    # Ensure others are gone
    refute Map.has_key?(kv5, 15)
    refute Map.has_key?(kv5, 17)

    enode
  end

  @doc """
  I test various scenarios of lock acquisition failures due to watermarks,
  existing values, and successful re-acquisition of existing locks.
  """
  @spec test_lock_failures_and_reacquisition() :: ENode.t()
  def test_lock_failures_and_reacquisition() do
    enode = ENode.start_node()
    node_id = enode.node_id
    shard_id = :test_shard_lock_failures
    shard_via = Registry.via(node_id, Shard, shard_id)
    key = "a"

    # Start the shard (empty initial state)
    {:ok, shard_pid} =
      Shard.start_link(node_id: node_id, id: shard_id, initial_kv: %{})

    # --- Setup Watermarks ---
    send(shard_pid, {:read_watermark_advanced, key, 10})
    send(shard_pid, {:write_watermark_advanced, key, 10})
    # Allow messages to process
    Process.sleep(50)

    # --- Test Locking Below Watermarks (Height 5) ---
    assert Shard.lock(shard_via, key, 5, :read) ==
             {:error, :locking_read_past_read_watermark}

    assert Shard.lock(shard_via, key, 5, :write) ==
             {:error, :locking_write_past_write_watermark}

    # Write check happens first for :read_write
    assert Shard.lock(shard_via, key, 5, :read_write) ==
             {:error, :locking_write_past_write_watermark}

    # --- Test Lock Re-acquisition (Height 15) ---
    # Sequence: read -> read -> write -> write -> read

    # 1st Read
    {:ok, %{read: read_ref_15_a, write: nil}} =
      Shard.lock(shard_via, key, 15, :read)

    assert is_reference(read_ref_15_a)

    # 2nd Read (should return same ref)
    {:ok, %{read: read_ref_15_b, write: nil}} =
      Shard.lock(shard_via, key, 15, :read)

    assert read_ref_15_a == read_ref_15_b

    # 1st Write (acquire alongside read)
    {:ok, %{read: read_ref_15_c, write: write_ref_15_a}} =
      Shard.lock(shard_via, key, 15, :write)

    # Read ref should persist
    assert read_ref_15_a == read_ref_15_c
    assert is_reference(write_ref_15_a)

    # 2nd Write (should return same refs)
    {:ok, %{read: read_ref_15_d, write: write_ref_15_b}} =
      Shard.lock(shard_via, key, 15, :write)

    assert read_ref_15_a == read_ref_15_d
    assert write_ref_15_a == write_ref_15_b

    # 3rd Read (should return same refs)
    {:ok, %{read: read_ref_15_e, write: write_ref_15_c}} =
      Shard.lock(shard_via, key, 15, :read)

    assert read_ref_15_a == read_ref_15_e
    assert write_ref_15_a == write_ref_15_c

    # --- Test Write Blocking Lock Acquisition (Height 20) ---
    # First, write a value to height 20
    {:ok, %{write: write_ref_20_setup}} =
      Shard.lock(shard_via, key, 20, :write)

    assert :ok ==
             Shard.write(
               shard_via,
               key,
               "value_at_20",
               20,
               write_ref_20_setup
             )

    # Sequence: write -> write -> read -> read -> write

    # 1st Write (should fail due to existing value)
    assert Shard.lock(shard_via, key, 20, :write) ==
             {:error, :slot_occupied_by_value}

    # 2nd Write (should fail)
    assert Shard.lock(shard_via, key, 20, :write) ==
             {:error, :slot_occupied_by_value}

    # 1st Read (should succeed even with value)
    {:ok, %{read: read_ref_20_a, write: nil}} =
      Shard.lock(shard_via, key, 20, :read)

    assert is_reference(read_ref_20_a)

    # 2nd Read (should succeed, return same ref)
    {:ok, %{read: read_ref_20_b, write: nil}} =
      Shard.lock(shard_via, key, 20, :read)

    assert read_ref_20_a == read_ref_20_b

    # 3rd Write (should fail)
    assert Shard.lock(shard_via, key, 20, :write) ==
             {:error, :slot_occupied_by_value}

    enode
  end

  @doc """
  I test that a read can resolve successfully even if an older write lock
  (at a height lower than the height of the value the read depends on)
  is still held. This verifies a fix for overly broad write lock blocking.
  """
  @spec test_read_past_old_write_lock() :: ENode.t()
  def test_read_past_old_write_lock() do
    enode = ENode.start_node()
    node_id = enode.node_id
    shard_id = :test_shard_read_past_lock
    shard_via = Registry.via(node_id, Shard, shard_id)
    key = "a"

    h_lock = 5
    h_write = 7
    write_value = 10
    h_read = 9

    # Start the shard with initial value
    {:ok, shard_pid} =
      Shard.start_link(node_id: node_id, id: shard_id, initial_kv: %{})

    # 1. Acquire write lock at h_lock (and HOLD it)
    {:ok, %{write: write_ref_lock}} =
      Shard.lock(shard_via, key, h_lock, :write)

    # 2. Write successfully at h_write
    {:ok, %{write: write_ref_write}} =
      Shard.lock(shard_via, key, h_write, :write)

    assert :ok ==
             Shard.write(
               shard_via,
               key,
               write_value,
               h_write,
               write_ref_write
             )

    # 3. Acquire read lock at h_read
    {:ok, %{read: read_ref_read}} = Shard.lock(shard_via, key, h_read, :read)

    # 4. Advance write watermark to allow the read at h_read
    # WM >= 9
    send(shard_pid, {:write_watermark_advanced, key, h_read + 1})

    # 5. Perform the read at h_read
    result = Shard.read(shard_via, key, h_read, read_ref_read)

    # 6. Assert: Read at 9 should resolve to value written at 7,
    #    despite the older write lock still held at 5.
    assert result == {:ok, write_value}

    # 7. Verify the lock at h_lock is still held (for sanity)
    state = :sys.get_state(shard_pid)
    assert state.kv[key][h_lock].write_lock_ref == write_ref_lock

    enode
  end

  @doc """
  I test writing to an initially empty shard, advancing the write watermark,
  and then performing reads both below and above the write height.
  """
  @spec test_write_then_reads_empty_start() :: ENode.t()
  def test_write_then_reads_empty_start() do
    enode = ENode.start_node()
    node_id = enode.node_id
    shard_id = :test_shard_empty_start_rw
    shard_via = Registry.via(node_id, Shard, shard_id)
    key = "a"
    write_height = 10
    write_value = 5
    wm_height = 20
    read_height_absent = 5
    read_height_ok = 15

    # Start the shard with empty initial state
    {:ok, shard_pid} =
      Shard.start_link(node_id: node_id, id: shard_id, initial_kv: %{})

    # 1. Write value at write_height
    {:ok, %{write: write_ref}} =
      Shard.lock(shard_via, key, write_height, :write)

    assert :ok ==
             Shard.write(shard_via, key, write_value, write_height, write_ref)

    # 2. Advance write watermark past the write and reads
    send(shard_pid, {:write_watermark_advanced, key, wm_height})
    # Allow message processing
    Process.sleep(50)

    # 3. Read at height_absent (should be absent as latest < 5 is nothing)
    {:ok, %{read: read_ref_absent}} =
      Shard.lock(shard_via, key, read_height_absent, :read)

    assert Shard.read(shard_via, key, read_height_absent, read_ref_absent) ==
             :absent

    # 4. Read at height_ok (should see write_value as latest < 15 is at 10)
    {:ok, %{read: read_ref_ok}} =
      Shard.lock(shard_via, key, read_height_ok, :read)

    assert Shard.read(shard_via, key, read_height_ok, read_ref_ok) ==
             {:ok, write_value}

    enode
  end
end
