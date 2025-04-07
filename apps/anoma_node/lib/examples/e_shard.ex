defmodule Anoma.Node.Examples.EShard do
  @moduledoc """
  I contain examples on how to interact with the Shard module.
  """

  alias Anoma.Node.Registry
  alias Anoma.Node.Transaction.Shard

  import ExUnit.Assertions

  @doc """
  I start a Shard with a predefined initial state and verify that
  reading the initial state (at height 0) returns the correct values.
  """
  @spec start_and_test_initial_state() :: :ok
  def start_and_test_initial_state() do
    node_id = "test_shard_1_node"
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

    # --- Simulate Watermark Advancement prior to acquiring reservations ---
    send(shard_pid, {:write_watermark_advanced, "c", 5})

    # --- Acquire Reservations First ---
    assert :ok == Shard.reserve(shard_via, "a", 0, :read)
    assert :ok == Shard.reserve(shard_via, "b", 5, :read)
    assert :ok == Shard.reserve(shard_via, "c", 4, :read)

    # --- Simulate Watermark Advancement after acquiring reservations ---
    # Use send/2 because Shard handles these via handle_info
    send(shard_pid, {:write_watermark_advanced, "a", 0})
    send(shard_pid, {:write_watermark_advanced, "b", 10})

    # --- Test Reads (Now that watermarks allow immediate resolution) ---

    # Test key "a"
    assert Shard.read(shard_via, "a", 0) == {:ok, 5}

    # Test key "b"
    assert Shard.read(shard_via, "b", 5) == :absent

    # Test key "c"
    assert Shard.read(shard_via, "c", 4) == {:ok, 15}

    :ok
  end

  @doc """
  I test a scenario where a read is requested before the watermark allows,
  then the watermark advances, and the read completes.
  """
  @spec test_queued_read() :: :ok
  def test_queued_read() do
    node_id = "test_shard_queued_node"
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

    # 1. Acquire Reservation
    assert :ok == Shard.reserve(shard_via, key, height, :read)

    # 2. Start the read in a separate task (it will block)
    read_task =
      Task.async(fn ->
        Shard.read(shard_via, key, height)
      end)

    # Give the task a tiny moment to start and make the call
    Process.sleep(50)

    # 3. Advance the watermark *after* the read call is blocked
    send(shard_pid, {:write_watermark_advanced, key, height + 1})

    # 4. Wait for the result from the task (should unblock now)
    result = Task.await(read_task, 1000)

    # 5. Assert the result
    assert result == {:ok, 5}

    :ok
  end

  @doc """
  I test a variation of queued read where a write reservation is acquired and
  a write is performed *after* the read is queued but *before* the read resolves,
  affecting the read's outcome.
  """
  @spec test_queued_read_with_intermediate_write() :: :ok
  def test_queued_read_with_intermediate_write() do
    node_id = "test_shard_queued_write_node"
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

    # 1. Acquire Read Reservation for the future read
    assert :ok == Shard.reserve(shard_via, key, read_height, :read)

    # 2. Start the read in a separate task (it will block)
    read_task =
      Task.async(fn ->
        Shard.read(shard_via, key, read_height)
      end)

    # Give the task a moment to start and block on the read call
    Process.sleep(50)

    # 3. Acquire Write Reservation for an intermediate height BEFORE watermark advances
    assert :ok == Shard.reserve(shard_via, key, write_height, :write)

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
               write_height
             )

    # 6. Await the result from the read task (should unblock due to WM)
    result = Task.await(read_task, 1000)

    # 7. Assert the result
    # The read at height 7 should resolve to the latest write strictly below 7.
    # The write at height 5 (value 10) occurred before the read resolved.
    # The initial value at -1 is 5.
    # Therefore, the latest write < 7 is the one at height 5.
    assert result == {:ok, write_value}

    :ok
  end

  @doc """
  I test a scenario where a read is requested, but the watermark never
  advances, causing the read to time out.
  """
  @spec test_read_timeout() :: :ok | nil
  def test_read_timeout() do
    node_id = "test_shard_timeout_node"
    shard_id = :test_shard_timeout
    shard_via = Registry.via(node_id, Shard, shard_id)

    # Start the shard (initial state doesn't matter)
    {:ok, _shard_pid} =
      Shard.start_link(node_id: node_id, id: shard_id, initial_kv: %{})

    key = "a"
    height = 5

    # 1. Acquire Reservation
    assert :ok == Shard.reserve(shard_via, key, height, :read)

    # 2. Start the read in a separate task (it will block)
    read_task =
      Task.async(fn ->
        # Note: The GenServer.call within Shard.read uses :infinity,
        # so this task itself won't timeout internally. The timeout
        # comes from Task.await below.
        Shard.read(shard_via, key, height)
      end)

    # 3. DO NOT advance the watermark

    # 4. Await the result with a short timeout
    # We expect this to exit with reason :timeout
    try do
      Task.await(read_task, 100)
      # If await succeeds, the test fails
      flunk("Task.await should have timed out and exited, but it returned.")
    catch
      :exit, reason ->
        assert reason == :timeout or match?({:timeout, _}, reason)
    end

    if Process.alive?(read_task.pid),
      do: Task.shutdown(read_task, :brutal_kill)
  end

  @doc """
  I test a scenario with two pending reads at different heights.
  An intermediate watermark advance unblocks only the lower-height read,
  while the higher-height read eventually times out.
  """
  @spec test_partial_read_unblocking_with_timeout() :: :ok | nil
  def test_partial_read_unblocking_with_timeout() do
    node_id = "test_shard_partial_unblock_node"
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

    # 1. Acquire Reservations
    assert :ok == Shard.reserve(shard_via, key, read_height_ok, :read)

    assert :ok == Shard.reserve(shard_via, key, read_height_timeout, :read)

    # 2. Start Read Tasks (both will block initially)
    read_task_ok =
      Task.async(fn ->
        Shard.read(shard_via, key, read_height_ok)
      end)

    read_task_timeout =
      Task.async(fn ->
        Shard.read(shard_via, key, read_height_timeout)
      end)

    # Give tasks time to start and block
    Process.sleep(50)

    # 3. Advance Watermark partially (enough for height 5, not for 15)
    send(shard_pid, {:write_watermark_advanced, key, watermark_height})

    # 4. Await the read that should succeed
    result_ok = Task.await(read_task_ok, 1000)
    # Read at height 5 resolves based on latest write < 5, which is height -1
    assert result_ok == {:ok, initial_value}

    # 5. Await the read that should time out
    try do
      Task.await(read_task_timeout, 100)

      flunk(
        "Task for height #{read_height_timeout} should have timed out, but it returned."
      )
    catch
      :exit, reason ->
        assert reason == :timeout or match?({:timeout, _}, reason)
    end

    if Process.alive?(read_task_timeout.pid),
      do: Task.shutdown(read_task_timeout, :brutal_kill)
  end

  @doc """
  I test a more complex scenario involving multiple writes, reads, and
  write watermark advancements.
  """
  @spec test_complex_write_and_read_scenario() :: :ok
  def test_complex_write_and_read_scenario() do
    node_id = "test_shard_complex_writes_node"
    shard_id = :test_shard_complex_writes
    shard_via = Registry.via(node_id, Shard, shard_id)
    key = "a"

    initial_kv = %{
      key => 3
    }

    # Start the shard
    {:ok, shard_pid} =
      Shard.start_link(node_id: node_id, id: shard_id, initial_kv: initial_kv)

    # --- Acquire Write Reservations ---
    assert :ok == Shard.reserve(shard_via, key, 5, :write)
    assert :ok == Shard.reserve(shard_via, key, 6, :write)
    assert :ok == Shard.reserve(shard_via, key, 10, :write)

    # --- Acquire Read Reservations ---
    assert :ok == Shard.reserve(shard_via, key, 0, :read)
    assert :ok == Shard.reserve(shard_via, key, 4, :read)
    assert :ok == Shard.reserve(shard_via, key, 5, :read)
    assert :ok == Shard.reserve(shard_via, key, 6, :read)
    assert :ok == Shard.reserve(shard_via, key, 7, :read)
    assert :ok == Shard.reserve(shard_via, key, 9, :read)
    assert :ok == Shard.reserve(shard_via, key, 10, :read)
    assert :ok == Shard.reserve(shard_via, key, 11, :read)

    # --- Perform Writes ---
    assert :ok == Shard.write(shard_via, key, 7, 5)
    assert :ok == Shard.write(shard_via, key, 2, 6)
    assert :ok == Shard.write(shard_via, key, 8, 10)

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
    assert Shard.read(shard_via, key, 0) == {:ok, 3}
    # Before write@5
    assert Shard.read(shard_via, key, 4) == {:ok, 3}
    # Before write@5
    assert Shard.read(shard_via, key, 5) == {:ok, 3}
    # Sees write@5
    assert Shard.read(shard_via, key, 6) == {:ok, 7}
    # Sees write@6
    assert Shard.read(shard_via, key, 7) == {:ok, 2}
    # Sees write@6
    assert Shard.read(shard_via, key, 9) == {:ok, 2}
    # Sees write@6
    assert Shard.read(shard_via, key, 10) == {:ok, 2}
    # Sees write@10
    assert Shard.read(shard_via, key, 11) == {:ok, 8}

    :ok
  end

  @doc """
  I test the internal state changes related to Garbage Collection (GC)
  and the state of entries after reservations are released.
  """
  @spec test_gc_and_reserve_release_state() :: :ok
  def test_gc_and_reserve_release_state() do
    node_id = "test_shard_gc_reserve_release_node"
    shard_id = :test_shard_gc_reserve_release
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
      assert :ok == Shard.reserve(shard_via, key, h, :write)
      assert :ok == Shard.write(shard_via, key, v, h)
    end)

    # --- Direct State Check (Post-Write) ---
    # Use :sys.get_state for internal inspection since Shard.read is stateful
    state1 = :sys.get_state(shard_pid)
    kv1 = state1.kv[key]

    assert Map.get(kv1, -1).value == 3
    assert !Map.get(kv1, -1).write_reserved?

    assert Map.get(kv1, 9).value == 5 and !Map.get(kv1, 9).write_reserved?
    assert Map.get(kv1, 15).value == 12 and !Map.get(kv1, 15).write_reserved?
    assert Map.get(kv1, 30).value == 16 and !Map.get(kv1, 30).write_reserved?
    assert Map.get(kv1, 32).value == 8 and !Map.get(kv1, 32).write_reserved?

    # -1, 9, 15, 30, 32
    assert map_size(kv1) == 5

    # --- Read Reservation ---
    assert :ok == Shard.reserve(shard_via, key, 17, :read)

    # Verify reservation presence in state
    state2 = :sys.get_state(shard_pid)
    kv2 = state2.kv[key]
    assert kv2[17].read_reserved?
    assert is_nil(kv2[17].value)
    assert !kv2[17].write_reserved?
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
    # - 15: Kept because it's needed for read reservation at 17 (max_h < 17)
    # - 17: Kept because it holds the active read reservation.
    # - 32: Kept because it's the latest entry <= the watermark 33.
    assert Map.has_key?(kv3, 15)
    # Check value consistency
    assert Map.get(kv3, 15).value == 12
    assert Map.has_key?(kv3, 17)
    # Reservation still held
    assert kv3[17].read_reserved?
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
    assert Shard.read(shard_via, key, 17) == {:ok, 12}

    # --- Direct State Check (Post-Read) ---
    state4 = :sys.get_state(shard_pid)
    kv4 = state4.kv[key]
    # Entry should still exist
    assert Map.has_key?(kv4, 17)
    # Reservation should be released
    assert !kv4[17].read_reserved?
    assert is_nil(kv4[17].value)
    assert !kv4[17].write_reserved?
    # Size remains same, just reservation released
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

    :ok
  end

  @doc """
  I test various scenarios of reservation acquisition failures due to watermarks,
  existing values, and successful re-acquisition of existing reservations.
  """
  @spec test_reserve_failures_and_reacquisition() :: :ok
  def test_reserve_failures_and_reacquisition() do
    node_id = "test_shard_reserve_failures_node"
    shard_id = :test_shard_reserve_failures
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

    # --- Test Reserving Below Watermarks (Height 5) ---
    assert Shard.reserve(shard_via, key, 5, :read) ==
             {:error, :reserving_read_under_read_watermark}

    assert Shard.reserve(shard_via, key, 5, :write) ==
             {:error, :reserving_write_under_write_watermark}

    # Write check happens first for :read_write
    assert Shard.reserve(shard_via, key, 5, :read_write) ==
             {:error, :reserving_write_under_write_watermark}

    # --- Test Reservation Re-acquisition (Height 15) ---
    # Sequence: read -> read -> write -> write -> read

    # 1st Read
    assert :ok == Shard.reserve(shard_via, key, 15, :read)

    # 2nd Read (should be ok, idempotent)
    assert :ok == Shard.reserve(shard_via, key, 15, :read)

    # 1st Write (acquire alongside read)
    assert :ok == Shard.reserve(shard_via, key, 15, :write)

    # 2nd Write (should be ok, idempotent)
    assert :ok == Shard.reserve(shard_via, key, 15, :write)

    # 3rd Read (should be ok, idempotent)
    assert :ok == Shard.reserve(shard_via, key, 15, :read)

    # Check state: both read and write should be reserved
    state_after_reacquire = :sys.get_state(shard_pid)
    kv_after_reacquire = state_after_reacquire.kv[key]
    assert kv_after_reacquire[15].read_reserved?
    assert kv_after_reacquire[15].write_reserved?

    # --- Test Write Blocking Reservation Acquisition (Height 20) ---
    # First, reserve and write a value to height 20
    assert :ok == Shard.reserve(shard_via, key, 20, :write)

    assert :ok == Shard.write(shard_via, key, "value_at_20", 20)

    # Sequence: write -> write -> read -> read -> write

    # 1st Write (should fail due to existing value)
    assert Shard.reserve(shard_via, key, 20, :write) ==
             {:error, :slot_occupied_by_value}

    # 2nd Write (should fail)
    assert Shard.reserve(shard_via, key, 20, :write) ==
             {:error, :slot_occupied_by_value}

    # 1st Read (should succeed even with value)
    assert :ok == Shard.reserve(shard_via, key, 20, :read)

    # 2nd Read (should succeed, idempotent)
    assert :ok == Shard.reserve(shard_via, key, 20, :read)

    # 3rd Write (should fail)
    assert Shard.reserve(shard_via, key, 20, :write) ==
             {:error, :slot_occupied_by_value}

    # Check state: read should be reserved, write should not
    state_after_blocking = :sys.get_state(shard_pid)
    kv_after_blocking = state_after_blocking.kv[key]
    assert kv_after_blocking[20].read_reserved?
    assert !kv_after_blocking[20].write_reserved?

    :ok
  end

  @doc """
  I test that a read can resolve successfully even if an older write reservation
  (at a height lower than the height of the value the read depends on)
  is still held. This verifies a fix for overly broad write reservation blocking.
  """
  @spec test_read_past_old_write_reserve() :: :ok
  def test_read_past_old_write_reserve() do
    node_id = "test_shard_read_past_reservation_node"
    shard_id = :test_shard_read_past_reservation
    shard_via = Registry.via(node_id, Shard, shard_id)
    key = "a"

    h_reserve = 5
    h_write = 7
    write_value = 10
    h_read = 9

    # Start the shard with initial value
    {:ok, shard_pid} =
      Shard.start_link(node_id: node_id, id: shard_id, initial_kv: %{})

    # 1. Acquire write reservation at h_reserve (and HOLD it)
    assert :ok == Shard.reserve(shard_via, key, h_reserve, :write)

    # 2. Write successfully at h_write
    assert :ok == Shard.reserve(shard_via, key, h_write, :write)
    assert :ok == Shard.write(shard_via, key, write_value, h_write)

    # 3. Acquire read reservation at h_read
    assert :ok == Shard.reserve(shard_via, key, h_read, :read)

    # 4. Advance write watermark to allow the read at h_read
    # WM >= 9
    send(shard_pid, {:write_watermark_advanced, key, h_read + 1})

    # 5. Perform the read at h_read
    result = Shard.read(shard_via, key, h_read)

    # 6. Assert: Read at 9 should resolve to value written at 7,
    #    despite the older write reservation still held at 5.
    assert result == {:ok, write_value}

    # 7. Verify the reservation at h_reserve is still held (for sanity)
    state = :sys.get_state(shard_pid)
    assert state.kv[key][h_reserve].write_reserved?

    :ok
  end

  @doc """
  I test writing to an initially empty shard, advancing the write watermark,
  and then performing reads both below and above the write height.
  """
  @spec test_write_then_reads_empty_start() :: :ok
  def test_write_then_reads_empty_start() do
    node_id = "test_shard_empty_start_rw_node"
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

    # 1. Reserve and write value at write_height
    assert :ok == Shard.reserve(shard_via, key, write_height, :write)
    assert :ok == Shard.write(shard_via, key, write_value, write_height)

    # 2. Advance write watermark past the write and reads
    send(shard_pid, {:write_watermark_advanced, key, wm_height})
    # Allow message processing
    Process.sleep(50)

    # 3. Read at height_absent (should be absent as latest < 5 is nothing)
    assert :ok == Shard.reserve(shard_via, key, read_height_absent, :read)
    assert Shard.read(shard_via, key, read_height_absent) == :absent

    # 4. Read at height_ok (should see write_value as latest < 15 is at 10)
    assert :ok == Shard.reserve(shard_via, key, read_height_ok, :read)
    assert Shard.read(shard_via, key, read_height_ok) == {:ok, write_value}

    :ok
  end

  @doc """
  I test the unreserve function by creating read reservations for key "a" and write reservations
  for key "b" at multiple heights, then unreserving at a specific height and verifying that only
  those reservations are released.
  """
  @spec test_unreserve() :: :ok
  def test_unreserve() do
    node_id = "test_shard_unreserve_node"
    shard_id = :test_shard_unreserve
    shard_via = Registry.via(node_id, Shard, shard_id)

    # Start the shard with initial values
    initial_kv = %{
      "a" => 10,
      "b" => 20
    }

    {:ok, shard_pid} =
      Shard.start_link(node_id: node_id, id: shard_id, initial_kv: initial_kv)

    # Create read reservations for key "a" at heights 1, 2, 3, 4, 5
    Enum.each(1..5, fn height ->
      assert :ok == Shard.reserve(shard_via, "a", height, :read)
    end)

    # Create write reservations for key "b" at heights 1, 2, 3, 4, 5
    Enum.each(1..5, fn height ->
      assert :ok == Shard.reserve(shard_via, "b", height, :write)
    end)

    # Verify that all reservations were made correctly
    state_before_unreserve = :sys.get_state(shard_pid)

    # Check "a" reservations (read)
    a_heights = state_before_unreserve.kv["a"]

    Enum.each(1..5, fn height ->
      assert Map.has_key?(a_heights, height)
      assert a_heights[height].read_reserved?
      assert !a_heights[height].write_reserved?
    end)

    # Check "b" reservations (write)
    b_heights = state_before_unreserve.kv["b"]

    Enum.each(1..5, fn height ->
      assert Map.has_key?(b_heights, height)
      assert !b_heights[height].read_reserved?
      assert b_heights[height].write_reserved?
    end)

    # Unreserve at height 3
    assert :ok == Shard.unreserve(shard_via, 3)

    # Give the unreserve message time to process
    Process.sleep(50)

    # Verify that only height 3 reservations were removed
    state_after_unreserve = :sys.get_state(shard_pid)

    # Check "a" reservations after unreserve
    a_heights_after = state_after_unreserve.kv["a"]

    # Height 3 should have read_reserved? = false
    assert Map.has_key?(a_heights_after, 3)
    assert !a_heights_after[3].read_reserved?

    # Other heights should still have read_reserved? = true
    Enum.each([1, 2, 4, 5], fn height ->
      assert Map.has_key?(a_heights_after, height)
      assert a_heights_after[height].read_reserved?
    end)

    # Check "b" reservations after unreserve
    b_heights_after = state_after_unreserve.kv["b"]

    # Height 3 should have write_reserved? = false
    assert Map.has_key?(b_heights_after, 3)
    assert !b_heights_after[3].write_reserved?

    # Other heights should still have write_reserved? = true
    Enum.each([1, 2, 4, 5], fn height ->
      assert Map.has_key?(b_heights_after, height)
      assert b_heights_after[height].write_reserved?
    end)

    :ok
  end

  @doc """
  I test that GC preserves the latest *committed* state below the read
  watermark, even if a later read reservation was acquired and released.
  """
  @spec test_gc_preserves_committed_state_before_watermark() :: :ok
  def test_gc_preserves_committed_state_before_watermark() do
    node_id = "gc_preserve_node"
    key = "gc_preserve_test"
    shard_id = String.to_atom(key)
    # Start with some initial state
    initial_kv = %{key => 0}

    {:ok, shard_pid} =
      Shard.start_link(node_id: node_id, id: shard_id, initial_kv: initial_kv)

    # 1. Write a value at height 3
    assert :ok == Shard.reserve(shard_pid, key, 3, :write)
    assert :ok == Shard.write(shard_pid, key, 100, 3)

    # 2. Reserve read at height 4
    assert :ok == Shard.reserve(shard_pid, key, 4, :read)

    # 3. Release reservation at height 4 (e.g., tx rollback)
    assert :ok == Shard.unreserve(shard_pid, 4)

    # 4. Advance read watermark past height 3 and 4, triggering GC
    send(shard_pid, {:read_watermark_advanced, key, 5})
    # Allow GC to run
    Process.sleep(50)

    # 5. Verify state
    state = :sys.get_state(shard_pid)
    key_height_map = Map.get(state.kv, key)

    # Check that the entry at height 3 (committed write) still exists
    assert %{value: 100, read_reserved?: false, write_reserved?: false} ==
             Map.get(key_height_map, 3),
           "Committed state at height 3 should be preserved by GC"

    # Check that the entry for height 4 (only reserved, then released) is gone or unreserved
    details_h4 = Map.get(key_height_map, 4)

    assert is_nil(details_h4),
           "State at height 4 should be GC'd"

    assert :ok == GenServer.stop(shard_pid)
    :ok
  end

  @doc """
  I test that unreserving a write reservation triggers the check for pending reads,
  allowing a previously blocked read (blocked by the reservation, not the watermark)
  to complete.
  """
  @spec test_unreserve_triggers_pending_read() :: :ok
  def test_unreserve_triggers_pending_read() do
    node_id = "test_shard_unreserve_trigger_node"
    shard_id = :test_shard_unreserve_trigger
    shard_via = Registry.via(node_id, Shard, shard_id)
    key = "a"

    h_write = 2
    write_value = 100
    h_blocking_reserve = 3
    h_read = 4
    wm_height = 5

    # Start the shard (empty initial state)
    {:ok, shard_pid} =
      Shard.start_link(node_id: node_id, id: shard_id, initial_kv: %{})

    # 1. Reserve and Write at h_write
    assert :ok == Shard.reserve(shard_via, key, h_write, :write)
    assert :ok == Shard.write(shard_via, key, write_value, h_write)

    # 2. Reserve Write at h_blocking_reserve (will block the read)
    assert :ok == Shard.reserve(shard_via, key, h_blocking_reserve, :write)

    # 3. Reserve Read at h_read
    assert :ok == Shard.reserve(shard_via, key, h_read, :read)

    # 4. Start Read Task (will block due to h_blocking_reserve)
    read_task =
      Task.async(fn ->
        Shard.read(shard_via, key, h_read)
      end)

    # 5. Advance Write Watermark (enough for h_read, but still blocked by reservation)
    send(shard_pid, {:write_watermark_advanced, key, wm_height})
    # Allow time for watermark message processing
    Process.sleep(50)

    # 6. Verify Read is Still Blocked (yield returns nil if task hasn't finished)
    assert Task.yield(read_task, 100) == nil,
           "Read task should still be blocked by the write reservation at height #{h_blocking_reserve}"

    # 7. Unreserve the Blocking Height
    assert :ok == Shard.unreserve(shard_via, h_blocking_reserve)

    # 8. Await Read Result (should now complete)
    result = Task.await(read_task, 1000)

    # 9. Assert the result is the value from h_write
    # Read at h_read(4) sees latest committed write < 4, which is at h_write(2)
    assert result == {:ok, write_value},
           "Read should have resolved to #{write_value} after unreserve"

    :ok
  end
end
