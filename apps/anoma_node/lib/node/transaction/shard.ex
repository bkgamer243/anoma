defmodule Anoma.Node.Transaction.Shard do
  @moduledoc """
  I am the Shard module.

  I manage a partition of the distributed key-value store, handling requests
  for locking, reading, and writing specific keys at specific heights.
  I maintain versioned state
  for read resolution and garbage collection based on dual watermarks.

  ### Public API

  I provide the following public functionality:

  - `start_link/1`
  - `lock/4`
  - `read/4`
  - `write/5`

  ### Key Concepts

  - **Height:** A transaction-specific identifier used for versioning.
  - **KV State:** A map storing key -> height -> entry_details.
  - **Locks:** Independent read and write locks associated with a `{key, height}` and unique references (`read_lock_ref`, `write_lock_ref`).
  - **Watermarks:** Per-key dual watermarks (`:read`, `:write`) control read resolution and GC.
  - **Synchronous Reads:** Read requests (`read/4`) block the caller until resolved. Resolution may be delayed internally if blocked by watermarks or preceding write locks. Read completion releases the specific read lock.

  """

  alias Anoma.Node.Registry

  require Logger

  use GenServer
  use TypedStruct

  ############################################################
  #                       Types                              #
  ############################################################

  @typedoc "The key in the key-value store."
  @type key :: binary()

  @typedoc "The height associated with an operation."
  # Allows -1 for initial state
  @type height :: integer()

  @typedoc "The value stored for a key at a height."
  @type value :: any()

  @typedoc "The capabilities requested or held by a lock."
  @type capabilities :: :read | :write | :read_write

  @typedoc "A unique reference identifying a lock instance."
  @type lock_ref :: reference()

  @typedoc "Stores the details for a specific {key, height}."
  @type kv_entry_details :: %{
          # The actual value, nil if not written yet
          value: value() | nil,
          # The ref if read-locked, nil otherwise
          read_lock_ref: reference() | nil,
          # The ref if write-locked, nil otherwise
          write_lock_ref: reference() | nil
        }

  ############################################################
  #                         State                            #
  ############################################################

  typedstruct enforce: true do
    @typedoc """
    I am the state of the Shard GenServer.

    ### Fields
    - `:id` - The identifier for this shard.
    - `:kv` - The core key-value store: `key => height => kv_entry_details`.
    - `:watermarks` - Per-key watermarks: `key => %{read: height, write: height}`.
    - `:pending_reads` - Reads waiting for watermark advancement: `key => height => GenServer.from()`.
    """
    field(:id, any())

    field(
      :kv,
      %{required(key()) => %{required(height()) => kv_entry_details()}},
      default: %{}
    )

    field(
      :watermarks,
      %{required(key()) => %{read: height(), write: height()}},
      default: %{}
    )

    field(
      :pending_reads,
      %{required(key()) => %{required(height()) => GenServer.from()}},
      default: %{}
    )
  end

  ############################################################
  #                    Public RPC API                      #
  ############################################################

  @doc """
  I am the start_link function for the Shard module.

  I start and link a Shard process, register it using the provided `id`,
  and initialize its KV state based on `initial_kv` options.
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    # id: shard_id, node_id: node_id, initial_kv: %{key => val}
    GenServer.start_link(__MODULE__, opts)
  end

  @doc """
  I am the lock function for the Shard module.

  I request a lock on a specific key at a given height.
  Capabilities can be `:read`, `:write`, or `:read_write`.
  I return `{:ok, %{read: read_ref | nil, write: write_ref | nil}}` containing
  the relevant lock references on success, or an error tuple.
  """
  @spec lock(GenServer.server(), key(), height(), capabilities()) ::
          {:ok, %{read: reference() | nil, write: reference() | nil}}
          | {:error,
             :locking_write_past_write_watermark
             | :locking_read_past_read_watermark
             | :slot_occupied_by_value}
  def lock(shard_pid, key, height, type) do
    # Todo: Timeout?
    GenServer.call(shard_pid, {:lock, key, height, type}, :infinity)
  end

  @doc """
  I am the read function for the Shard module.

  I perform a synchronous read request for a key at a specific height.
  I require the `read_ref` obtained from a prior `lock` call.
  The caller blocks until the read can be resolved (potentially waiting for watermarks)
  and receives the result directly.
  Returns `{:ok, value}`, `:absent`, or an error tuple.
  """
  @spec read(GenServer.server(), key(), height(), read_ref :: reference()) ::
          {:ok, value()}
          | :absent
          | {:error, :invalid_or_missing_lock_ref | :read_already_pending}
  def read(shard_pid, key, height, read_ref) do
    # Use call and wait for the actual result or error
    GenServer.call(shard_pid, {:read, key, height, read_ref}, :infinity)
  end

  @doc """
  I am the write function for the Shard module.

  I write a value for a key at a specific height, requiring a valid write lock reference
  obtained from a prior `lock` call.
  I return `:ok` on success, or an error tuple.
  """
  @spec write(
          GenServer.server(),
          key(),
          value(),
          height(),
          write_ref :: reference()
        ) ::
          :ok | {:error, :write_lock_required | :invalid_lock_ref}
  def write(shard_pid, key, value, height, write_ref) do
    # Using call to get confirmation/error back
    GenServer.call(
      shard_pid,
      {:write, key, value, height, write_ref},
      :infinity
    )
  end

  ############################################################
  #                    Genserver Helpers                     #
  ############################################################

  @impl true
  def init(opts) do
    Process.set_label(__MODULE__)
    id = Keyword.fetch!(opts, :id)
    node_id = Keyword.fetch!(opts, :node_id)
    initial_kv_arg = Keyword.get(opts, :initial_kv, %{})

    # Register the shard process
    case Registry.register(node_id, __MODULE__, id) do
      {:ok, _pid} ->
        Logger.debug(
          "Shard #{id} successfully registered for node #{node_id}"
        )

      {:error, reason} ->
        Logger.error(
          "Shard #{id} failed to register for node #{node_id}: #{inspect(reason)}"
        )
    end

    # Initialize KV with schema values at height -1 using the new structure
    kv =
      Enum.reduce(initial_kv_arg, %{}, fn {key, value}, acc ->
        # Initial state: has value, no locks
        initial_details = %{
          value: value,
          read_lock_ref: nil,
          write_lock_ref: nil
        }

        Map.put(acc, key, %{-1 => initial_details})
      end)

    # Initialize watermarks for keys present in initial_kv
    watermarks =
      Enum.reduce(initial_kv_arg, %{}, fn {key, _}, acc ->
        Map.put(acc, key, %{read: -1, write: -1})
      end)

    state = %__MODULE__{
      id: id,
      kv: kv,
      watermarks: watermarks,
      pending_reads: %{}
    }

    {:ok, state}
  end

  ############################################################
  #                   Genserver Callbacks                    #
  ############################################################

  # --- Lock Handling ---
  @impl true
  def handle_call({:lock, key, height, type}, _from, state) do
    key_watermarks = Map.get(state.watermarks, key, %{read: -1, write: -1})

    # Check against watermarks based on requested lock type
    cond do
      # Cannot acquire WRITE lock at or below the WRITE watermark
      type in [:write, :read_write] and height <= key_watermarks.write ->
        {:reply, {:error, :locking_write_past_write_watermark}, state}

      # Cannot acquire READ lock at or below the READ watermark
      type in [:read, :read_write] and height <= key_watermarks.read ->
        {:reply, {:error, :locking_read_past_read_watermark}, state}

      # Height is valid relative to relevant watermarks, proceed
      true ->
        key_height_map = Map.get(state.kv, key, %{})
        # Get current details or default to empty
        details =
          Map.get(key_height_map, height, %{
            value: nil,
            read_lock_ref: nil,
            write_lock_ref: nil
          })

        # Process Read Lock Request
        {details_after_read, new_read_ref} =
          if type in [:read, :read_write] and is_nil(details.read_lock_ref) do
            ref = make_ref()
            {%{details | read_lock_ref: ref}, ref}
          else
            # Either not requested, or already locked
            {details, details.read_lock_ref}
          end

        # Process Write Lock Request
        {details_after_write, new_write_ref, error} =
          cond do
            type not in [:write, :read_write] ->
              # Write not requested
              {details_after_read, details_after_read.write_lock_ref, nil}

            not is_nil(details_after_read.value) ->
              # Cannot acquire write lock if value already exists
              {details_after_read, details_after_read.write_lock_ref,
               :slot_occupied_by_value}

            is_nil(details_after_read.write_lock_ref) ->
              # Acquire new write lock
              ref = make_ref()
              {%{details_after_read | write_lock_ref: ref}, ref, nil}

            true ->
              # Write lock already exists
              {details_after_read, details_after_read.write_lock_ref, nil}
          end

        if error do
          # Primarily handles :slot_occupied_by_value for write attempts
          {:reply, {:error, error}, state}
        else
          # Update state only if changes occurred
          final_details = details_after_write

          if final_details != details do
            new_key_height_map =
              Map.put(key_height_map, height, final_details)

            new_kv = Map.put(state.kv, key, new_key_height_map)
            new_state = %{state | kv: new_kv}

            {:reply, {:ok, %{read: new_read_ref, write: new_write_ref}},
             new_state}
          else
            # No change in lock status (e.g., locks already held)
            {:reply, {:ok, %{read: new_read_ref, write: new_write_ref}},
             state}
          end
        end
    end
  end

  # --- Write Handling ---
  @impl true
  def handle_call({:write, key, value, height, write_ref}, _from, state) do
    key_height_map = Map.get(state.kv, key, %{})

    details =
      Map.get(key_height_map, height, %{
        value: nil,
        read_lock_ref: nil,
        write_lock_ref: nil
      })

    cond do
      is_nil(details.write_lock_ref) ->
        {:reply, {:error, :write_lock_required}, state}

      details.write_lock_ref != write_ref ->
        {:reply, {:error, :invalid_lock_ref}, state}

      true ->
        # Valid write lock ref
        # Update value, clear write lock ref, KEEP read lock ref
        updated_details = %{details | value: value, write_lock_ref: nil}
        new_key_height_map = Map.put(key_height_map, height, updated_details)
        new_kv = Map.put(state.kv, key, new_key_height_map)
        new_state = %{state | kv: new_kv}

        # Check pending reads *after* state update (write might allow resolution if watermark matches)
        final_state = check_pending_reads_for_write(key, height, new_state)
        {:reply, :ok, final_state}
    end
  end

  # --- Read Handling ---
  @impl true
  def handle_call({:read, key, height_req, read_ref}, from, state) do
    # --- Validation ---
    key_height_map = Map.get(state.kv, key, %{})

    details_at_req =
      Map.get(key_height_map, height_req, %{
        value: nil,
        read_lock_ref: nil,
        write_lock_ref: nil
      })

    cond do
      # 1. Invalid Lock Ref
      is_nil(details_at_req.read_lock_ref) or
          details_at_req.read_lock_ref != read_ref ->
        {:reply, {:error, :invalid_or_missing_lock_ref}, state}

      # 2. Read Already Pending
      !is_nil(get_in(state.pending_reads, [key, height_req])) ->
        {:reply, {:error, :read_already_pending}, state}

      # 3. Attempt Resolution
      true ->
        key_watermarks =
          Map.get(state.watermarks, key, %{read: -1, write: -1})

        resolution_result =
          resolve_read_value(height_req, key_height_map, key_watermarks)

        case resolution_result do
          # Includes {:ok, :absent} or {:ok, {:ok, val}}
          {:ok, value_or_absent} ->
            # Resolve succeeded, release lock and reply
            new_state =
              if details_at_req.read_lock_ref == read_ref do
                updated_details = %{details_at_req | read_lock_ref: nil}

                new_key_height_map =
                  Map.put(key_height_map, height_req, updated_details)

                new_kv = Map.put(state.kv, key, new_key_height_map)
                %{state | kv: new_kv}
              else
                # Should ideally not happen due to check 1, but log if it does
                Logger.warning(
                  "Shard #{inspect(state.id)}: Read resolved for key #{inspect(key)}, height #{height_req}, but read_ref #{inspect(read_ref)} did not match stored ref #{inspect(details_at_req.read_lock_ref)} during release."
                )

                state
              end

            # Map internal {:ok, :absent} to just :absent for the caller
            {:reply, value_or_absent, new_state}

          block_reason
          when block_reason in [:blocked_by_watermark, :blocked_by_write_lock] ->
            # Queue the read
            pending_for_key = Map.get(state.pending_reads, key, %{})

            updated_pending_for_key =
              Map.put(pending_for_key, height_req, from)

            new_pending_reads =
              Map.put(state.pending_reads, key, updated_pending_for_key)

            {:noreply, %{state | pending_reads: new_pending_reads}}
        end
    end
  end

  # --- Watermark Update Handling ---

  # Handle Write Watermark Advancement Message
  @impl true
  def handle_info({:write_watermark_advanced, key, h_write}, state) do
    current_key_watermarks =
      Map.get(state.watermarks, key, %{read: -1, write: -1})

    # Write watermark should only advance
    new_write_wm = max(h_write, current_key_watermarks.write)

    if new_write_wm > current_key_watermarks.write do
      updated_watermarks = %{current_key_watermarks | write: new_write_wm}
      new_watermarks_map = Map.put(state.watermarks, key, updated_watermarks)
      state_after_wm_update = %{state | watermarks: new_watermarks_map}

      # Check pending reads based ONLY on the new write watermark
      state_after_reads =
        check_pending_reads_for_watermark(key, state_after_wm_update)

      {:noreply, state_after_reads}
    else
      # Watermark did not advance for this key
      {:noreply, state}
    end
  end

  # Handle Read Watermark Advancement
  @impl true
  def handle_info({:read_watermark_advanced, key, h_read}, state) do
    current_key_watermarks =
      Map.get(state.watermarks, key, %{read: -1, write: -1})

    # Read watermark should only advance
    new_read_wm = max(h_read, current_key_watermarks.read)

    if new_read_wm > current_key_watermarks.read do
      updated_watermarks = %{current_key_watermarks | read: new_read_wm}
      new_watermarks_map = Map.put(state.watermarks, key, updated_watermarks)
      state_after_wm_update = %{state | watermarks: new_watermarks_map}

      # Perform Garbage Collection based ONLY on the new read watermark
      state_after_gc = gc_key(key, new_read_wm, state_after_wm_update)

      {:noreply, state_after_gc}
    else
      # Watermark did not advance for this key
      {:noreply, state}
    end
  end

  # Catch-all for other info messages
  @impl true
  def handle_info(msg, state) do
    Logger.debug(
      "Shard #{inspect(state.id)} received unhandled info: #{inspect(msg)}"
    )

    {:noreply, state}
  end

  ############################################################
  #                 Internal Helper Functions                #
  ############################################################

  # I am the helper function to check pending reads after a write.

  # I check if reads pending for a specific key might be resolvable after a write has
  # occurred. Currently, I delegate directly to `check_pending_reads_for_watermark`
  # as watermark advancement is the primary trigger for resolving pending reads.
  @spec check_pending_reads_for_write(key(), height(), __MODULE__.t()) ::
          __MODULE__.t()
  defp check_pending_reads_for_write(key, _write_height, state) do
    # A write might make a *newer* read resolvable if the watermark allows it.
    # The main check is handled by check_pending_reads_for_watermark.
    check_pending_reads_for_watermark(key, state)
  end

  # I am the helper function to check pending reads after a watermark update.

  # I check all pending reads for a given key after a watermark update.
  # If a read becomes resolvable, I calculate the result, reply directly to the waiting
  # caller using `GenServer.reply/2`, release the corresponding read lock, and
  # remove the request from the pending map.
  @spec check_pending_reads_for_watermark(key(), __MODULE__.t()) ::
          __MODULE__.t()
  defp check_pending_reads_for_watermark(key, state) do
    # Now: %{height => from}
    pending_for_key = Map.get(state.pending_reads, key, %{})
    key_watermarks = Map.get(state.watermarks, key, %{read: -1, write: -1})

    # Iterate through pending heights {height_req => from}
    {new_pending_for_key, updated_state} =
      Enum.reduce(pending_for_key, {%{}, state}, fn {height_req, from},
                                                    {acc_pending_map,
                                                     acc_state} ->
        # Re-fetch key_height_map inside reduce as it might change due to lock release
        current_key_height_map = Map.get(acc_state.kv, key, %{})

        resolution_result =
          resolve_read_value(
            height_req,
            current_key_height_map,
            key_watermarks
          )

        case resolution_result do
          # Includes :absent or {:ok, val}
          {:ok, value_or_absent} ->
            # Resolve succeeded

            # Reply directly to the original caller
            GenServer.reply(from, value_or_absent)

            # --- Release Read Lock ---
            details_at_req_height =
              Map.get(current_key_height_map, height_req, %{
                value: nil,
                read_lock_ref: nil,
                write_lock_ref: nil
              })

            state_after_lock_release =
              if !is_nil(details_at_req_height.read_lock_ref) do
                updated_details = %{
                  details_at_req_height
                  | read_lock_ref: nil
                }

                new_key_height_map =
                  Map.put(current_key_height_map, height_req, updated_details)

                new_kv = Map.put(acc_state.kv, key, new_key_height_map)
                %{acc_state | kv: new_kv}
              else
                # Should not happen if logic is correct, but log if it does
                Logger.warning(
                  "Shard #{inspect(acc_state.id)}: Resolved PENDING read for key #{inspect(key)}, height #{height_req}, but read lock was already nil when releasing."
                )

                acc_state
              end

            # Don't add this height back to accumulator, effectively removing it from pending
            {acc_pending_map, state_after_lock_release}

          block_reason
          when block_reason in [:blocked_by_watermark, :blocked_by_write_lock] ->
            # Still blocked, keep pending
            {Map.put(acc_pending_map, height_req, from), acc_state}
        end

        # end case resolution_result
      end)

    # end Enum.reduce

    # Update the state's pending reads map for the key
    new_pending_reads =
      if map_size(new_pending_for_key) > 0 do
        Map.put(updated_state.pending_reads, key, new_pending_for_key)
      else
        # Clean up if no reads left for this key
        Map.delete(updated_state.pending_reads, key)
      end

    %{updated_state | pending_reads: new_pending_reads}
  end

  # I am the garbage collection helper function.

  # I perform garbage collection for a specific key based on the read watermark.
  # I remove entries with height < read_watermark, preserving the latest entry
  # at or below the watermark, and any entries needed to resolve active read locks.
  @spec gc_key(key(), height(), __MODULE__.t()) :: __MODULE__.t()
  defp gc_key(key, read_watermark, state) do
    case Map.get(state.kv, key) do
      nil ->
        # Key not present, nothing to GC
        state

      key_height_map ->
        # 1. Find latest height <= watermark
        maybe_max_h_le_wm =
          key_height_map
          |> Enum.filter(fn {h, _} -> h <= read_watermark end)
          # Returns {h, details} or nil
          |> Enum.max_by(fn {h, _} -> h end, fn -> nil end)

        # Start with the height of the latest entry <= watermark (if any)
        heights_to_keep =
          case maybe_max_h_le_wm do
            {h, _} -> MapSet.new([h])
            nil -> MapSet.new()
          end

        # 2. Find heights needed to support active read locks
        read_lock_heights =
          for {h, details} <- key_height_map,
              not is_nil(details.read_lock_ref),
              do: h

        # Convert to set for efficient union later
        read_lock_heights_set = MapSet.new(read_lock_heights)

        supporting_heights =
          for h_rl <- read_lock_heights do
            # Find greatest height < h_rl
            maybe_max_h_lt_rl =
              key_height_map
              |> Enum.filter(fn {h, _} -> h < h_rl end)
              |> Enum.max_by(fn {h, _} -> h end, fn -> nil end)

            case maybe_max_h_lt_rl do
              # Just need the height
              {h, _} -> h
              nil -> nil
            end
          end
          # Filter out cases where no lower height exists
          |> Enum.reject(&is_nil(&1))
          |> MapSet.new()

        # 3. Combine all essential heights: latest <= WM, supporting heights, and heights with locks
        all_heights_to_keep =
          heights_to_keep
          |> MapSet.union(supporting_heights)
          # Add heights holding the locks
          |> MapSet.union(read_lock_heights_set)

        # 4. Filter the map: Keep entries > watermark OR in the essential set
        new_key_height_map =
          Enum.filter(key_height_map, fn {h, _details} ->
            h > read_watermark or MapSet.member?(all_heights_to_keep, h)
          end)
          |> Map.new()

        # 5. Update state
        if map_size(new_key_height_map) > 0 do
          new_kv = Map.put(state.kv, key, new_key_height_map)
          %{state | kv: new_kv}
        else
          # If GC removed all entries for the key, remove the key itself
          # Optional: Consider also removing from state.watermarks here if appropriate
          new_kv = Map.delete(state.kv, key)
          %{state | kv: new_kv}
        end
    end
  end

  # I am the helper function to attempt resolving a read request.

  # I check if a read for `key` at `height_req` can be resolved based on the
  # current `key_height_map` and `key_watermarks`.
  # I return:
  # - `{:ok, :absent}` if resolvable and no value exists below `height_req`.
  # - `{:ok, {:ok, value}}` if resolvable and a value exists.
  # - `:blocked_by_watermark` if `height_req` is above the write watermark.
  # - `:blocked_by_write_lock` if the latest entry below `height_req` holds a write lock.
  @spec resolve_read_value(height(), map(), map()) ::
          {:ok, :absent | {:ok, value()}}
          | :blocked_by_watermark
          | :blocked_by_write_lock
  defp resolve_read_value(height_req, key_height_map, key_watermarks) do
    cond do
      # 1. Check Watermark (Unchanged)
      height_req > key_watermarks.write ->
        :blocked_by_watermark

      true ->
        # 2. Find the latest entry below height_req that has EITHER a value OR a write lock.
        # This represents the most recent operation determining the state relevant to the read.
        maybe_relevant_entry =
          key_height_map
          |> Enum.filter(fn {h, details} ->
            h < height_req and
              (not is_nil(details.value) or not is_nil(details.write_lock_ref))
          end)
          |> Enum.max_by(fn {h, _details} -> h end, fn -> nil end)

        case maybe_relevant_entry do
          # 3. No relevant entry found below height_req (implies initial state or empty)
          nil ->
            # If no entry with a value or lock exists below height_req, the result is absent.
            {:ok, :absent}

          # 4. Relevant entry found, check its state
          {_h, details} ->
            cond do
              # If the latest relevant entry has a value (is committed), resolve the read.
              not is_nil(details.value) ->
                {:ok, {:ok, details.value}}

              # If the latest relevant entry holds a write lock, block the read.
              not is_nil(details.write_lock_ref) ->
                :blocked_by_write_lock

              # Should be unreachable.
              true ->
                Logger.error(
                  "Shard: Unreachable state in resolve_read_value for key height map: #{inspect(key_height_map)}, height_req: #{height_req}"
                )

                # Treat as absent if we somehow reach here
                {:ok, :absent}
            end
        end
    end
  end
end
