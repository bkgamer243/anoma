defmodule Anoma.Node.Transaction.Shard do
  @moduledoc """
  I am the Shard module.

  I manage a partition of the distributed key-value store, handling requests
  for reserving slots, reading, and writing specific keys at specific heights.
  I maintain versioned state
  for read resolution and garbage collection based on dual watermarks.

  ### Public API

  I provide the following public functionality:

  - `start_link/1`
  - `reserve/4`
  - `read/3`
  - `write/4`
  - `unreserve/5`

  ### Key Concepts

  - **Height:** A transaction-specific identifier used for versioning.
  - **KV State:** A map storing key -> height -> entry_details.
  - **Reservations:** Independent read and write reservations associated with a `{key, height}`.
  - **Watermarks:** Per-key dual watermarks (`:read`, `:write`) control GC and read resolution respectively.
  - **Synchronous Reads:** Read requests (`read/3`) block the caller until resolved. Resolution may be delayed internally if blocked by watermarks or preceding write reservations. Read completion releases the specific read reservation.
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

  @typedoc "The capabilities requested or held by a reservation."
  @type capabilities :: :read | :write | :read_write

  @typedoc "Stores the details for a specific {key, height}."
  @type kv_entry_details :: %{
          # The actual value, nil if not written yet
          value: value() | nil,
          # True if read-reserved, false otherwise
          read_reserved?: boolean(),
          # True if write-reserved, false otherwise
          write_reserved?: boolean()
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
    - `:pending_reads` - Reads blocked by a watermark or write reservation: `key => height => GenServer.from()`.
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
  I am the reserve function for the Shard module. Use me to reserve a
  read or write at a specific key at a specific height.

  Reservations exist to inform the KV store that a value will
  be read or written at a specific height at some point in the future.
  If I know that an empty entry will be written to, then an immediate read
  will have to wait until the write occurs. If I know that some entry will
  be read from, then I know I must keep around immediately preceding committed values at
  least until the read is completed.

  I request a reservation on a specific key at a given height.
  Capabilities can be `:read`, `:write`, or `:read_write`.
  I return `:ok` on success, or an error tuple.
  """
  @spec reserve(GenServer.server(), key(), height(), capabilities()) ::
          :ok
          | {:error,
             :reserving_write_under_write_watermark
             | :reserving_read_under_read_watermark
             | :slot_occupied_by_value}
  def reserve(shard_pid, key, height, type) do
    # Todo: Timeout?
    GenServer.call(shard_pid, {:reserve, key, height, type}, :infinity)
  end

  @doc """
  I am the read function for the Shard module.

  I perform a synchronous read request for a key at a specific height.
  I require a prior `reserve` call with `:read` or `:read_write` capability for this `{key, height}`.
  The caller blocks until the read can be resolved (potentially waiting for watermarks)
  and receives the result directly.
  Returns `{:ok, value}`, `:absent`, or an error tuple.
  """
  @spec read(GenServer.server(), key(), height()) ::
          {:ok, value()}
          | :absent
          | {:error, :read_not_reserved | :read_already_pending}
  def read(shard_pid, key, height) do
    GenServer.call(shard_pid, {:read, key, height}, :infinity)
  end

  @doc """
  I am the write function for the Shard module.

  I write a value for a key at a specific height, requiring a prior `reserve` call
  with `:write` or `:read_write` capability for this `{key, height}`.
  I return `:ok` on success, or an error tuple.
  """
  @spec write(
          GenServer.server(),
          key(),
          value(),
          height()
        ) ::
          :ok | {:error, :write_reservation_required}
  def write(shard_pid, key, value, height) do
    GenServer.call(
      shard_pid,
      {:write, key, value, height},
      :infinity
    )
  end

  @doc """
  I release all reservations for all keys at a given height.

  This is an asynchronous operation used for rollbacks of failed transactions.
  """
  @spec unreserve(GenServer.server(), height()) :: :ok
  def unreserve(shard_pid, height) do
    GenServer.cast(shard_pid, {:unreserve, height})
  end

  ############################################################
  #                    Genserver Callbacks                    #
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

    # Initialize KV with schema values at height -1
    kv =
      Enum.reduce(initial_kv_arg, %{}, fn {key, value}, acc ->
        # Initial state: has value, no reservations
        initial_details = %{
          value: value,
          read_reserved?: false,
          write_reserved?: false
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

  # --- Reservation Handling ---
  @impl true
  def handle_call({:reserve, key, height, type}, _from, state) do
    handle_reserve(key, height, type, state)
  end

  # --- Write Handling ---
  @impl true
  def handle_call({:write, key, value, height}, _from, state) do
    handle_write(key, value, height, state)
  end

  # --- Read Handling ---
  @impl true
  def handle_call({:read, key, height_req}, from, state) do
    handle_read(key, height_req, from, state)
  end

  # --- Watermark Update Handling ---
  @impl true
  def handle_info({:write_watermark_advanced, key, h_write}, state) do
    handle_write_watermark_advanced(key, h_write, state)
  end

  @impl true
  def handle_info({:read_watermark_advanced, key, h_read}, state) do
    handle_read_watermark_advanced(key, h_read, state)
  end

  # --- Unreserve Handling ---
  @impl true
  def handle_cast({:unreserve, height}, state) do
    handle_unreserve(height, state)
  end

  ############################################################
  #             Internal Callback Handler Functions          #
  ############################################################

  # Handles the `:reserve` GenServer call.

  # Orchestrates the reservation process using helper functions and a `with` statement.
  # 1. Checks watermarks.
  # 2. Retrieves or initializes details for the {key, height}.
  # 3. Processes the specific reservation request (:read, :write, or :read_write).
  # 4. Updates the state if the reservation was successful and changed the details.
  @spec handle_reserve(key(), height(), capabilities(), __MODULE__.t()) ::
          {:reply, :ok | {:error, atom()}, __MODULE__.t()}
  defp handle_reserve(key, height, type, state) do
    key_watermarks = Map.get(state.watermarks, key, %{read: -1, write: -1})

    with :ok <- check_watermarks(height, type, key_watermarks),
         original_details = get_or_initialize_details(state.kv, key, height),
         {:ok, final_details} <-
           process_reservation_request(type, original_details) do
      # Update state only if changes occurred
      if final_details != original_details do
        key_height_map = Map.get(state.kv, key, %{})
        new_key_height_map = Map.put(key_height_map, height, final_details)
        new_kv = Map.put(state.kv, key, new_key_height_map)
        new_state = %{state | kv: new_kv}
        {:reply, :ok, new_state}
      else
        # No change in reservation status (e.g., reservations already held)
        {:reply, :ok, state}
      end
    else
      # Handle errors from check_watermarks or process_reservation_request
      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  # Checks if a reservation request conflicts with existing watermarks.
  @spec check_watermarks(height(), capabilities(), map()) ::
          :ok | {:error, atom()}
  defp check_watermarks(height, type, key_watermarks)
       when type in [:write, :read_write] and height <= key_watermarks.write do
    {:error, :reserving_write_under_write_watermark}
  end

  defp check_watermarks(height, type, key_watermarks)
       when type in [:read, :read_write] and height <= key_watermarks.read do
    {:error, :reserving_read_under_read_watermark}
  end

  defp check_watermarks(_height, _type, _key_watermarks), do: :ok

  # Retrieves the kv_entry_details for a {key, height} or returns initial default details.
  @spec get_or_initialize_details(map(), key(), height()) ::
          kv_entry_details()
  defp get_or_initialize_details(kv, key, height) do
    kv
    |> Map.get(key, %{})
    |> Map.get(height, %{
      value: nil,
      read_reserved?: false,
      write_reserved?: false
    })
  end

  # Processes a reservation request based on the type and current details.

  # Handles granting read/write reservations and checks for conflicts like existing values
  # when attempting a write reservation. Uses function heads for clarity.
  @spec process_reservation_request(capabilities(), kv_entry_details()) ::
          {:ok, kv_entry_details()} | {:error, atom()}

  defp process_reservation_request(:read, details) do
    {:ok, %{details | read_reserved?: true}}
  end

  defp process_reservation_request(:write, %{value: value})
       when not is_nil(value) do
    {:error, :slot_occupied_by_value}
  end

  defp process_reservation_request(:write, details) do
    {:ok, %{details | write_reserved?: true}}
  end

  defp process_reservation_request(:read_write, %{value: value})
       when not is_nil(value) do
    {:error, :slot_occupied_by_value}
  end

  defp process_reservation_request(:read_write, details) do
    {:ok, %{details | read_reserved?: true, write_reserved?: true}}
  end

  @spec handle_write(key(), value(), height(), __MODULE__.t()) ::
          {:reply, :ok | {:error, atom()}, __MODULE__.t()}
  defp handle_write(key, value, height, state) do
    key_height_map = Map.get(state.kv, key, %{})

    details =
      Map.get(key_height_map, height, %{
        value: nil,
        read_reserved?: false,
        write_reserved?: false
      })

    cond do
      !details.write_reserved? ->
        {:reply, {:error, :write_reservation_required}, state}

      true ->
        # Valid write reservation
        # Update value, clear write reservation, KEEP read reservation
        updated_details = %{details | value: value, write_reserved?: false}
        new_key_height_map = Map.put(key_height_map, height, updated_details)
        new_kv = Map.put(state.kv, key, new_key_height_map)
        new_state = %{state | kv: new_kv}

        # Check pending reads *after* state update (write might allow resolution if watermark matches)
        final_state = check_pending_reads(key, new_state)
        {:reply, :ok, final_state}
    end
  end

  @spec handle_read(key(), height(), GenServer.from(), __MODULE__.t()) ::
          {:reply, {:ok, value()} | :absent | {:error, atom()},
           __MODULE__.t()}
          | {:noreply, __MODULE__.t()}
  defp handle_read(key, height_req, from, state) do
    # --- Validation ---
    key_height_map = Map.get(state.kv, key, %{})

    details_at_req =
      Map.get(key_height_map, height_req, %{
        value: nil,
        read_reserved?: false,
        write_reserved?: false
      })

    cond do
      # 1. Check Read Reservation
      !details_at_req.read_reserved? ->
        {:reply, {:error, :read_not_reserved}, state}

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
            # Resolve succeeded, release reservation and reply
            new_state =
              if details_at_req.read_reserved? do
                updated_details = %{details_at_req | read_reserved?: false}

                new_key_height_map =
                  Map.put(key_height_map, height_req, updated_details)

                new_kv = Map.put(state.kv, key, new_key_height_map)
                %{state | kv: new_kv}
              else
                # Should ideally not happen due to check 1, but log if it does
                Logger.warning(
                  "Shard #{inspect(state.id)}: Read resolved for key #{inspect(key)}, height #{height_req}, but read reservation was already false during release."
                )

                state
              end

            # Map internal {:ok, :absent} to just :absent for the caller
            {:reply, value_or_absent, new_state}

          block_reason
          when block_reason in [
                 :blocked_by_watermark,
                 :blocked_by_write_reservation
               ] ->
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

  @spec handle_write_watermark_advanced(key(), height(), __MODULE__.t()) ::
          {:noreply, __MODULE__.t()}
  defp handle_write_watermark_advanced(key, h_write, state) do
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
        check_pending_reads(key, state_after_wm_update)

      {:noreply, state_after_reads}
    else
      # Watermark did not advance for this key
      {:noreply, state}
    end
  end

  @spec handle_read_watermark_advanced(key(), height(), __MODULE__.t()) ::
          {:noreply, __MODULE__.t()}
  defp handle_read_watermark_advanced(key, h_read, state) do
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

  @spec handle_unreserve(height(), __MODULE__.t()) ::
          {:noreply, __MODULE__.t()}
  defp handle_unreserve(height, state) do
    # Iterate through all keys in the KV store, accumulating the state
    final_state =
      Enum.reduce(state.kv, state, fn {key, key_height_map}, acc_state ->
        # Check if this key has an entry at the target height
        case Map.get(key_height_map, height) do
          nil ->
            # No entry at this height, skip this key, state remains unchanged for this iteration
            acc_state

          details ->
            # Release both read and write reservations
            updated_details = %{
              details
              | read_reserved?: false,
                write_reserved?: false
            }

            if updated_details != details do
              # Only update if a change actually occurred
              new_key_height_map =
                Map.put(key_height_map, height, updated_details)

              # Update the kv map within the accumulated state
              updated_kv = Map.put(acc_state.kv, key, new_key_height_map)
              state_after_kv_update = %{acc_state | kv: updated_kv}

              # Check pending reads for this key
              # The state might be further updated if reads are resolved
              check_pending_reads(key, state_after_kv_update)
            else
              # No change
              acc_state
            end
        end
      end)

    {:noreply, final_state}
  end

  ############################################################
  #                 Internal Helper Functions                #
  ############################################################

  # I am the helper function to check pending reads after a watermark update, write, or unreserve.

  # I check all pending reads for a given key.
  # If a read becomes resolvable, I calculate the result, reply directly to the waiting
  # caller using `GenServer.reply/2`, release the corresponding read reservation, and
  # remove the request from the pending map.
  @spec check_pending_reads(key(), __MODULE__.t()) :: __MODULE__.t()
  defp check_pending_reads(key, state) do
    pending_for_key = Map.get(state.pending_reads, key, %{})
    key_watermarks = Map.get(state.watermarks, key, %{read: -1, write: -1})

    # Iterate through pending heights {height_req => from}
    {new_pending_for_key, updated_state} =
      Enum.reduce(pending_for_key, {%{}, state}, fn {height_req, from},
                                                    {acc_pending_map,
                                                     acc_state} ->
        # Re-fetch key_height_map inside reduce as it might change due to reservation release
        current_key_height_map = Map.get(acc_state.kv, key, %{})

        resolution_result =
          resolve_read_value(
            height_req,
            current_key_height_map,
            key_watermarks
          )

        case resolution_result do
          {:ok, value_or_absent} ->
            # Reply directly to the original caller
            GenServer.reply(from, value_or_absent)

            # Release Read Reservation
            details_at_req_height =
              Map.get(current_key_height_map, height_req, %{
                value: nil,
                read_reserved?: false,
                write_reserved?: false
              })

            state_after_reservation_release =
              if details_at_req_height.read_reserved? do
                updated_details = %{
                  details_at_req_height
                  | read_reserved?: false
                }

                new_key_height_map =
                  Map.put(current_key_height_map, height_req, updated_details)

                new_kv = Map.put(acc_state.kv, key, new_key_height_map)
                %{acc_state | kv: new_kv}
              else
                # Should not happen if logic is correct, but log if it does
                Logger.warning(
                  "Shard #{inspect(acc_state.id)}: Resolved PENDING read for key #{inspect(key)}, height #{height_req}, but read reservation was already false when releasing."
                )

                acc_state
              end

            # Don't add this height back to accumulator
            {acc_pending_map, state_after_reservation_release}

          block_reason
          when block_reason in [
                 :blocked_by_watermark,
                 :blocked_by_write_reservation
               ] ->
            # Still blocked, keep pending
            {Map.put(acc_pending_map, height_req, from), acc_state}
        end

        # end case resolution_result
      end)

    # end Enum.reduce

    new_pending_reads =
      if map_size(new_pending_for_key) > 0 do
        Map.put(updated_state.pending_reads, key, new_pending_for_key)
      else
        # Clean up if no reads left for this key
        Map.delete(updated_state.pending_reads, key)
      end

    %{updated_state | pending_reads: new_pending_reads}
  end

  # Finds essential heights to keep below a given target height.
  # Returns a MapSet containing:
  # - The height of the latest committed value strictly below target_height.
  # - The heights of all write reservations between that value and target_height.
  @spec find_essential_heights_below(height(), map()) :: MapSet.t(height())
  defp find_essential_heights_below(target_height, key_height_map) do
    # 1. Find the highest height h_val < target_height with a committed value
    maybe_max_h_val =
      key_height_map
      |> Enum.filter(fn {h, details} ->
        h < target_height and not is_nil(details.value)
      end)
      |> Enum.max_by(fn {h, _} -> h end, fn -> nil end)

    case maybe_max_h_val do
      nil ->
        # No committed value below target_height. Find write reservations below target_height.
        write_reservation_heights_below =
          key_height_map
          |> Enum.filter(fn {h, details} ->
            h < target_height and details.write_reserved?
          end)
          # Keep only the heights
          |> Enum.map(fn {h, _} -> h end)

        MapSet.new(write_reservation_heights_below)

      {h_val, _details} ->
        # 2. Find all heights h_wr with write reservations between h_val and target_height
        write_reservation_heights_between =
          key_height_map
          |> Enum.filter(fn {h, details} ->
            h > h_val and h < target_height and details.write_reserved?
          end)
          # Keep only the heights
          |> Enum.map(fn {h, _} -> h end)

        # 3. Combine h_val and the intermediate write reservation heights
        MapSet.new([h_val | write_reservation_heights_between])
    end
  end

  # I am the garbage collection helper function.

  # I perform garbage collection for a specific key based on the read watermark.
  # I remove entries older than the watermark unless they are essential for resolving
  # reads at or past the watermark height or at heights with active read reservations.
  @spec gc_key(key(), height(), __MODULE__.t()) :: __MODULE__.t()
  defp gc_key(key, read_watermark, state) do
    case Map.get(state.kv, key) do
      nil ->
        # Key not present, nothing to GC
        state

      key_height_map ->
        # 1. Identify heights with active read reservations
        read_reservation_heights =
          for {h, details} <- key_height_map, details.read_reserved?, do: h

        read_reservation_heights_set = MapSet.new(read_reservation_heights)

        # 2. Determine essential heights to keep below the read watermark
        essential_below_watermark =
          find_essential_heights_below(read_watermark, key_height_map)

        # 3. Determine essential heights to keep below each active read reservation
        essential_below_reservations =
          read_reservation_heights
          |> Enum.map(&find_essential_heights_below(&1, key_height_map))
          |> Enum.reduce(MapSet.new(), fn set1, set2 ->
            MapSet.union(set1, set2)
          end)

        # 4. Combine all heights that MUST be kept:
        #    - Heights holding read reservations themselves.
        #    - Essential heights supporting the watermark.
        #    - Essential heights supporting each reservation.
        all_essential_heights_below_watermark =
          read_reservation_heights_set
          |> MapSet.union(essential_below_watermark)
          |> MapSet.union(essential_below_reservations)

        # 5. Filter the map: Keep entries >= watermark OR in the essential set below watermark
        new_key_height_map =
          Enum.filter(key_height_map, fn {h, _details} ->
            # Keep if at or above watermark OR essential below
            h >= read_watermark or
              MapSet.member?(all_essential_heights_below_watermark, h)
          end)
          |> Map.new()

        # 6. Update state
        if map_size(new_key_height_map) > 0 do
          new_kv = Map.put(state.kv, key, new_key_height_map)
          %{state | kv: new_kv}
        else
          # If GC removed all entries for the key, remove the key itself
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
  # - `:blocked_by_write_reservation` if the latest entry below `height_req` holds a write reservation.
  @spec resolve_read_value(height(), map(), map()) ::
          {:ok, :absent | {:ok, value()}}
          | :blocked_by_watermark
          | :blocked_by_write_reservation
  defp resolve_read_value(height_req, key_height_map, key_watermarks) do
    cond do
      # 1. Check Watermark
      height_req > key_watermarks.write ->
        :blocked_by_watermark

      true ->
        # 2. Find the latest entry below height_req that has EITHER a value OR a write reservation.
        # This represents the most recent operation determining the state relevant to the read.
        maybe_relevant_entry =
          key_height_map
          |> Enum.filter(fn {h, details} ->
            h < height_req and
              (not is_nil(details.value) or details.write_reserved?)
          end)
          |> Enum.max_by(fn {h, _details} -> h end, fn -> nil end)

        case maybe_relevant_entry do
          # 3. No relevant entry found below height_req (implies initial state or empty)
          nil ->
            # If no entry with a value or reservation exists below height_req, the result is absent.
            {:ok, :absent}

          # 4. Relevant entry found, check its state
          {_h, details} ->
            cond do
              # If the latest relevant entry has a value (is committed), resolve the read.
              not is_nil(details.value) ->
                {:ok, {:ok, details.value}}

              # If the latest relevant entry holds a write reservation, block the read.
              details.write_reserved? ->
                :blocked_by_write_reservation

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
