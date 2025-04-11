defmodule Anoma.Node.Transaction.Ordering do
  @moduledoc """
  I am the Ordering Engine.

  I act as a mediator between Workers and Storage. In particular, Workers
  working on a transaction may ask to read and write information. However,
  they do not know when to do it, they only know the ID of the transaction
  they work on.

  I process such requests, keeping them waiting until consensus provides
  some ordering to a transaction in question. Once they do, I pair a
  transaction ID with its timestamp and forward queries to Storage.

  ### Public API

  I provide the following public functonality:

  - `read/2`
  - `write/2`
  - `append/2`
  - `add/2`
  - `order/2`
  - `request_reservations/3`
  - `transaction_completed/2`
  - `transaction_failed/2`
  """

  alias __MODULE__
  alias Anoma.Node
  alias Anoma.Node.Registry
  alias Anoma.Node.Transaction.Shard
  alias Anoma.Node.Transaction.ShardRouter
  alias Anoma.Node.Transaction.Storage

  require Logger
  require Node.Event

  use EventBroker.DefFilter
  use GenServer
  use TypedStruct

  ############################################################
  #                       Types                              #
  ############################################################

  @typedoc """
  Type of the arguments the ordering genserver expects
  """
  @type args_t ::
          [
            node_id: String.t(),
            next_height: non_neg_integer()
          ]
          | [node_id: String.t()]
  ############################################################
  #                         State                            #
  ############################################################

  typedstruct enforce: true do
    @typedoc """
    I am the type of the Ordering Enigine.

    I contain the Node for which the Ordering is launched, the upcoming
    height as well as a map from transaction IDs to their global order.

    ### Fields

    - `:node_id` - The ID of the Node to which an Ordering instantiation is
                   bound.
    - `:next_height` - The height that the next ordered transaction
                       candidate will get.
                       Default: 1
    - `:tx_id_to_height` - A map from an ID of a transaction candidate to
                           its order.
    - `:tx_reservations` - A map from an ID of a transaction candidate to
                           its reservation information.
    - `:watermark_state` - A map from a key to its watermark state.
    - `:highest_consecutive_completion` - The highest consecutive completion height.
    - `:completed_above_hcc` - A set of heights completed above the highest consecutive completion.
    """
    field(:node_id, String.t())
    field(:next_height, integer(), default: 1)
    # maps tx ids to their height for writing.
    # the previous height is used for reading.
    field(:tx_id_to_height, %{binary() => integer()}, default: %{})
    # maps tx ids to their reservation information
    field(
      :tx_reservations,
      %{
        binary() => %{
          height: integer(),
          reservations: [{:read | :write, binary()}],
          status: :pending | :acquired,
          shard_pids: %{binary() => pid()}
        }
      },
      default: %{}
    )

    # tracks watermarks for each key
    # Watermarks represent the highest height h-1 such that all transactions < h
    # are either completed or don't reserve the key for the specific type.
    # Default is -1, meaning no heights are guaranteed safe yet.
    field(
      :watermark_state,
      %{
        binary() => %{
          read_watermark: integer(),
          write_watermark: integer()
        }
      },
      default: %{}
    )

    field(:highest_consecutive_completion, integer(), default: 0)
    field(:completed_above_hcc, MapSet.t(integer()), default: MapSet.new())
  end

  typedstruct enforce: true, module: OrderEvent do
    @typedoc """
    I am the type of an ordering Event.

    I am sent whenever the transaction with which I am associated gets a
    global timestamp.

    ### Fields

    - `tx_id` - The ID of the transaction which was ordered.
    """

    field(:tx_id, binary())
  end

  deffilter TxIdFilter, tx_id: binary() do
    %EventBroker.Event{body: %Node.Event{body: %{tx_id: ^tx_id}}} -> true
    _ -> false
  end

  @doc """
  I am the start_link function for the Ordering Engine.

  I register the engine with supplied node ID provided by the arguments.
  """

  @spec start_link(args_t()) :: GenServer.on_start()
  def start_link(args \\ []) do
    name = Registry.via(args[:node_id], __MODULE__)
    GenServer.start_link(__MODULE__, args, name: name)
  end

  ############################################################
  #                    Genserver Helpers                     #
  ############################################################

  @doc """
  I am the initialization function for the Ordering Engine.

  From the specified arguments, I get the Node ID as well as the info
  regarding the next height Ordering should be started with.
  """

  @impl true
  def init(args) do
    Process.set_label(__MODULE__)

    args = Keyword.validate!(args, [:node_id, next_height: 1])

    state = struct(Ordering, Enum.into(args, %{}))

    {:ok, state}
  end

  ############################################################
  #                      Public RPC API                      #
  ############################################################

  @doc """
  I am the Ordering read function.

  I receive a Node ID and an {id, key} tuple. There are two states possible
  when Ordering processes my request. Either:

  - The id has been assigned an order.
  - The id has not been assigned an order.

  If the former is true, I send the request to read the key at the
  specified height minus one to the Storage. That is, we ask the storage to
  read the most recent value assigned to the key from the point of view of
  the transaction candidate. See `Storage.read/2`.

  If the latter is true, I leave the caller blocked until the id has been
  assigned a value, i.e. until a corresponding event gets received.
  """

  @spec read(String.t(), {binary(), any()}) :: any()
  def read(node_id, {id, key}) do
    GenServer.call(
      Registry.via(node_id, __MODULE__),
      {:read, {id, key}},
      :infinity
    )
  end

  @doc """
  I am the Ordering write function.

  I receive a Node ID and an {id, kvlist} tuple. There are two states
  possible when Ordering processes my request. Either:

  - The id has been assigned an order.
  - The id has not been assigned an order.

  If the former is true, I send the request to write the key-value list at
  the specified height to the Storage. See `Storage.write/2`

  If the latter is true, I leave the caller blocked until the id has been
  assigned a value, i.e. until a corresponding event gets received.
  """

  @spec write(String.t(), {binary(), list({any(), any()})}) :: :ok
  def write(node_id, {id, kvlist}) do
    GenServer.call(
      Registry.via(node_id, __MODULE__),
      {:write, {id, kvlist}},
      :infinity
    )
  end

  @doc """
  I am the Ordering append function.

  I receive a Node ID and an {id, kvlist} tuple. There are two states
  possible when Ordering processes my request. Either:

  - The id has been assigned an order.
  - The id has not been assigned an order.

  If the former is true, I send the request to append the key-value list at
  the specified height to the Storage. See `Storage.append/2`

  If the latter is true, I leave the caller blocked until the id has been
  assigned a value, i.e. until a corresponding event gets received.
  """

  @spec append(String.t(), {binary(), list({any(), MapSet.t()})}) :: :ok
  def append(node_id, {id, kvlist}) do
    GenServer.call(
      Registry.via(node_id, __MODULE__),
      {:append, {id, kvlist}},
      :infinity
    )
  end

  @doc """
  I am the Ordering write function.

  I receive a Node ID and an {id, map} tuple. There are two states possible
  when Ordering processes my request. Either:

  - The id has been assigned an order.
  - The id has not been assigned an order.

  If the former is true, I send the request to appropriately add the map to
  the Storage at the specified height. See `Storage.add/2`

  If the latter is true, I leave the caller blocked until the id has been
  assigned a value, i.e. until a corresponding event gets received.
  """

  @spec add(String.t(), {binary(), %{write: list(), append: list()}}) :: any()
  def add(node_id, {id, map}) do
    GenServer.call(
      Registry.via(node_id, __MODULE__),
      {:add, {id, map}},
      :infinity
    )
  end

  @doc """
  I am the Ordering order function.

  Given a Node ID and a list of transaction IDs, I percieve the latter as a
  partial ordering of transactions. Afterwards, I assign them a global
  ordering by adding the next height stored in the Ordering Engine to the
  respective ordering inside a list.

  Afterwards, I send an event specifying that a particular ID has indeed
  received an order.
  """

  @spec order(String.t(), [binary()]) :: :ok
  def order(node_id, txs) do
    GenServer.cast(Registry.via(node_id, __MODULE__), {:order, txs})
  end

  @doc """
  I am the Ordering request_reservations function.

  I receive a Node ID, a transaction ID, and a list of reservations.
  I attempt to acquire all reservations in a transactional manner.

  If all reservations are acquired successfully, I return {:ok, {height, shard_pids}}.
  Otherwise, I return {:error, reason}.
  """
  @spec request_reservations(String.t(), binary(), [
          {:read | :write, binary()}
        ]) ::
          {:ok, {integer(), %{binary() => pid()}}} | {:error, term()}
  def request_reservations(node_id, id, reservations) do
    GenServer.call(
      Registry.via(node_id, __MODULE__),
      {:request_reservations, id, reservations},
      :infinity
    )
  end

  @doc """
  I am the Ordering transaction_completed function.

  I receive a Node ID, a transaction ID, its height.
  For completed transactions, I update watermarks for keys that were reserved.
  """
  @spec transaction_completed(String.t(), binary()) :: :ok
  def transaction_completed(node_id, id) do
    GenServer.cast(
      Registry.via(node_id, __MODULE__),
      {:transaction_completed, id}
    )
  end

  @doc """
  I am the Ordering transaction_failed function.

  For failed transactions, I release all reservations and clean up state.
  """
  @spec transaction_failed(String.t(), binary()) :: :ok
  def transaction_failed(node_id, id) do
    GenServer.cast(
      Registry.via(node_id, __MODULE__),
      {:transaction_failed, id}
    )
  end

  ############################################################
  #                      Public Filters                      #
  ############################################################

  @doc """
  I am a filter spec which filters for any event with a `tx_id` field and
  matches iff the ID stored is the one supplied.
  """

  @spec tx_id_filter(binary()) :: TxIdFilter.t()
  def tx_id_filter(tx_id) do
    %__MODULE__.TxIdFilter{tx_id: tx_id}
  end

  ############################################################
  #                    Genserver Behavior                    #
  ############################################################

  @impl true
  def handle_call({write_opt, {tx_id, args}}, from, state)
      when write_opt in [:write, :append, :add] do
    handle_write(write_opt, {tx_id, args}, from, state)

    {:noreply, state}
  end

  def handle_call({:read, {tx_id, key}}, from, state) do
    handle_read({tx_id, key}, from, state)
    {:noreply, state}
  end

  def handle_call({:request_reservations, tx_id, reservations}, from, state) do
    case handle_request_reservations(tx_id, reservations, from, state) do
      {:reply, response, new_state} -> {:reply, response, new_state}
      {:noreply, new_state} -> {:noreply, new_state}
    end
  end

  def handle_call(_msg, _from, state) do
    {:reply, :ok, state}
  end

  @impl true
  def handle_cast({:order, tx_id_list}, state) do
    {:noreply, handle_order(tx_id_list, state)}
  end

  def handle_cast({:transaction_completed, id}, state) do
    {:noreply, handle_transaction_finished(id, state)}
  end

  def handle_cast({:transaction_failed, id}, state) do
    {:noreply, handle_transaction_finished(id, state, failed: true)}
  end

  def handle_cast(_msg, state) do
    {:noreply, state}
  end

  @impl true
  def handle_info(_info, state) do
    {:noreply, state}
  end

  ############################################################
  #                 Genserver Implementation                 #
  ############################################################

  @spec handle_write(
          Storage.write_opts(),
          {binary(), [any()]},
          GenServer.from(),
          t()
        ) :: any()
  defp handle_write(write_opt, {tx_id, args}, from, state) do
    call = &chose_write_function(write_opt).(state.node_id, &1)

    with {:ok, height} <- Map.fetch(state.tx_id_to_height, tx_id) do
      Task.start(fn ->
        GenServer.reply(from, call.({height, args}))
      end)
    else
      _ ->
        node_id = state.node_id

        block_spawn(
          tx_id,
          fn ->
            blocking_write(node_id, {tx_id, args}, from)
          end,
          node_id
        )
    end
  end

  @spec handle_read({binary(), any()}, GenServer.from(), t()) :: any()
  defp handle_read({tx_id, key}, from, state) do
    with {:ok, height} <- Map.fetch(state.tx_id_to_height, tx_id) do
      Task.start(fn ->
        GenServer.reply(from, Storage.read(state.node_id, {height - 1, key}))
      end)

      {:noreply, state}
    else
      _ ->
        node_id = state.node_id

        block_spawn(
          tx_id,
          fn ->
            blocking_read(node_id, {tx_id, key}, from)
          end,
          node_id
        )

        {:noreply, state}
    end
  end

  @spec handle_order(list(binary()), t()) :: t()
  defp handle_order(tx_id_list, state) do
    {map, next_order} =
      for tx_id <- tx_id_list,
          reduce: {state.tx_id_to_height, state.next_height} do
        {map, order} ->
          order_event =
            Node.Event.new_with_body(state.node_id, %__MODULE__.OrderEvent{
              tx_id: tx_id
            })

          EventBroker.event(order_event)
          {Map.put(map, tx_id, order), order + 1}
      end

    %__MODULE__{state | tx_id_to_height: map, next_height: next_order}
  end

  @spec handle_request_reservations(
          binary(),
          [{:read | :write, binary()}],
          GenServer.from(),
          t()
        ) ::
          {:reply,
           {:ok, {integer(), %{binary() => pid()}}} | {:error, term()}, t()}
          | {:noreply, t()}
  defp handle_request_reservations(tx_id, reservations, from, state) do
    # Check if we have a height for this transaction
    case Map.fetch(state.tx_id_to_height, tx_id) do
      {:ok, height} ->
        # Store initial reservation info before attempting acquisition
        tx_res_state = %{
          height: height,
          reservations: reservations,
          # Start as pending
          status: :pending,
          shard_pids: %{}
        }

        # Tentatively add/update the transaction in the reservations map
        state_with_pending_tx = %{
          state
          | tx_reservations:
              Map.put(state.tx_reservations, tx_id, tx_res_state)
        }

        # Try to acquire reservations
        {success, acquired_shard_pids, error} =
          Enum.reduce_while(reservations, {true, %{}, nil}, fn {type, key},
                                                               {_, pids_acc,
                                                                _} ->
            # First, see if we already have this shard's pid
            case Map.fetch(pids_acc, key) do
              {:ok, shard_pid} ->
                # Already have this pid, just reserve
                case Shard.reserve(shard_pid, key, height, type) do
                  :ok ->
                    {:cont, {true, pids_acc, nil}}

                  {:error, reason} ->
                    {:halt,
                     {false, pids_acc, {:reservation_failed, key, reason}}}
                end

              :error ->
                # Need to look up the shard via the router
                case get_shard_pid(state.node_id, key) do
                  {:ok, shard_pid} ->
                    # Got the pid, now reserve
                    case Shard.reserve(shard_pid, key, height, type) do
                      :ok ->
                        # Add to accumulated pids and continue
                        {:cont,
                         {true, Map.put(pids_acc, key, shard_pid), nil}}

                      {:error, reason} ->
                        # Reserve failed, halt and rollback
                        {
                          :halt,
                          # Include failed shard pid for rollback
                          {false, Map.put(pids_acc, key, shard_pid),
                           {:reservation_failed, key, reason}}
                        }
                    end

                  :error ->
                    # Router failed to find the shard pid
                    {:halt, {false, pids_acc, {:shard_lookup_failed, key}}}
                end

                # end case get_shard_pid
            end

            # end case Map.fetch pids_acc
          end)

        # end Enum.reduce_while

        if success do
          # All reservations succeeded
          # Update reservation status to :acquired and store acquired PIDs
          updated_res_state = %{
            state_with_pending_tx.tx_reservations[tx_id]
            | status: :acquired,
              # Use the actually acquired pids
              shard_pids: acquired_shard_pids
          }

          state_with_acquired_tx = %{
            state_with_pending_tx
            | tx_reservations:
                Map.put(
                  state_with_pending_tx.tx_reservations,
                  tx_id,
                  updated_res_state
                )
          }

          # Recalculate watermarks for the keys involved
          keys_to_update = Enum.map(reservations, fn {_, key} -> key end)

          final_state =
            recalculate_and_update_watermarks_for_keys(
              keys_to_update,
              state_with_acquired_tx
            )

          {:reply, {:ok, {height, acquired_shard_pids}}, final_state}
        else
          # Reservation failed, rollback successful ones on shards
          # acquired_shard_pids contains pids up to the point of failure
          release_reservations(acquired_shard_pids, height)

          # Remove the failed transaction entry
          state_without_failed_tx = %{
            state_with_pending_tx
            | # Start from the state before this attempt
              tx_reservations:
                Map.delete(state_with_pending_tx.tx_reservations, tx_id)
          }

          # Recalculate watermarks - failure might unblock something
          keys_to_update = Enum.map(reservations, fn {_, key} -> key end)

          final_state =
            recalculate_and_update_watermarks_for_keys(
              keys_to_update,
              # Use the state with the tx removed
              state_without_failed_tx
            )

          {:reply, {:error, error}, final_state}
        end

      :error ->
        # No height found for transaction yet
        # Wait for order event then re-handle the request
        block_spawn(
          tx_id,
          fn ->
            block_reservations(state.node_id, tx_id, reservations, from)
          end,
          state.node_id
        )

        {:noreply, state}
    end
  end

  @spec block_reservations(
          String.t(),
          binary(),
          [{:read | :write, binary()}],
          GenServer.from()
        ) :: :ok
  defp block_reservations(node_id, tx_id, reservations, from) do
    receive do
      %EventBroker.Event{
        body: %Node.Event{body: %__MODULE__.OrderEvent{tx_id: ^tx_id}}
      } ->
        # Transaction has been ordered, make the request again
        result = request_reservations(node_id, tx_id, reservations)
        GenServer.reply(from, result)

      _ ->
        IO.puts("this should be unreachable")
    end

    EventBroker.unsubscribe_me([
      Node.Event.node_filter(node_id),
      this_module_filter(),
      tx_id_filter(tx_id)
    ])
  end

  @spec handle_transaction_finished(binary(), t(), Keyword.t()) :: t()
  defp handle_transaction_finished(tx_id, state, opts \\ []) do
    failed? = Keyword.get(opts, :failed, false)

    case Map.fetch(state.tx_reservations, tx_id) do
      {:ok, tx_res} ->
        height = tx_res.height

        # Release reservations if failed
        if failed? do
          release_reservations(tx_res.shard_pids, height)
        end

        # Remove the transaction from the reservations map
        updated_tx_reservations = Map.delete(state.tx_reservations, tx_id)

        state_after_removal = %{
          state
          | tx_reservations: updated_tx_reservations
        }

        # Get keys associated with this transaction
        keys = Enum.map(tx_res.reservations, fn {_, key} -> key end)

        {final_hcc, final_completed_set} =
          update_hcc(height, state_after_removal)

        state_after_hcc = %{
          state_after_removal
          | # Use the state that has updated tx_reservations
            highest_consecutive_completion: final_hcc,
            completed_above_hcc: final_completed_set
        }

        recalculate_and_update_watermarks_for_keys(keys, state_after_hcc)

      :error ->
        Logger.warning(
          "Transaction #{inspect(tx_id)} not found in reservations during #{if failed?, do: "failure", else: "completion"}."
        )

        state
    end
  end

  # Helper to release reservations on Shards
  @spec release_reservations(%{binary() => pid()}, integer()) :: :ok
  defp release_reservations(shard_pids, height) do
    Enum.each(shard_pids, fn {_key, shard_pid} ->
      # Cast should be okay for unreserve as it's best-effort rollback
      Shard.unreserve(shard_pid, height)
    end)
  end

  # Recalculates watermarks for a list of keys and sends updates to shards if they advance.
  @spec recalculate_and_update_watermarks_for_keys([binary()], t()) :: t()
  defp recalculate_and_update_watermarks_for_keys(keys, state) do
    Enum.reduce(keys |> Enum.uniq(), state, fn key, acc_state ->
      recalculate_and_update_watermarks_for_key(key, acc_state)
    end)
  end

  # Recalculates watermarks for a single key and sends updates to shards if they advance.
  @spec recalculate_and_update_watermarks_for_key(binary(), t()) :: t()
  defp recalculate_and_update_watermarks_for_key(key, state) do
    current_wms =
      Map.get(state.watermark_state, key, %{
        read_watermark: -1,
        write_watermark: -1
      })

    new_read_wm = calculate_watermark(key, :read, state)
    new_write_wm = calculate_watermark(key, :write, state)

    state_after_update = state

    # Check and update read watermark
    state_after_update =
      if new_read_wm > current_wms.read_watermark do
        case get_shard_pid(state.node_id, key) do
          {:ok, shard_pid} ->
            send(shard_pid, {:read_watermark_advanced, key, new_read_wm})

          :error ->
            Logger.error(
              "Failed to get shard PID for key #{inspect(key)} when advancing read watermark."
            )

            # Continue without sending update
        end

        updated_key_wms = Map.put(current_wms, :read_watermark, new_read_wm)

        %{
          state_after_update
          | watermark_state:
              Map.put(
                state_after_update.watermark_state,
                key,
                updated_key_wms
              )
        }
      else
        # No change
        state_after_update
      end

    # Check and update write watermark
    # Re-fetch current_wms in case read watermark was updated
    current_wms_after_read =
      Map.get(state_after_update.watermark_state, key, %{
        read_watermark: -1,
        write_watermark: -1
      })

    state_after_update =
      if new_write_wm > current_wms_after_read.write_watermark do
        case get_shard_pid(state.node_id, key) do
          {:ok, shard_pid} ->
            send(shard_pid, {:write_watermark_advanced, key, new_write_wm})

          :error ->
            Logger.error(
              "Failed to get shard PID for key #{inspect(key)} when advancing write watermark."
            )

            # Continue without sending update
        end

        updated_key_wms =
          Map.put(current_wms_after_read, :write_watermark, new_write_wm)

        %{
          state_after_update
          | watermark_state:
              Map.put(
                state_after_update.watermark_state,
                key,
                updated_key_wms
              )
        }
      else
        # No change
        state_after_update
      end

    # Return final state for this key
    state_after_update
  end

  # Calculates the watermark value for a specific key and reservation type.
  # The watermark `w` for a given {key, type} is the highest height such that all
  # transactions with height `h <= w` are either completed or are pending but do
  # *not* reserve this specific {key, type}.
  #
  # This is determined by finding the lowest height `h` that *blocks* the watermark.
  # A height `h` blocks the watermark if it is *not* completed AND either:
  #   a) It is pending (in `tx_reservations`) and reserves {key, type}.
  #   b) Its status is unknown (i.e., `h > hcc`, `h` not in `completed_above_hcc`,
  #      and `h` not in `tx_reservations`).
  # The final watermark is `min_blocking_height - 1`.
  @spec calculate_watermark(binary(), :read | :write, t()) ::
          integer()
  defp calculate_watermark(key, type, state) do
    # 1. Find the lowest height of a *pending* transaction that reserves {key, type}
    lowest_pending_blocker =
      state.tx_reservations
      |> Enum.reduce_while(state.next_height, fn {_tx_id, tx_res}, min_h ->
        reserves_key_type? =
          Enum.any?(tx_res.reservations, fn res -> res == {type, key} end)

        if reserves_key_type? and tx_res.height < min_h do
          {:cont, tx_res.height}
        else
          {:cont, min_h}
        end
      end)

    # 2. Find the lowest height h > hcc that is neither completed nor pending.
    pending_heights =
      MapSet.new(state.tx_reservations, fn {_tx_id, tx_res} ->
        tx_res.height
      end)

    lowest_unknown_blocker =
      Enum.find(
        if state.highest_consecutive_completion + 1 <= state.next_height - 1 do
          (state.highest_consecutive_completion + 1)..(state.next_height - 1)
        else
          []
        end,
        # Default if none found
        state.next_height,
        fn h ->
          # A height is unknown if it's not completed AND not pending
          not MapSet.member?(state.completed_above_hcc, h) and
            not MapSet.member?(pending_heights, h)
        end
      )

    # 3. The actual blocker is the minimum of the two potential blockers.
    min_blocking_height = min(lowest_pending_blocker, lowest_unknown_blocker)

    # 4. The watermark is the height just before the blocker.
    max(min_blocking_height - 1, -1)
  end

  # Helper to get the PID of the shard responsible for a key.
  @spec get_shard_pid(String.t(), binary()) :: {:ok, pid()} | :error
  defp get_shard_pid(node_id, key) do
    case ShardRouter.get_shard_label(node_id, key) do
      {:ok, shard_label} when is_atom(shard_label) ->
        # Found label, now find the registered Shard PID
        case Registry.whereis(node_id, Shard, shard_label) do
          nil ->
            Logger.error(
              "Shard #{inspect(shard_label)} not registered for node #{node_id}"
            )

            :error

          pid ->
            {:ok, pid}
        end

      other ->
        Logger.error(
          "ShardRouter :get_shard_label returned unexpected value for key #{inspect(key)}: #{inspect(other)}"
        )

        :error
    end
  end

  ############################################################
  #                           Helpers                        #
  ############################################################

  @spec chose_write_function(Storage.write_opts()) ::
          (String.t(), {non_neg_integer(), list() | map()} ->
             any())
  defp chose_write_function(:write), do: &Storage.write/2
  defp chose_write_function(:append), do: &Storage.append/2
  defp chose_write_function(:add), do: &Storage.add/2

  ############################################################
  #                      Private Filters                     #
  ############################################################

  defp this_module_filter() do
    %EventBroker.Filters.SourceModule{module: __MODULE__}
  end

  ############################################################
  #                    Blocking Operations                   #
  ############################################################

  defp block_spawn(id, call, node_id) do
    {:ok, pid} =
      Task.start(call)

    EventBroker.subscribe(pid, [
      Node.Event.node_filter(node_id),
      this_module_filter(),
      tx_id_filter(id)
    ])
  end

  @spec blocking_read(String.t(), {binary(), any()}, GenServer.from()) :: :ok
  defp blocking_read(node_id, {id, key}, from) do
    block(from, id, fn -> read(node_id, {id, key}) end, node_id)
  end

  @spec blocking_write(String.t(), {binary(), [any()]}, GenServer.from()) ::
          :ok
  defp blocking_write(node_id, {id, kvlist}, from) do
    block(
      from,
      id,
      fn ->
        write(node_id, {id, kvlist})
      end,
      node_id
    )
  end

  @spec block(GenServer.from(), binary(), (-> any()), String.t()) :: :ok
  defp block(from, tx_id, call, node_id) do
    receive do
      %EventBroker.Event{
        body: %Node.Event{body: %__MODULE__.OrderEvent{tx_id: ^tx_id}}
      } ->
        result = call.()
        GenServer.reply(from, result)

      _ ->
        IO.puts("this should be unreachable")
    end

    EventBroker.unsubscribe_me([
      Node.Event.node_filter(node_id),
      this_module_filter(),
      tx_id_filter(tx_id)
    ])
  end

  # Helper to update Highest Consecutive Completion
  @spec update_hcc(integer(), t()) :: {integer(), MapSet.t()}
  defp update_hcc(finished_height, state) do
    new_completed_set = MapSet.put(state.completed_above_hcc, finished_height)
    current_hcc = state.highest_consecutive_completion

    # Check if the just-finished transaction allows advancing HCC
    advance_hcc(current_hcc, new_completed_set)
  end

  # Recursive helper to advance HCC as much as possible
  @spec advance_hcc(integer(), MapSet.t()) :: {integer(), MapSet.t()}
  defp advance_hcc(hcc, completed_set) do
    next_h = hcc + 1

    if MapSet.member?(completed_set, next_h) do
      # Found the next consecutive one, remove it and recurse
      advance_hcc(next_h, MapSet.delete(completed_set, next_h))
    else
      # Cannot advance further
      {hcc, completed_set}
    end
  end
end
