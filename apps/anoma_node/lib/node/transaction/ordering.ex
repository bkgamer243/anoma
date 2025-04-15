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
  alias Anoma.Node.Transaction.ShardSupervisor
  alias Anoma.Node.Transaction.Storage
  alias Anoma.Node.Transaction.Backends

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
            next_height: non_neg_integer(),
            shard_key_map: :ets.tab()
          ]
          | [node_id: String.t(), shard_key_map: :ets.tab()]
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
    - `:shard_key_map` - ETS table mapping keys to their shard labels.
    - `:tx_id_to_intended_reservations` - A map from transaction IDs to their intended reservations.
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

    # Stores completion data for transactions that finish before getting a height.
    # Maps tx_id => {vm_result, backend, failed?}
    field(
      :tx_id_to_completion_data,
      %{binary() => {Backends.vm_result(), Backends.backend(), boolean()}},
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
    field(:shard_key_map, :ets.tab(), default: nil)
    # Stores the original list of reservations requested by a transaction.
    # Used during finalization to ensure watermarks are recalculated correctly,
    # even if dynamic reservations happened outside this state tracker.
    field(
      :tx_id_to_intended_reservations,
      %{binary() => [{:read | :write, binary()}]},
      default: %{}
    )
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

  typedstruct enforce: true, module: ReservationsAcquiredEvent do
    @typedoc """
    I am the type of a reservations acquired Event.

    I am sent when reservations for a transaction have been successfully acquired.

    ### Fields

    - `tx_id` - The ID of the transaction which had reservations acquired.
    - `height` - The height assigned to the transaction.
    - `shard_pids` - Map of keys to their shard PIDs.
    """
    field(:tx_id, binary())
    field(:height, integer())
    field(:shard_pids, %{binary() => pid()})
  end

  typedstruct enforce: true, module: TransactionFinishedEvent do
    @typedoc """
    I am the type of a transaction finished Event.

    I am sent when a transaction associated with the tx_id has finished its shard storage logic or reservation release, either successfully or with failure.

    ### Fields

    - `tx_id` - The ID of the transaction which finished.
    - `failed?` - Boolean indicating if the transaction failed.
    - `vm_result` - The result of the transaction's VM execution.
    - `backend` - The backend associated with the transaction.
    """
    field(:tx_id, binary())
    field(:failed?, boolean())
    field(:vm_result, Backends.vm_result())
    field(:backend, Backends.backend())
  end

  deffilter TxIdFilter, tx_id: binary() do
    %EventBroker.Event{body: %Node.Event{body: %{tx_id: ^tx_id}}} -> true
    _ -> false
  end

  deffilter TransactionFinishedFilter do
    %EventBroker.Event{body: %Node.Event{body: %TransactionFinishedEvent{}}} ->
      true

    _ ->
      false
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

    node_id = Keyword.fetch!(args, :node_id)

    args = Keyword.validate!(args, [:node_id, :shard_key_map, next_height: 1])

    # Try to get the shard key map from the ShardSupervisor
    shard_key_map = ShardSupervisor.get_shard_key_map(node_id)

    # Create initial state with the shard key map
    args =
      if not is_nil(shard_key_map) do
        Keyword.put(args, :shard_key_map, shard_key_map)
      else
        args
      end

    state = struct(Ordering, Enum.into(args, %{}))

    # Subscribe to completion and failure events
    EventBroker.subscribe_me([
      Node.Event.node_filter(node_id),
      %Anoma.Node.Transaction.Ordering.TransactionFinishedFilter{}
    ])

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

  This is an asynchronous operation. The result will be sent as a ReservationsAcquiredEvent.
  """
  @spec request_reservations(String.t(), binary(), [
          {:read | :write, binary()}
        ]) ::
          :ok
  def request_reservations(node_id, id, reservations) do
    GenServer.cast(
      Registry.via(node_id, __MODULE__),
      {:request_reservations, id, reservations}
    )
  end

  @doc """
  I am the Ordering shard_read function.

  I receive a Node ID and an {tx_id, key} tuple. I look up the appropriate shard
  for the key and read from it at the height associated with the transaction ID.

  If the transaction ID does not yet have a height assigned, I wait for an
  OrderEvent to arrive with that transaction ID.
  """
  @spec shard_read(String.t(), {binary(), binary()}) :: any()
  def shard_read(node_id, {tx_id, key}) do
    GenServer.call(
      Registry.via(node_id, __MODULE__),
      {:shard_read, {tx_id, key}},
      :infinity
    )
  end

  @doc """
  I am the Ordering shard_write function.

  I receive a Node ID and an {tx_id, key, value} tuple. I look up the appropriate shard
  for the key and write the value to it at the height associated with the transaction ID.

  If the transaction ID does not yet have a height assigned, I wait for an
  OrderEvent to arrive with that transaction ID.
  """
  @spec shard_write(String.t(), {binary(), binary(), any()}) :: :ok
  def shard_write(node_id, {tx_id, key, value}) do
    GenServer.call(
      Registry.via(node_id, __MODULE__),
      {:shard_write, {tx_id, key, value}},
      :infinity
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

  def handle_call({:shard_read, {tx_id, key}}, from, state) do
    handle_shard_operation(:read, tx_id, key, nil, from, state)
    {:noreply, state}
  end

  def handle_call({:shard_write, {tx_id, key, value}}, from, state) do
    handle_shard_operation(:write, tx_id, key, value, from, state)
    {:noreply, state}
  end

  def handle_call(_msg, _from, state) do
    {:reply, :ok, state}
  end

  @impl true
  def handle_cast({:order, tx_id_list}, state) do
    {:noreply, handle_order(tx_id_list, state)}
  end

  def handle_cast({:request_reservations, tx_id, reservations}, state) do
    {:noreply, handle_request_reservations(tx_id, reservations, state)}
  end

  def handle_cast({:set_shard_key_map, ets_tid}, state) do
    Logger.info(
      "Received shard key map ETS table with tid: #{inspect(ets_tid)}"
    )

    {:noreply, %{state | shard_key_map: ets_tid}}
  end

  def handle_cast(_msg, state) do
    {:noreply, state}
  end

  @impl true
  def handle_info(
        %EventBroker.Event{
          body: %Node.Event{
            body: %TransactionFinishedEvent{
              tx_id: id,
              failed?: failed?,
              vm_result: vm_result,
              backend: backend
            }
          }
        },
        state
      ) do
    {:noreply,
     process_transaction_finished(id, failed?, vm_result, backend, state)}
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
    {map, next_order, final_state} =
      Enum.reduce(
        tx_id_list,
        {state.tx_id_to_height, state.next_height, state},
        fn tx_id, {acc_map, current_order, current_state} ->
          {new_map, next_order_for_tx, updated_state} =
            process_ordered_tx(tx_id, current_order, acc_map, current_state)

          {new_map, next_order_for_tx, updated_state}
        end
      )

    %{final_state | tx_id_to_height: map, next_height: next_order}
  end

  # Helper function to process a single transaction during ordering
  @spec process_ordered_tx(binary(), integer(), %{binary() => integer()}, t()) ::
          {%{binary() => integer()}, integer(), t()}
  defp process_ordered_tx(tx_id, current_order, acc_map, current_state) do
    # Assign height
    new_map = Map.put(acc_map, tx_id, current_order)

    state_with_height = %{
      current_state
      | tx_id_to_height: new_map,
        next_height: current_order + 1
    }

    # Send OrderEvent first, allowing potential listeners (like reservation waiters) to react
    order_event =
      Node.Event.new_with_body(current_state.node_id, %__MODULE__.OrderEvent{
        tx_id: tx_id
      })

    EventBroker.event(order_event)

    final_state = try_finalize_transaction(tx_id, state_with_height)

    {new_map, current_order + 1, final_state}
  end

  @spec handle_request_reservations(
          binary(),
          [{:read | :write, binary()}],
          t()
        ) :: t()
  defp handle_request_reservations(tx_id, reservations, state) do
    # Store the intended reservations regardless of height availability
    state_with_intended = %{
      state
      | tx_id_to_intended_reservations:
          Map.put(state.tx_id_to_intended_reservations, tx_id, reservations)
    }

    # Check if we have a height for this transaction
    case Map.fetch(state_with_intended.tx_id_to_height, tx_id) do
      {:ok, height} ->
        tx_res_state = %{
          height: height,
          reservations: reservations,
          status: :pending,
          shard_pids: %{}
        }

        state_with_pending_tx = %{
          state_with_intended
          | tx_reservations:
              Map.put(state.tx_reservations, tx_id, tx_res_state)
        }

        keys_to_update =
          Enum.map(reservations, fn {_, key} -> key end) |> Enum.uniq()

        case try_acquire_reservations(
               reservations,
               height,
               state_with_pending_tx
             ) do
          {:ok, acquired_shard_pids} ->
            handle_reservation_success(
              tx_id,
              height,
              acquired_shard_pids,
              keys_to_update,
              state_with_pending_tx
            )

          {:error, failed_shard_pids, _reason} ->
            handle_reservation_failure(
              tx_id,
              height,
              failed_shard_pids,
              keys_to_update,
              state_with_pending_tx
            )
        end

      :error ->
        # No height found for transaction yet, schedule retry
        block_spawn(
          tx_id,
          fn ->
            block_reservations(
              state_with_intended.node_id,
              tx_id,
              reservations
            )
          end,
          state_with_intended.node_id
        )

        state_with_intended
    end
  end

  @spec try_acquire_reservations([{type, key}], height, t()) ::
          {:ok, %{key => pid}}
          | {:error, %{key => pid}, {atom(), key, any()}}
        when type: :read | :write, key: binary(), height: integer()
  defp try_acquire_reservations(reservations, height, state) do
    Enum.reduce_while(reservations, {:ok, %{}}, fn {type, key},
                                                   {:ok, pids_acc} ->
      # First, see if we already have this shard's pid
      case Map.fetch(pids_acc, key) do
        {:ok, shard_pid} ->
          # Already have this pid, just reserve
          case Shard.reserve(shard_pid, key, height, type) do
            :ok ->
              {:cont, {:ok, pids_acc}}

            {:error, reason} ->
              {:halt, {:error, pids_acc, {:reservation_failed, key, reason}}}
          end

        :error ->
          case get_shard_pid(key, state) do
            {:ok, shard_pid} ->
              case Shard.reserve(shard_pid, key, height, type) do
                :ok ->
                  {:cont, {:ok, Map.put(pids_acc, key, shard_pid)}}

                {:error, reason} ->
                  # Reserve failed, halt and include failed shard pid for rollback
                  {:halt,
                   {:error, Map.put(pids_acc, key, shard_pid),
                    {:reservation_failed, key, reason}}}
              end

            {:error, reason} ->
              Logger.error(
                "Failed to get shard PID for key #{inspect(key)} during reservation: #{inspect(reason)}"
              )

              {:halt,
               {:error, pids_acc, {:get_shard_pid_failed, key, reason}}}
          end
      end
    end)
  end

  @spec handle_reservation_success(
          binary(),
          integer(),
          %{binary() => pid()},
          [binary()],
          t()
        ) :: t()
  defp handle_reservation_success(
         tx_id,
         height,
         acquired_shard_pids,
         keys_to_update,
         state
       ) do
    # All reservations succeeded
    # Update reservation status to :acquired and store acquired PIDs
    updated_res_state = %{
      state.tx_reservations[tx_id]
      | status: :acquired,
        # Use the actually acquired pids
        shard_pids: acquired_shard_pids
    }

    state_with_acquired_tx = %{
      state
      | tx_reservations:
          Map.put(state.tx_reservations, tx_id, updated_res_state)
    }

    reservation_event =
      Node.Event.new_with_body(
        state.node_id,
        %__MODULE__.ReservationsAcquiredEvent{
          tx_id: tx_id,
          height: height,
          shard_pids: acquired_shard_pids
        }
      )

    EventBroker.event(reservation_event)

    # Recalculate watermarks for the keys involved
    state_with_watermarks =
      recalculate_and_update_watermarks_for_keys(
        keys_to_update,
        state_with_acquired_tx
      )

    try_finalize_transaction(tx_id, state_with_watermarks)
  end

  @spec handle_reservation_failure(
          binary(),
          integer(),
          %{binary() => pid()},
          [binary()],
          t()
        ) :: t()
  defp handle_reservation_failure(
         tx_id,
         height,
         failed_shard_pids,
         keys_to_update,
         state
       ) do
    # Reservation failed, rollback successful ones on shards
    # failed_shard_pids contains pids up to the point of failure
    release_reservations(failed_shard_pids, height)

    # Remove the failed transaction entry from the state it was added to
    state_without_failed_tx = %{
      state
      | tx_reservations: Map.delete(state.tx_reservations, tx_id)
    }

    # Recalculate watermarks - failure might unblock something (use argument)
    state_with_watermarks =
      recalculate_and_update_watermarks_for_keys(
        keys_to_update,
        state_without_failed_tx
      )

    try_finalize_transaction(tx_id, state_with_watermarks)
  end

  @spec block_reservations(
          String.t(),
          binary(),
          [{:read | :write, binary()}]
        ) :: :ok
  defp block_reservations(node_id, tx_id, reservations) do
    receive do
      %EventBroker.Event{
        body: %Node.Event{body: %__MODULE__.OrderEvent{tx_id: ^tx_id}}
      } ->
        request_reservations(node_id, tx_id, reservations)

      _ ->
        IO.puts("this should be unreachable")
    end

    EventBroker.unsubscribe_me([
      Node.Event.node_filter(node_id),
      this_module_filter(),
      tx_id_filter(tx_id)
    ])
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
        case get_shard_pid(key, state) do
          {:ok, shard_pid} ->
            send(shard_pid, {:read_watermark_advanced, key, new_read_wm})

          {:error, reason} ->
            Logger.error(
              "Failed to get shard PID for key #{inspect(key)} when advancing read watermark: #{inspect(reason)}"
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
        case get_shard_pid(key, state) do
          {:ok, shard_pid} ->
            send(shard_pid, {:write_watermark_advanced, key, new_write_wm})

          {:error, reason} ->
            Logger.error(
              "Failed to get shard PID for key #{inspect(key)} when advancing write watermark: #{inspect(reason)}"
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
  @spec get_shard_pid(binary(), t()) ::
          {:ok, pid()}
          | {:error, :shard_key_map_unavailable}
          | {:error, :key_not_found_in_map, binary()}
          | {:error, :shard_not_registered, atom()}
          | {:error, :unexpected_ets_result, any()}
          | {:error, :ets_lookup_failed, any()}
  defp get_shard_pid(key, state = %__MODULE__{}) do
    try do
      ets_tid = state.shard_key_map

      case ets_tid do
        nil ->
          {:error, :shard_key_map_unavailable}

        ets_tid ->
          case :ets.lookup(ets_tid, key) do
            [{^key, shard_label}] when is_atom(shard_label) ->
              # Found label, now find the registered Shard PID
              case Registry.whereis(state.node_id, Shard, shard_label) do
                nil ->
                  {:error, :shard_not_registered, shard_label}

                pid ->
                  {:ok, pid}
              end

            [] ->
              {:error, :key_not_found_in_map, key}

            other ->
              Logger.error(
                "Unexpected ETS lookup result for key #{inspect(key)}: #{inspect(other)}"
              )

              {:error, :unexpected_ets_result, other}
          end
      end
    catch
      kind, reason ->
        Logger.error(
          "Exception during ETS shard lookup for key #{inspect(key)} - Kind: #{kind}, Reason: #{inspect(reason)}, Stacktrace: #{inspect(__STACKTRACE__)}"
        )

        {:error, :ets_lookup_failed, {kind, reason}}
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
    {:ok, task_pid} =
      Task.start(call)

    EventBroker.subscribe(task_pid, [
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

  @spec blocking_shard_operation(
          :read | :write,
          String.t(),
          binary(),
          binary(),
          any() | nil,
          GenServer.from()
        ) :: :ok
  defp blocking_shard_operation(:read, node_id, tx_id, key, _value, from) do
    block(from, tx_id, fn -> shard_read(node_id, {tx_id, key}) end, node_id)
  end

  defp blocking_shard_operation(:write, node_id, tx_id, key, value, from) do
    block(
      from,
      tx_id,
      fn -> shard_write(node_id, {tx_id, key, value}) end,
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

  @spec handle_shard_operation(
          :read | :write,
          binary(),
          binary(),
          # Value is nil for :read
          any() | nil,
          GenServer.from(),
          t()
        ) :: any()
  defp handle_shard_operation(type, tx_id, key, value, from, state) do
    case Map.fetch(state.tx_id_to_height, tx_id) do
      {:ok, height} ->
        case get_shard_pid(key, state) do
          {:ok, shard_pid} ->
            # Have height and shard pid, check reservation
            case get_reservation_for_key(tx_id, key, type, state) do
              {:ok, _} ->
                # Have reservation, proceed with operation
                Task.start(fn ->
                  result =
                    case type do
                      :read -> Shard.read(shard_pid, key, height)
                      :write -> Shard.write(shard_pid, key, value, height)
                    end

                  GenServer.reply(from, result)
                end)

              :error ->
                # No reservation found, queue request to wait for acquisition
                block_spawn(
                  tx_id,
                  fn ->
                    block_for_reservation(
                      type,
                      tx_id,
                      key,
                      value,
                      from,
                      shard_pid,
                      height,
                      state.node_id
                    )
                  end,
                  state.node_id
                )
            end

          {:error, reason} ->
            Logger.error(
              "Failed to get shard PID for key #{inspect(key)} in handle_shard_operation: #{inspect(reason)}"
            )

            # Block until height is known (will implicitly retry shard PID lookup upon retry)
            block_spawn(
              tx_id,
              fn ->
                blocking_shard_operation(
                  type,
                  state.node_id,
                  tx_id,
                  key,
                  value,
                  from
                )
              end,
              state.node_id
            )
        end

      :error ->
        # Block until height is known
        block_spawn(
          tx_id,
          fn ->
            blocking_shard_operation(
              type,
              state.node_id,
              tx_id,
              key,
              value,
              from
            )
          end,
          state.node_id
        )
    end
  end

  # Helper function to wait for reservations to be acquired before proceeding with a read/write.
  @spec block_for_reservation(
          :read | :write,
          binary(),
          binary(),
          any() | nil,
          GenServer.from(),
          pid(),
          integer(),
          String.t()
        ) :: :ok
  defp block_for_reservation(
         type,
         tx_id,
         key,
         # nil for reads
         value,
         from,
         shard_pid,
         height,
         node_id
       ) do
    # Subscribe only to the events we care about for this specific tx_id
    filters = [
      Node.Event.node_filter(node_id),
      tx_id_filter(tx_id)
    ]

    EventBroker.subscribe_me(filters)

    receive do
      %EventBroker.Event{
        body: %Node.Event{
          body: %ReservationsAcquiredEvent{
            tx_id: ^tx_id,
            shard_pids: shard_pids
          }
        }
      } ->
        # Check if the specific shard PID is in the acquired list
        if Map.has_key?(shard_pids, key) and shard_pids[key] == shard_pid do
          result =
            case type do
              :read -> Shard.read(shard_pid, key, height)
              :write -> Shard.write(shard_pid, key, value, height)
            end

          GenServer.reply(from, result)
        else
          # This should not happen if the reservation logic is correct.
          Logger.error(
            "Received ReservationsAcquiredEvent for tx #{inspect(tx_id)}, but shard PID for key #{inspect(key)} mismatch or missing."
          )

          GenServer.reply(from, {:error, :reservation_pid_mismatch})
        end

      %EventBroker.Event{
        body: %Node.Event{
          body: %TransactionFinishedEvent{tx_id: ^tx_id, failed?: failed?}
        }
      } ->
        # Transaction finished (potentially failed) before/during reservation.
        reply_msg =
          if failed? do
            {:error, :transaction_failed}
          else
            {:error, :transaction_completed_without_operation}
          end

        GenServer.reply(from, reply_msg)

      _other ->
        block_for_reservation(
          type,
          tx_id,
          key,
          value,
          from,
          shard_pid,
          height,
          node_id
        )
    end

    EventBroker.unsubscribe_me(filters)
    :ok
  end

  # Helper to check if a transaction has a reservation for a specific key and type
  @spec get_reservation_for_key(binary(), binary(), :read | :write, t()) ::
          {:ok, any()} | :error
  defp get_reservation_for_key(tx_id, key, type, state) do
    with {:ok, tx_res} <- Map.fetch(state.tx_reservations, tx_id),
         true <- tx_res.status == :acquired,
         true <-
           Enum.any?(tx_res.reservations, fn res -> res == {type, key} end) do
      {:ok, tx_res}
    else
      _ -> :error
    end
  end

  # Helper function to handle TransactionFinishedEvent
  @spec process_transaction_finished(
          binary(),
          boolean(),
          Backends.vm_result(),
          Backends.backend(),
          t()
        ) :: t()
  defp process_transaction_finished(id, failed?, vm_result, backend, state) do
    # Store the completion data
    updated_completion_data =
      Map.put(
        state.tx_id_to_completion_data,
        id,
        {vm_result, backend, failed?}
      )

    state_with_completion_data = %{
      state
      | tx_id_to_completion_data: updated_completion_data
    }

    # Try to finalize with the centralized function
    try_finalize_transaction(id, state_with_completion_data)
  end

  # Centralized function that checks if a transaction can be finalized and performs necessary steps
  @spec try_finalize_transaction(binary(), t()) :: t()
  defp try_finalize_transaction(tx_id, state) do
    # Check conditions: height, completion data, and INTENDED reservations
    with {:height, {:ok, height}} <-
           {:height, Map.fetch(state.tx_id_to_height, tx_id)},
         {:completion, {:ok, {vm_result, backend, failed?}}} <-
           {:completion, Map.fetch(state.tx_id_to_completion_data, tx_id)} do
      # Check if INTENDED reservations were recorded for this tx
      case Map.fetch(state.tx_id_to_intended_reservations, tx_id) do
        {:ok, intended_reservations} ->
          # All conditions met - notify completion and perform cleanup
          Logger.debug(
            "Transaction #{inspect(tx_id)} can be finalized - has height, completion data, and intended reservations. Performing cleanup."
          )

          # Notify backends of completion
          Backends.notify_completion(
            state.node_id,
            tx_id,
            vm_result,
            backend,
            failed?
          )

          # Best effort release on failure
          if failed? do
            case Map.fetch(state.tx_reservations, tx_id) do
              {:ok, tx_res} ->
                release_reservations(tx_res.shard_pids, height)

              :error ->
                :ok
            end
          end

          # Remove from central tracking maps
          updated_tx_reservations = Map.delete(state.tx_reservations, tx_id)

          updated_intended_reservations =
            Map.delete(state.tx_id_to_intended_reservations, tx_id)

          updated_completion_data =
            Map.delete(state.tx_id_to_completion_data, tx_id)

          state_after_removal = %{
            state
            | tx_reservations: updated_tx_reservations,
              tx_id_to_intended_reservations: updated_intended_reservations,
              tx_id_to_completion_data: updated_completion_data
          }

          # Update HCC (needs height)
          {final_hcc, final_completed_set} =
            update_hcc(height, state_after_removal)

          state_after_hcc = %{
            state_after_removal
            | highest_consecutive_completion: final_hcc,
              completed_above_hcc: final_completed_set
          }

          # Recalculate watermarks using keys from the INTENDED list
          intended_keys =
            Enum.map(intended_reservations, fn {_, key} -> key end)

          recalculate_and_update_watermarks_for_keys(
            intended_keys,
            state_after_hcc
          )

        :error ->
          # No reservations required or tracked
          Logger.debug(
            "Transaction #{inspect(tx_id)} has height and completion data, but no INTENDED reservations recorded (non-sharded tx?). Notifying completion."
          )

          Backends.notify_completion(
            state.node_id,
            tx_id,
            vm_result,
            backend,
            failed?
          )

          # No reservation cleanup needed, just remove completion data
          %{
            state
            | tx_id_to_completion_data:
                Map.delete(state.tx_id_to_completion_data, tx_id),
              # Also clean up intended reservations if somehow present but empty
              tx_id_to_intended_reservations:
                Map.delete(state.tx_id_to_intended_reservations, tx_id)
          }
      end
    else
      {:height, :error} ->
        Logger.debug(
          "Transaction #{inspect(tx_id)} doesn't have a height assigned yet."
        )

        state

      {:completion, :error} ->
        Logger.debug(
          "Transaction #{inspect(tx_id)} doesn't have completion data yet."
        )

        state
    end
  end
end
