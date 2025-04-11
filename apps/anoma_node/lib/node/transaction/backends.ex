defmodule Anoma.Node.Transaction.Backends do
  @moduledoc """
  I am the Transaction Backend module.

  I define a set of backends for the execution of the given transaction candidate.
  Currently, I support transparent resource machine (RM) execution as well as
  the following debug executions: read-only, key-value store, and blob store executions.

  ### Public API

  I have the following public functionality:
  - `execute/3`
  """

  alias Anoma.CairoResource.Transaction, as: CTransaction
  alias Anoma.Node
  alias Anoma.Node.Logging
  alias Anoma.Node.Transaction.Executor
  alias Anoma.Node.Transaction.Ordering
  alias Anoma.Node.Transaction.Storage
  alias Anoma.Node.Transaction.Shard
  alias Anoma.TransparentResource
  alias Anoma.TransparentResource.Resource, as: TResource
  alias Anoma.TransparentResource.Transaction, as: TTransaction

  require Node.Event
  require Noun

  import Nock

  use EventBroker.DefFilter
  use TypedStruct

  @type backend() ::
          :debug_term_storage
          | {:read_only, pid}
          | :debug_bloblike
          | :transparent_resource
          | :cairo_resource
          | :shard_storage

  @type transaction() :: {backend(), Noun.t() | binary()}

  typedstruct enforce: true, module: ResultEvent do
    @typedoc """
    I hold the content of the Result Event, which conveys the result of
    the transaction candidate code execution on the Anoma VM to
    the Mempool engine.

    ### Fields
    - `:tx_id`              - The transaction id.
    - `:tx_result`          - VM execution result; either :error or an
                              {:ok, noun} tuple.
    """
    field(:tx_id, binary())
    field(:vm_result, {:ok, Noun.t()} | :error)
  end

  typedstruct enforce: true, module: CompleteEvent do
    @typedoc """
    I hold the content of the Complete Event, which communicates the result
    of the transaction candidate execution to the Executor engine.

    ### Fields
    - `:tx_id`              - The transaction id.
    - `:tx_result`          - Execution result; either :error or an
                              {:ok, value} tuple.
    """
    field(:tx_id, binary())
    field(:tx_result, {:ok, any()} | :error)
  end

  typedstruct enforce: true, module: TRMEvent do
    @typedoc """
    I hold the content of the The Resource Machine Event, which
    communicates a set of nullifiers/commitments defined by the actions of the
    transaction candidate to the Intent Pool.

    ### Fields

    - `:commitments`        - The set of commitments.
    - `:nullifiers`         - The set of nullifiers.
    - `:commitments`        - The set of commitments.
    """
    field(:commitments, MapSet.t(binary()))
    field(:nullifiers, MapSet.t(binary()))
  end

  typedstruct enforce: true, module: SRMEvent do
    @typedoc """
    I hold the content of the The Shielded Resource Machine Event, which
    communicates a set of nullifiers/commitments defined by the actions of the
    transaction candidate to the Intent Pool.

    ### Fields

    - `:commitments`        - The set of commitments.
    - `:nullifiers`         - The set of nullifiers.
    """
    field(:commitments, MapSet.t(binary()))
    field(:nullifiers, MapSet.t(binary()))
  end

  deffilter CompleteFilter do
    %EventBroker.Event{body: %Node.Event{body: %CompleteEvent{}}} ->
      true

    _ ->
      false
  end

  deffilter ForMempoolFilter do
    %EventBroker.Event{body: %Node.Event{body: %ResultEvent{}}} ->
      true

    _ ->
      false
  end

  deffilter ForMempoolExecutionFilter do
    %EventBroker.Event{body: %Node.Event{body: %Executor.ExecutionEvent{}}} ->
      true

    _ ->
      false
  end

  @doc """
  I execute the specified transaction candidate using the designated backend.
  If the transaction is provided as a `jam`med noun atom, I first attempt
  to apply `cue/1` in order to unpack the transaction code.

  First, I execute the transaction code on the Anoma VM. Next, I apply processing
  logic to the resulting value, dependent on the selected backend.
  - For read-only backend, the value is sent directly to specified recepient.
  - For the key-value and blob store executions, the obtained value is stored
  and a Complete Event is issued.
  - For the transparent Resource Machine (RM) execution, I verify the
    transaction's validity and compute the corresponding set of nullifiers,
    which is transmitted as a Nullifier Event.
  """

  @spec execute(node_id, {back, Noun.t()}, id) :: :ok
        when id: binary(),
             node_id: String.t(),
             back: backend()
  def execute(node_id, {backend, tx_code}, id) do
    if backend == :shard_storage do
      execute_shard_storage(node_id, tx_code, id, backend)
    else
      # For non-shard backends
      time = Storage.current_time(node_id)

      scry =
        fn list ->
          if list do
            with [id, key] <- list |> Noun.list_nock_to_erlang(),
                 {:ok, value} <-
                   (case backend do
                      {:read_only, _pid} ->
                        Storage.read(
                          node_id,
                          {time, key |> Noun.list_nock_to_erlang()}
                        )

                      _ ->
                        Ordering.read(
                          node_id,
                          {id, key |> Noun.list_nock_to_erlang()}
                        )
                    end) do
              {:ok, value}
            else
              _ -> :error
            end
          else
            :error
          end
        end

      env = %Nock{scry_function: scry}

      vm_result = vm_execute(tx_code, env, id)
      result_event(id, vm_result, node_id, backend)

      res =
        with {:ok, vm_res} <- vm_result,
             {:ok, backend_res} <-
               backend_logic(backend, node_id, id, vm_res, time: time) do
          {:ok, backend_res}
        else
          _e ->
            empty_write(backend, node_id, id)

            :error
        end

      complete_event(id, res, node_id, backend)
    end
  end

  # Execute transaction using the shard storage backend
  @spec execute_shard_storage(
          String.t(),
          Noun.t(),
          binary(),
          backend()
        ) :: :ok
  defp execute_shard_storage(node_id, tx_code, id, backend) do
    # For shard storage, first extract reservations
    case vm_execute_stage1(tx_code) do
      {:ok, {stage_2_tx, reservations}} ->
        case parse_reservations(reservations) do
          {:ok, parsed_reservations} ->
            # Request reservations from Ordering
            case Ordering.request_reservations(
                   node_id,
                   id,
                   parsed_reservations
                 ) do
              {:ok, {height, shard_pids}} ->
                # Update scry function with height
                # Nock VM passes the evaluated key noun directly to the scry function
                shard_scry =
                  fn key_noun ->
                    binary_key = Noun.atom_integer_to_binary(key_noun)

                    case Map.fetch(shard_pids, binary_key) do
                      {:ok, pid} ->
                        case Shard.read(
                               pid,
                               binary_key,
                               height
                             ) do
                          {:ok, value} -> {:ok, value}
                          :absent -> :absent
                          _ -> :error
                        end

                      :error ->
                        :error
                    end
                  end

                shard_env = %Nock{scry_function: shard_scry}

                # Execute the rest with reservations
                vm_result = vm_execute_stage2(stage_2_tx, id, shard_env)
                result_event(id, vm_result, node_id, backend)

                # Process the result and handle completion/failure notification
                res =
                  case vm_result do
                    {:ok, vm_res} ->
                      # Try to store the result using shard logic
                      case shard_storage_logic(
                             node_id,
                             id,
                             vm_res,
                             height,
                             shard_pids
                           ) do
                        {:ok, backend_res} ->
                          Ordering.transaction_completed(
                            node_id,
                            id
                          )

                          {:ok, backend_res}

                        :error ->
                          # Shard logic failed
                          # Do not call transaction_failed here, it was done in shard_storage_logic
                          :error
                      end

                    :vm_error ->
                      # VM stage 2 failed
                      Logging.log_event(
                        node_id,
                        :error,
                        "Transaction #{inspect(id)} failed: VM execution stage 2 error."
                      )

                      Ordering.transaction_failed(
                        node_id,
                        id
                      )

                      :error
                  end

                # end case vm_result

                # Notify Executor about the final result (:ok/:error)
                complete_event(id, res, node_id, backend)

                :ok

              {:error, reason} ->
                # Reservation failed
                Logging.log_event(
                  node_id,
                  :error,
                  "Transaction #{inspect(id)} failed: Could not acquire reservations. Reason: #{inspect(reason)}"
                )

                # No need to call transaction_failed here
                # as Ordering handles failed reservation attempts internally.
                # Tell mempool VM step failed
                result_event(id, :vm_error, node_id, backend)
                # Tell executor overall failed
                complete_event(id, :error, node_id, backend)
                :ok
            end

          {:error, reason} ->
            # Invalid reservation format
            # No need to call transaction_failed here
            # as no reservations were successfully acquired
            Logging.log_event(
              node_id,
              :error,
              "Invalid reservation format: #{inspect(reason)}"
            )

            result_event(id, :vm_error, node_id, backend)
            complete_event(id, :error, node_id, backend)
        end

      _error ->
        # Stage 1 execution failed
        # No need to call transaction_failed here
        # as no reservations were successfully acquired
        result_event(id, :vm_error, node_id, backend)
        complete_event(id, :error, node_id, backend)
    end
  end

  ############################################################
  #                       VM Execution                       #
  ############################################################

  @spec vm_execute(Noun.t(), Nock.t(), binary()) ::
          {:ok, Noun.t()} | :vm_error
  defp vm_execute(tx_code, env, id) do
    with {:ok, code} <- cue_when_atom(tx_code),
         {:ok, [_ | stage_2_tx]} <- nock(code, [9, 2, 0 | 1], env),
         {:ok, ordered_tx} <- nock(stage_2_tx, [10, [6, 1 | id], 0 | 1], env),
         {:ok, result} <- nock(ordered_tx, [9, 2, 0 | 1], env) do
      {:ok, result}
    else
      _e -> :vm_error
    end
  end

  # First stage of VM execution - extracts reservations
  @spec vm_execute_stage1(Noun.t()) ::
          {:ok, {Noun.t(), Noun.t()}} | :vm_error
  defp vm_execute_stage1(tx_code) do
    with {:ok, code} <- cue_when_atom(tx_code),
         {:ok, [reservations | stage_2_tx]} <-
           nock(code, [9, 2, 0 | 1], %Nock{}) do
      {:ok, {stage_2_tx, reservations}}
    else
      _e -> :vm_error
    end
  end

  # Second stage of VM execution - called after reservation acquisition
  @spec vm_execute_stage2(Noun.t(), binary(), Nock.t()) ::
          {:ok, Noun.t()} | :vm_error
  defp vm_execute_stage2(stage_2_tx, id, env) do
    with {:ok, ordered_tx} <- nock(stage_2_tx, [10, [6, 1 | id], 0 | 1], env),
         {:ok, result} <- nock(ordered_tx, [9, 2, 0 | 1], env) do
      {:ok, result}
    else
      _e -> :vm_error
    end
  end

  @doc """
  I parse the reservations list from the transaction, expecting an improper list structure.

  I return a list of read/write access requests for specific keys.
  I handle formats like `[[0 | key_a] | [1 | key_b]]` or `[0 | key_a]`.
  """
  @spec parse_reservations(Noun.t()) ::
          {:ok, [{:read | :write, binary()}]} | {:error, atom()}
  def parse_reservations(reservations) do
    do_parse_reservations(reservations, [])
  end

  # Helper for parsing reservation lists recursively
  @spec do_parse_reservations(Noun.t(), [{:read | :write, binary()}]) ::
          {:ok, [{:read | :write, binary()}]} | {:error, atom()}
  defp do_parse_reservations(noun, acc) when Noun.is_noun_zero(noun) do
    {:ok, acc}
  end

  defp do_parse_reservations(noun, acc) do
    case noun do
      # --- Recursive case: [ [type | key] | rest ] ---
      [[type_num | key_noun] = head | rest] ->
        case process_reservation_pair(type_num, key_noun) do
          {:ok, reservation} ->
            do_parse_reservations(rest, [reservation | acc])

          {:error, reason} ->
            {:error,
             {:invalid_reservation_pair_format,
              {reason, Noun.condensed_print(head)}}}
        end

      # --- Base case: Single pair [type | key] (end of improper list) ---
      [type_num | key_noun] = pair ->
        case process_reservation_pair(type_num, key_noun) do
          {:ok, reservation} ->
            {:ok, Enum.reverse([reservation | acc])}

          {:error, reason} ->
            {:error,
             {:invalid_reservation_pair_format,
              {reason, Noun.condensed_print(pair)}}}
        end

      # --- Invalid format ---
      _invalid ->
        {:error, :invalid_reservation_format}
    end
  end

  # Processes a single [type | key] pair from the reservation list
  @spec process_reservation_pair(Noun.t(), Noun.t()) ::
          {:ok, {:read | :write, binary()}} | {:error, atom}
  defp process_reservation_pair(type_num, key_noun) do
    key_bin = Noun.atom_integer_to_binary(key_noun)

    case type_num do
      0 -> {:ok, {:read, key_bin}}
      1 -> {:ok, {:write, key_bin}}
      _ -> {:error, :invalid_reservation_type}
    end
  end

  @spec cue_when_atom(Noun.t()) :: :error | {:ok, Noun.t()}
  defp cue_when_atom(tx_code) when Noun.is_noun_atom(tx_code) do
    Noun.Jam.cue(tx_code)
  end

  defp cue_when_atom(tx_code) do
    {:ok, tx_code}
  end

  ############################################################
  #                     Backend Execution                    #
  ############################################################

  @spec backend_logic(backend(), String.t(), binary(), Noun.t(), list()) ::
          :error | {:ok, any()}
  defp backend_logic(:debug_term_storage, node_id, id, vm_res, _opts) do
    store_value(node_id, id, vm_res)
  end

  defp backend_logic({:read_only, pid}, _node_id, _id, vm_res, opts) do
    send_value(vm_res, pid, opts)
  end

  defp backend_logic(:debug_bloblike, node_id, id, vm_res, _opts) do
    blob_store(node_id, id, vm_res)
  end

  defp backend_logic(:transparent_resource, node_id, id, vm_res, _opts) do
    transparent_resource_tx(node_id, id, vm_res)
  end

  defp backend_logic(:cairo_resource, node_id, id, vm_res, _opts) do
    cairo_resource_tx(node_id, id, vm_res)
  end

  defp backend_logic(:shard_storage, _node_id, _id, _vm_res, _opts) do
    # This is handled directly in execute/3 when requesting reservations
    {:error, :unexpected_backend_logic_call}
  end

  # Handle shard storage transactions after vm execution
  # Expects result to be an improper list of [key | value] pairs.
  @spec shard_storage_logic(String.t(), binary(), Noun.t(), integer(), %{
          binary() => pid()
        }) ::
          {:ok, any()} | :error
  defp shard_storage_logic(node_id, id, result_noun, height, shard_pids) do
    # Parse the improper list of write operations
    case parse_writes(result_noun) do
      {:ok, kvlist} ->
        # Try writing to shards, track failures
        write_results =
          Enum.map(kvlist, fn {k_noun, v_noun} ->
            key_bin = Noun.atom_integer_to_binary(k_noun)

            case Map.fetch(shard_pids, key_bin) do
              {:ok, pid} ->
                case Shard.write(pid, key_bin, v_noun, height) do
                  :ok ->
                    {:ok, key_bin}

                  {:error, reason} ->
                    {:error, {key_bin, :write_failed, reason}}
                end

              :error ->
                # Key not found in reserved pids (shouldn't happen if reservation succeeded)
                {:error, {key_bin, :pid_not_found}}
            end
          end)

        # Check if any writes failed
        failed_writes =
          Enum.filter(write_results, fn {status, _} -> status == :error end)

        if Enum.empty?(failed_writes) do
          # All writes succeeded
          # Return original noun on success
          {:ok, result_noun}
        else
          # Log detailed errors for each failure
          Enum.each(failed_writes, fn
            {:error, {key, :write_failed, reason}} ->
              Logging.log_event(
                node_id,
                :error,
                "Shard write failed for tx #{inspect(id)}, key #{inspect(key)}: Write error - #{inspect(reason)}"
              )

            {:error, {key, :pid_not_found}} ->
              Logging.log_event(
                node_id,
                :error,
                "Shard write failed for tx #{inspect(id)}, key #{inspect(key)}: Shard PID not found in reservations"
              )
          end)

          # Notify Ordering transaction failed - IMPORTANT
          Ordering.transaction_failed(node_id, id)
          :error
        end
    end

    # end case parse_writes
  end

  # Helper to parse an improper list of [key | value] writes.
  @spec parse_writes(Noun.t()) ::
          {:ok, [{Noun.t(), Noun.t()}]} | {:error, atom}
  defp parse_writes(noun) do
    do_parse_writes(noun, [])
  end

  @spec do_parse_writes(Noun.t(), [{Noun.t(), Noun.t()}]) ::
          {:ok, [{Noun.t(), Noun.t()}]} | {:error, atom}
  defp do_parse_writes(noun, acc) when Noun.is_noun_zero(noun) do
    {:ok, acc}
  end

  defp do_parse_writes(noun, acc) do
    case noun do
      # Recursive case: [ [key | value] | rest ]
      [[key_noun | value_noun] = head | rest] ->
        if Noun.is_noun_atom(key_noun) and not is_list(value_noun) do
          do_parse_writes(rest, [{key_noun, value_noun} | acc])
        else
          {:error, {:invalid_write_pair_format, Noun.condensed_print(head)}}
        end

      # Base case: Single write [key | value] or improper list end
      [key_noun | value_noun] = pair ->
        # Basic validation similar to the recursive case
        if Noun.is_noun_atom(key_noun) and not is_list(value_noun) do
          {:ok, Enum.reverse([{key_noun, value_noun} | acc])}
        else
          {:error, {:invalid_write_pair_format, Noun.condensed_print(pair)}}
        end

      _ ->
        {:error, {:invalid_write_format, Noun.condensed_print(noun)}}
    end
  end

  @spec transparent_resource_tx(String.t(), binary(), Noun.t()) ::
          {:ok, any} | :error
  defp transparent_resource_tx(node_id, id, result) do
    storage_checks = fn tx -> storage_check?(node_id, id, tx) end
    verify_tx_root = fn tx -> verify_tx_root(node_id, tx) end

    verify_options = [
      double_insertion_closure: storage_checks,
      root_closure: verify_tx_root
    ]

    with {:ok, tx} <- TransparentResource.Transaction.from_noun(result),
         true <- TransparentResource.Transaction.verify(tx, verify_options) do
      map =
        for action <- tx.actions,
            reduce: %{
              commitments: MapSet.new(),
              nullifiers: MapSet.new(),
              blobs: []
            } do
          %{commitments: cms, nullifiers: nlfs, blobs: blobs} ->
            %{
              commitments: MapSet.union(cms, action.commitments),
              nullifiers: MapSet.union(nlfs, action.nullifiers),
              blobs:
                for {key, {value, bool}} <- action.app_data, reduce: blobs do
                  acc ->
                    if Noun.equal?(bool, 0) and
                         :crypto.hash(:sha256, Noun.Jam.jam(value)) == key do
                      [{key, value} | acc]
                    else
                      acc
                    end
                end
            }
        end

      old_cms =
        case Ordering.read(node_id, {id, anoma_keyspace("commitments")}) do
          :absent -> MapSet.new()
          {:ok, res} -> res
        end

      writes =
        for {key, value} <- map.blobs,
            reduce: [
              {anoma_keyspace("anchor"),
               value(
                 MapSet.union(
                   map.commitments,
                   old_cms
                 )
               )}
            ] do
          acc -> [{["anoma", "blob", key], value} | acc]
        end

      Ordering.add(
        node_id,
        {id,
         %{
           append: [
             {anoma_keyspace("nullifiers"), map.nullifiers},
             {anoma_keyspace("commitments"), map.commitments}
           ],
           write: writes
         }}
      )

      transparent_rm_event(map.commitments, map.nullifiers, node_id)

      {:ok, tx}
    else
      e ->
        unless e == :error do
          Logging.log_event(
            node_id,
            :error,
            "Transaction verification failed. Reason: #{inspect(e)}"
          )
        end

        :error
    end
  end

  @spec verify_tx_root(String.t(), TTransaction.t()) ::
          true | {:error, String.t()}
  defp verify_tx_root(node_id, trans = %TTransaction{}) do
    with true <- commitments_exist_in_roots(node_id, trans) do
      true
    else
      {:error, msg} ->
        {:error,
         "Nullified resources are not committed at latest root: " <> msg}
    end
  end

  @spec storage_check?(String.t(), binary(), TTransaction.t()) ::
          true | {:error, String.t()}
  defp storage_check?(node_id, id, trans) do
    stored_commitments =
      Ordering.read(node_id, {id, anoma_keyspace("commitments")})

    stored_nullifiers =
      Ordering.read(node_id, {id, anoma_keyspace("nullifiers")})

    # TODO improve error messages
    cond do
      any_nullifiers_already_exist?(stored_nullifiers, trans) ->
        {:error, "A submitted nullifier already exists in storage"}

      any_commitments_already_exist?(stored_commitments, trans) ->
        {:error, "A submitted commitment already exists in storage"}

      true ->
        true
    end
  end

  @spec any_nullifiers_already_exist?(
          {:ok, MapSet.t(TResource.nullifier())} | :absent,
          TTransaction.t()
        ) :: boolean()
  defp any_nullifiers_already_exist?(:absent, _) do
    false
  end

  defp any_nullifiers_already_exist?(
         {:ok, stored_nulls},
         trans = %TTransaction{}
       ) do
    nullifiers = TTransaction.nullifiers(trans)
    Enum.any?(nullifiers, &MapSet.member?(stored_nulls, &1))
  end

  @spec any_commitments_already_exist?(
          {:ok, MapSet.t(TResource.commitment())} | :absent,
          TTransaction.t()
        ) :: boolean()
  defp any_commitments_already_exist?(:absent, _) do
    false
  end

  defp any_commitments_already_exist?(
         {:ok, stored_comms},
         trans = %TTransaction{}
       ) do
    commitments = TTransaction.commitments(trans)
    Enum.any?(commitments, &MapSet.member?(stored_comms, &1))
  end

  @spec commitments_exist_in_roots(String.t(), TTransaction.t()) ::
          true | {:error, String.t()}
  defp commitments_exist_in_roots(
         node_id,
         trans = %TTransaction{}
       ) do
    latest_root_time =
      for root <- trans.roots, reduce: 0 do
        time ->
          with {:atomic, [{_, {height, _}, ^root}]} <-
                 :mnesia.transaction(fn ->
                   :mnesia.match_object(
                     {Storage.values_table(node_id),
                      {:_, anoma_keyspace("anchor")}, root}
                   )
                 end) do
            if height > time do
              height
            else
              time
            end
          else
            {:atomic, []} -> time
          end
      end

    action_nullifiers = TTransaction.nullifiers(trans)

    if latest_root_time > 0 do
      {:ok, root_coms} =
        Storage.read(
          node_id,
          {latest_root_time, anoma_keyspace("commitments")}
        )

      commitments =
        for <<"NF_", rest::binary>> <- action_nullifiers,
            reduce: MapSet.new([]) do
          cm_set ->
            if ephemeral?(rest) do
              cm_set
            else
              MapSet.put(cm_set, "CM_" <> rest)
            end
        end

      case commitments |> MapSet.subset?(root_coms) do
        true ->
          true

        _ ->
          {:error,
           "commitments absent: #{inspect(MapSet.difference(commitments, root_coms) |> Enum.to_list())}"}
      end
    else
      if Enum.all?(action_nullifiers, fn <<"NF_", rest::binary>> ->
           ephemeral?(rest)
         end) do
        true
      else
        {:error, "not all resources ephemeral for initiating transaction"}
      end
    end
  end

  @spec ephemeral?(Noun.noun_atom()) :: boolean()
  defp ephemeral?(jammed_transaction) do
    nock_boolean =
      Noun.Jam.cue(jammed_transaction)
      |> elem(1)
      |> Noun.list_nock_to_erlang_safe()
      |> elem(1)
      |> List.pop_at(2)
      |> elem(0)

    nock_boolean in [0, <<>>, <<0>>, []]
  end

  @spec send_value(Noun.t(), pid(), list()) ::
          {:ok, any()}
  defp send_value(result, reply_to, opts) do
    send(reply_to, {opts[:time], result})
    {:ok, result}
  end

  @spec blob_store(String.t(), binary(), Noun.t()) :: {:ok, any} | :error
  def blob_store(node_id, id, result) do
    key = :crypto.hash(:sha256, :erlang.term_to_binary(result))
    Ordering.write(node_id, {id, [{key, result}]})
    {:ok, key}
  end

  @spec store_value(String.t(), binary(), Noun.t()) :: {:ok, any} | :error
  def store_value(node_id, id, result) do
    with {:ok, list} <- result |> Noun.list_nock_to_erlang_safe(),
         true <-
           Enum.all?(list, fn
             [_ | _] -> true
             _ -> false
           end) do
      Ordering.write(
        node_id,
        {id, list |> Enum.map(fn [k | v] -> {k, v} end)}
      )

      {:ok, list}
    else
      _ -> :error
    end
  end

  @spec empty_write(backend(), String.t(), binary()) :: :ok
  defp empty_write({:read_only, _}, _node_id, _id) do
    :ok
  end

  defp empty_write(:shard_storage, _node_id, _id) do
    :ok
  end

  defp empty_write(_backend, node_id, id) do
    Ordering.write(node_id, {id, []})
  end

  @spec cairo_resource_tx(String.t(), binary(), Noun.t()) ::
          :ok | :error
  defp cairo_resource_tx(node_id, id, result) do
    with {:ok, tx} <- CTransaction.from_noun(result),
         true <- Anoma.RM.Transaction.verify(tx),
         true <- root_existence_check(tx, node_id, id),
         # No need to check the commitment existence
         true <- nullifier_existence_check(tx, node_id, id) do
      {ct, append_roots} =
        case Ordering.read(node_id, {id, anoma_keyspace("cairo_ct")}) do
          :absent ->
            {CTransaction.cm_tree(),
             MapSet.new([Anoma.Constants.default_cairo_rm_root()])}

          {:ok, val} ->
            {val, MapSet.new()}
        end

      {ct_new, anchor} =
        CommitmentTree.add(ct, tx.commitments)

      ciphertexts = tx |> CTransaction.get_cipher_texts() |> MapSet.new()

      Ordering.add(
        node_id,
        {id,
         %{
           append: [
             {anoma_keyspace("cairo_nullifiers"), MapSet.new(tx.nullifiers)},
             {anoma_keyspace("cairo_roots"),
              MapSet.put(append_roots, anchor)},
             {anoma_keyspace("cairo_ciphertexts"), ciphertexts}
           ],
           write: [{anoma_keyspace("cairo_ct"), ct_new}]
         }}
      )

      # Get ciphertext from tx
      _ciphertext = CTransaction.get_cipher_texts(tx)

      # TODO: Store ciphertext

      cairo_rm_event(
        MapSet.new(tx.commitments),
        MapSet.new(tx.nullifiers),
        node_id
      )

      {:ok, tx}
    else
      e ->
        unless e == :error do
          Logging.log_event(
            node_id,
            :error,
            "Transaction verification failed. Reason: #{inspect(e)}"
          )
        end

        :error
    end
  end

  @spec nullifier_existence_check(CTransaction.t(), String.t(), binary()) ::
          true | {:error, String.t()}
  def nullifier_existence_check(transaction, node_id, id) do
    with {:ok, stored_nullifiers} <-
           Ordering.read(node_id, {id, anoma_keyspace("cairo_nullifiers")}) do
      if Enum.any?(
           transaction.nullifiers,
           &MapSet.member?(stored_nullifiers, &1)
         ) do
        {:error, "A submitted nullifier already exists in storage"}
      else
        true
      end
    else
      # stored_nullifiers is empty
      _ -> true
    end
  end

  @spec root_existence_check(CTransaction.t(), String.t(), binary()) ::
          true | {:error, String.t()}
  def root_existence_check(transaction, node_id, id) do
    stored_roots =
      case Ordering.read(node_id, {id, anoma_keyspace("cairo_roots")}) do
        :absent -> MapSet.new([Anoma.Constants.default_cairo_rm_root()])
        {:ok, val} -> val
      end

    Enum.all?(transaction.roots, &MapSet.member?(stored_roots, &1)) or
      {:error, "A submitted root dose not exist in storage"}
  end

  ############################################################
  #                        Helpers                           #
  ############################################################

  @spec complete_event(
          String.t(),
          :error | {:ok, any()},
          String.t(),
          backend()
        ) :: :ok
  defp complete_event(id, result, node_id, backend) do
    event =
      Node.Event.new_with_body(node_id, %__MODULE__.CompleteEvent{
        tx_id: id,
        tx_result: result
      })

    event(backend, event)
  end

  @spec result_event(String.t(), any(), String.t(), backend()) :: :ok
  defp result_event(id, result, node_id, backend) do
    event =
      Node.Event.new_with_body(node_id, %__MODULE__.ResultEvent{
        tx_id: id,
        vm_result: result
      })

    event(backend, event)
  end

  @spec transparent_rm_event(
          MapSet.t(binary()),
          MapSet.t(binary()),
          String.t()
        ) :: :ok
  defp transparent_rm_event(cms, nlfs, node_id) do
    event =
      Node.Event.new_with_body(node_id, %__MODULE__.TRMEvent{
        commitments: cms,
        nullifiers: nlfs
      })

    EventBroker.event(event)
  end

  @spec cairo_rm_event(
          MapSet.t(binary()),
          MapSet.t(binary()),
          String.t()
        ) :: :ok
  defp cairo_rm_event(cms, nlfs, node_id) do
    event =
      Node.Event.new_with_body(node_id, %__MODULE__.SRMEvent{
        commitments: cms,
        nullifiers: nlfs
      })

    EventBroker.event(event)
  end

  @spec event(backend(), EventBroker.Event.t()) :: :ok
  defp event({:read_only, _}, _event) do
    :ok
  end

  defp event(_backend, event) do
    EventBroker.event(event)
  end

  @doc """
  I am the commitment accumulator add function for the transparent resource
  machine.

  Given the commitment set, I add a commitment to it.
  """

  @spec add(MapSet.t(), binary()) :: MapSet.t()
  def add(acc, cm) do
    MapSet.put(acc, cm)
  end

  @doc """
  I am the commitment accumulator witness function for the transparent
  resource machine.

  Given the commitment set and a commitment, I return the original set if
  the commitment is a member of the former. Otherwise, I return nil
  """

  @spec witness(MapSet.t(), binary()) :: MapSet.t() | nil
  def witness(acc, cm) do
    if MapSet.member?(acc, cm) do
      acc
    end
  end

  @doc """
  I am the commitment accumulator value function for the transparent
  resource machine.

  Given the commitment set, I turn it to binary and then hash it using
  sha-256.
  """

  @spec value(MapSet.t()) :: binary()
  def value(acc) do
    :crypto.hash(:sha256, :erlang.term_to_binary(acc))
  end

  @doc """
  I am the commitment accumulator verify function for the transparent
  resource machine.

  Given the commitment, a witness (i.e. a set) and a commitment value, I
  output true iff the witness's value is the same as the provided value and
  the commitment is indeed in the set.
  """

  @spec verify(binary(), MapSet.t(), binary()) :: bool()
  def verify(cm, w, val) do
    val == value(w) and MapSet.member?(w, cm)
  end

  @spec anoma_keyspace(String.t()) :: list(String.t())
  defp anoma_keyspace(key) do
    ["anoma", key]
  end
end
