defmodule Anoma.Node.Transaction.ShardSupervisor do
  @moduledoc """
  I am the supervisor for `Anoma.Node.Transaction.Shard` processes.

  I start and manage individual `Shard` processes according to a strategy and
  schema provided in my arguments. I create and maintain a named ETS table
  (`:shard_key_map`) mapping keys to the registered name
  (`{:via, Registry, {Anoma.Node, {Shard, key}}}`) of the `Shard` process
  responsible for that key. The actual lookup of keys is handled by the
  `Anoma.Node.Transaction.Ordering`.

  ### Key Concepts

  - **Supervisor Args:** Keyword list including `:strategy` and `:schema`.
  - **Strategy:** Determines how shards are created (e.g., `:one_per_key`).
  - **Schema:** Defines the initial keys and their starting values.
  - **ETS Table:** `:shard_key_map` for key -> shard name lookup (used by Anoma.Node.Transaction.Ordering).

  ### Public API

  - `start_link/1`: I start the supervisor.
  - `get_shard_key_map/1`: I return the ETS table for the shard key map for the given node ID.
  """

  use Supervisor

  alias Anoma.Node.Registry
  alias Anoma.Node.Transaction.Shard

  require Logger

  # ETS table name for storing the shard key map
  @shard_key_map_ets :shard_key_map

  ############################################################
  #                       Types                              #
  ############################################################

  @typedoc "I represent a key managed by a shard."
  @type key_t :: binary()

  @typedoc "I represent the initial value associated with a key in a shard."
  @type initial_value_t :: any()

  @typedoc """
  I am the schema defining the keys and their initial values for shards.
  For the `:one_per_key` strategy, I expect a list containing either `key` binaries
  or `{key, initial_value}` tuples. If only a key is provided, there is no
  initial value.
  """
  @type schema_t :: [key_t() | {key_t(), initial_value_t()}]

  @typedoc """
  I am the sharding strategy.
  Currently, I only support `:one_per_key`.
  """
  @type strategy_t :: :one_per_key

  @typedoc """
  I am the type of the arguments that the ShardSupervisor expects at startup.
  I require `:node_id` and optionally `:strategy` and `:schema` keys.
  """
  @type supervisor_args_t :: [
          node_id: String.t(),
          strategy: strategy_t() | nil,
          schema: schema_t() | nil
        ]

  @typedoc """
  I am the type of the arguments that the Shard process expects.
  I am not explicitly used, but this may be useful to know.
  """
  @type shard_args_t :: [
          id: key_t(),
          initial_kv: %{key_t() => initial_value_t()}
        ]

  ############################################################
  #                 Supervisor Implementation                #
  ############################################################

  @doc """
  I am the start_link function for the ShardSupervisor.

  I start and link the supervisor process under the current supervision tree,
  registering myself locally using a node-specific name.
  """
  @spec start_link(args :: supervisor_args_t()) :: Supervisor.on_start()
  def start_link(args) do
    name = Registry.via(args[:node_id], __MODULE__)
    Supervisor.start_link(__MODULE__, args, name: name)
  end

  @impl true
  @doc """
  I am the Supervisor initialization callback.

  I set the process label. If valid :strategy and :schema are provided,
  I calculate the full key->name mapping and shard child specs,
  populate a named ETS table with key -> shard ID mappings, and
  start all configured `Shard` children using a `:one_for_one` strategy.
  """
  @spec init(args :: supervisor_args_t()) ::
          {:ok, {Supervisor.sup_flags(), [Supervisor.child_spec()]}}
  def init(args) do
    node_id = Keyword.fetch!(args, :node_id)
    Process.set_label({__MODULE__, node_id})

    try do
      # Process schema only if strategy and schema are validly provided
      {shard_child_specs, key_to_id_map} =
        process_schema(
          node_id,
          Keyword.get(args, :strategy),
          Keyword.get(args, :schema)
        )

      # Create ETS table if shards were generated
      case create_and_populate_ets_table(key_to_id_map, node_id) do
        {:ok, _ets_tid} ->
          # Simply initialize the supervisor with the child specs
          Supervisor.init(shard_child_specs, strategy: :one_for_one)

        :no_shards ->
          # No shards configured or generated, start no children
          Supervisor.init([], strategy: :one_for_one)

        {:error, _reason} ->
          {:stop, :ets_table_creation_failed}
      end
    rescue
      e ->
        # Reraise the exception to ensure supervisor termination/restart according to strategy
        reraise e, __STACKTRACE__
    end
  end

  ############################################################
  #                    Private Helpers                       #
  ############################################################

  # Processes the schema based on the strategy to generate shard child specs
  # and a map of key -> shard_id.
  @spec process_schema(
          node_id :: String.t(),
          strategy :: strategy_t() | nil,
          schema :: schema_t() | nil
        ) ::
          {
            [Supervisor.child_spec()],
            %{key_t() => atom()}
          }
  defp process_schema(node_id, :one_per_key, schema) when is_list(schema) do
    Enum.reduce(schema, {[], %{}}, fn schema_entry, {specs_acc, map_acc} ->
      case schema_entry do
        # Case 1: Schema entry is {key, initial_value}
        {key, initial_value} when is_binary(key) ->
          shard_id = String.to_atom(key)

          shard_args = [
            node_id: node_id,
            id: shard_id,
            initial_kv: %{key => initial_value}
          ]

          child_spec = %{
            id: shard_id,
            start: {Shard, :start_link, [shard_args]}
          }

          {[child_spec | specs_acc], Map.put(map_acc, key, shard_id)}

        # Case 2: Schema entry is just a key
        key when is_binary(key) ->
          shard_id = String.to_atom(key)

          shard_args = [
            node_id: node_id,
            id: shard_id,
            initial_kv: %{}
          ]

          child_spec = %{
            id: shard_id,
            start: {Shard, :start_link, [shard_args]}
          }

          {[child_spec | specs_acc], Map.put(map_acc, key, shard_id)}

        _invalid_entry ->
          # Skip invalid entry
          {specs_acc, map_acc}
      end
    end)
  end

  # Cases where strategy/schema are nil or invalid
  defp process_schema(_node_id, _strategy, _schema) do
    {[], %{}}
  end

  @doc """
  I return the ETS table for the shard key map for the given node ID.
  """
  @spec get_shard_key_map(node_id :: String.t()) :: :ets.tab() | nil
  def get_shard_key_map(node_id) do
    table_name = :"#{@shard_key_map_ets}_#{node_id}"

    case :ets.whereis(table_name) do
      :undefined -> nil
      tid -> tid
    end
  end

  # Creates a new ETS table and populates it with the key -> shard_id mapping.
  # Returns {:ok, tid} on success, :no_shards if the map is empty, or {:error, reason}.
  @spec create_and_populate_ets_table(
          map :: %{key_t() => atom()},
          node_id :: String.t()
        ) ::
          {:ok, :ets.tab()} | :no_shards | {:error, any()}
  defp create_and_populate_ets_table(key_to_id_map, node_id)
       when map_size(key_to_id_map) > 0 do
    try do
      # Create a named ETS table for this node
      table_name = :"#{@shard_key_map_ets}_#{node_id}"

      # Check if table already exists, if so delete it
      case :ets.whereis(table_name) do
        :undefined -> :ok
        _ -> :ets.delete(table_name)
      end

      ets_tid =
        :ets.new(table_name, [
          :set,
          :public,
          :named_table
        ])

      Enum.each(key_to_id_map, fn {key, shard_id} ->
        :ets.insert(ets_tid, {key, shard_id})
      end)

      {:ok, ets_tid}
    catch
      kind, reason ->
        stack = __STACKTRACE__
        {:error, {kind, reason, stack}}
    end
  end

  defp create_and_populate_ets_table(_empty_map, _node_id) do
    :no_shards
  end
end
