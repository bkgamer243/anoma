defmodule Anoma.Node.Transaction.ShardSupervisor do
  @moduledoc """
  I am the supervisor for `Anoma.Node.Transaction.Shard` processes.

  I start and manage individual `Shard` processes according to a strategy and
  schema provided in my arguments. I create and maintain a named ETS table
  (`:shard_key_map`) mapping keys to the registered name
  (`{:via, Registry, {Anoma.Node, {Shard, key}}}`) of the `Shard` process
  responsible for that key. The actual lookup of keys is handled by the
  `Anoma.Node.Transaction.ShardRouter`.

  ### Key Concepts

  - **Supervisor Args:** Keyword list including `:strategy` and `:schema`.
  - **Strategy:** Determines how shards are created (e.g., `:one_per_key`).
  - **Schema:** Defines the initial keys and their starting values.
  - **ETS Table:** `:shard_key_map` for key -> shard name lookup (used by ShardRouter).

  ### Public API

  - `start_link/1`: I start the supervisor.
  """

  use Supervisor

  alias Anoma.Node.Registry
  alias Anoma.Node.Transaction.Shard
  alias Anoma.Node.Transaction.ShardRouter

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
  #                       Constants                          #
  ############################################################

  @ets_table_name :shard_key_map

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
  populate the `:shard_key_map` ETS table, and then start the
  `ShardRouter` and all configured `Shard` children using a
  `:one_for_one` strategy.
  """
  @spec init(args :: supervisor_args_t()) ::
          {:ok, {Supervisor.sup_flags(), [Supervisor.child_spec()]}}
  def init(args) do
    node_id = Keyword.fetch!(args, :node_id)
    Process.set_label({__MODULE__, node_id})

    # Process schema only if strategy and schema are validly provided
    {shard_child_specs, key_to_id_map} =
      case {Keyword.get(args, :strategy), Keyword.get(args, :schema)} do
        {:one_per_key, schema} when is_list(schema) ->
          # Iterate schema once to build specs and key->id map
          Enum.reduce(schema, {[], %{}}, fn schema_entry,
                                            {specs_acc, map_acc} ->
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
                shard_args = [node_id: node_id, id: shard_id, initial_kv: %{}]

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

        {nil, _} ->
          {[], %{}}

        {_strategy, nil} ->
          {[], %{}}

        {_invalid_strategy, _} ->
          {[], %{}}
      end

    # Create and populate ETS table (mapping key -> shard_id) if shards were generated
    if map_size(key_to_id_map) > 0 do
      ets_table =
        :ets.new(@ets_table_name, [
          :set,
          :public,
          :named_table,
          read_concurrency: true
        ])

      # Verify table creation/access and log insertions
      if :ets.info(ets_table, :name) == @ets_table_name do
        for {key, shard_id} <- key_to_id_map do
          :ets.insert(ets_table, {key, shard_id})
        end
      end

      # Define router child spec (only needed if shards exist)
      router_child_spec = {ShardRouter, [node_id: node_id]}
      all_children = [router_child_spec | shard_child_specs]

      Supervisor.init(all_children, strategy: :one_for_one)
    else
      # No shards configured or generated, start no children
      Supervisor.init([], strategy: :one_for_one)
    end
  end
end
