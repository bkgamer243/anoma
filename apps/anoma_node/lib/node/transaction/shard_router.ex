defmodule Anoma.Node.Transaction.ShardRouter do
  @moduledoc """
  I am a GenServer responsible for routing requests to the correct Shard.

  My primary responsibility is to look up the registered name
  of the shard responsible for a given key using the shared `:shard_key_map` ETS table.
  This table is populated by the `Anoma.Node.Transaction.ShardSupervisor`.

  ### Public API

  - `start_link/1`: I start the router GenServer.
  - `get_shard_label/1`: I look up the shard label for a key.
  """

  use GenServer

  alias Anoma.Node.Registry

  require Logger

  ############################################################
  #                       Types                              #
  ############################################################

  @typedoc "I represent a key managed by a shard."
  @type key_t :: binary()

  @typedoc "I am the arguments passed to start_link. Requires node_id."
  @type args_t :: [node_id: String.t()]

  ############################################################
  #                       Constants                          #
  ############################################################

  # The ETS table is created and owned by the ShardSupervisor, but I read from it.
  @ets_table_name :shard_key_map

  ############################################################
  #                    Public Client API                     #
  ############################################################

  @doc """
  I start the ShardRouter GenServer.

  I link the process and register it locally using a node-specific name.
  """
  @spec start_link(args :: args_t()) :: GenServer.on_start()
  def start_link(args) do
    name = Registry.via(args[:node_id], __MODULE__)
    GenServer.start_link(__MODULE__, args, name: name)
  end

  @doc """
  I retrieve the label (atom) of the shard responsible for the given key.

  I make a synchronous call to my GenServer process to perform the lookup
  in the ETS table (`:shard_key_map`).
  I return `{:ok, shard_label}` if the key is found, otherwise `:error`.
  """
  @spec get_shard_label(key :: key_t()) ::
          {:ok, atom()} | :error
  def get_shard_label(key) when is_binary(key) do
    GenServer.call(__MODULE__, {:get_shard_label, key})
  end

  ############################################################
  #                    GenServer Callbacks                   #
  ############################################################

  @impl true
  @doc """
  I initialize the ShardRouter GenServer.
  """
  @spec init(args :: args_t()) :: {:ok, :no_state}
  def init(args) do
    node_id = Keyword.fetch!(args, :node_id)
    Process.set_label({__MODULE__, node_id})
    Logger.debug("ShardRouter #{node_id} started.")
    {:ok, :no_state}
  end

  @impl true
  @doc """
  I handle the `:get_shard_label` request.

  I perform the lookup in the shared `:shard_key_map` ETS table.
  I return the shard label (atom) stored in the table.
  """
  @spec handle_call({:get_shard_label, key_t()}, GenServer.from(), :no_state) ::
          {:reply, {:ok, atom()} | :error, :no_state}
  def handle_call({:get_shard_label, key}, _from, state) do
    try do
      lookup_result = :ets.lookup(@ets_table_name, key)

      reply =
        case lookup_result do
          [{^key, shard_id}] -> {:ok, shard_id}
          [] -> :error
        end

      {:reply, reply, state}
    catch
      kind, reason ->
        Logger.error(
          "ShardRouter: Error during ETS lookup for key #{inspect(key)} - Kind: #{kind}, Reason: #{inspect(reason)}, Stacktrace: #{inspect(__STACKTRACE__)}"
        )

        {:reply, :error, state}
    end
  end

  @impl true
  @doc """
  I clean up the ETS table when I (the router) terminate.
  """
  @spec terminate(reason :: any(), state :: :no_state) :: :ok
  def terminate(_reason, _state) do
    :ets.delete(@ets_table_name)
    :ok
  end
end
