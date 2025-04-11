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

  @typedoc "I am the arguments passed to start_link. Requires node_id and tid."
  @type args_t :: [node_id: String.t(), tid: :ets.tab()]

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
  @spec get_shard_label(node_id :: String.t(), key :: key_t()) ::
          {:ok, atom()} | :error
  def get_shard_label(node_id, key) when is_binary(key) do
    target_process = Registry.via(node_id, __MODULE__)
    GenServer.call(target_process, {:get_shard_label, key})
  end

  ############################################################
  #                    GenServer Callbacks                   #
  ############################################################

  @impl true
  @doc """
  I initialize the ShardRouter GenServer.
  """
  @spec init(args :: args_t()) :: {:ok, %{tid: :ets.tab()}}
  def init(args) do
    node_id = Keyword.fetch!(args, :node_id)
    ets_tid = Keyword.fetch!(args, :tid)
    Process.set_label({__MODULE__, node_id})

    Logger.debug(
      "ShardRouter #{node_id} started with ETS tid: #{inspect(ets_tid)}."
    )

    {:ok, %{tid: ets_tid}}
  end

  @impl true
  @doc """
  I handle the `:get_shard_label` request.

  I perform the lookup in the shared ETS table using the `tid` from the state.
  I return the shard label (atom) stored in the table.
  """
  @spec handle_call({:get_shard_label, key_t()}, GenServer.from(), %{
          tid: :ets.tab()
        }) ::
          {:reply, {:ok, atom()} | :error, %{tid: :ets.tab()}}
  def handle_call({:get_shard_label, key}, _from, state = %{tid: ets_tid}) do
    try do
      # Use the tid from the state for the lookup
      lookup_result = :ets.lookup(ets_tid, key)

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
end
