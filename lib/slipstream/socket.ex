defmodule Slipstream.Socket do
  @moduledoc """
  A data structure representing a potential websocket client connection

  This structure closely resembles `t:Phoenix.Socket.t/0`, but is not
  compatible with its functions. All documented functions from this module
  are imported by `use Slipstream`.
  """

  import Kernel, except: [send: 2, pid: 1]

  alias Slipstream.{TelemetryHelper, Socket.Join}
  alias Slipstream.Events

  if Version.match?(System.version(), ">= 1.8.0") do
    @derive {Inspect, only: [:assigns]}
  end

  defstruct [
    :channel_pid,
    :channel_monitor,
    :socket_pid,
    :channel_config,
    :response_headers,
    metadata: %{},
    reconnect_counter: 0,
    joins: %{},
    assigns: %{}
  ]

  @typedoc """
  A socket data structure representing a potential websocket client connection
  """
  @typedoc since: "0.1.0"
  @type t :: %__MODULE__{
          channel_pid: nil | pid(),
          channel_monitor: nil | reference(),
          socket_pid: pid(),
          channel_config: Slipstream.Configuration.t() | nil,
          metadata: %{atom() => String.t() | %{String.t() => String.t()}},
          reconnect_counter: non_neg_integer(),
          assigns: map(),
          joins: %{String.t() => %Join{}}
        }

  @doc false
  @spec new() :: t()
  def new do
    %__MODULE__{
      socket_pid: self(),
      metadata: %{
        socket_id: TelemetryHelper.trace_id(),
        joins: %{}
      }
    }
  end

  @doc """
  Adds key-value pairs to socket assigns

  Behaves the same as `Phoenix.Socket.assign/3`

  ## Examples

      iex> assign(socket, :key, :value)
      iex> assign(socket, key: :value)
  """
  # and indeed the implementation is just about the same as well.
  # we can't defdelegate/2 though because the socket module is different
  # (hence the struct doesn't match)
  @doc since: "0.1.0"
  @spec assign(t(), Keyword.t()) :: t()
  @spec assign(t(), key :: atom(), value :: any()) :: t()
  def assign(%__MODULE__{} = socket, key, value) when is_atom(key) do
    assign(socket, [{key, value}])
  end

  def assign(%__MODULE__{} = socket, attrs)
      when is_list(attrs) or is_map(attrs) do
    %__MODULE__{socket | assigns: Map.merge(socket.assigns, Map.new(attrs))}
  end

  @doc """
  Updates an existing key in the socket assigns

  Raises a `KeyError` if the key is not present in `socket.assigns`.

  `func` should be an 1-arity function which takes the existing value at assign
  `key` and updates it to a new value. The new value will take the old value's
  place in `socket.assigns[key]`.

  This function is a useful alternative to `assign/3` when the key is already
  present in assigns and is a list, map, or similarly malleable data structure.

  ## Examples

      @impl Slipstream
      def handle_cast({:join, topic}, socket) do
        socket =
          socket
          |> update(:topics, &[topic | &1])
          |> join(topic)

        {:noreply, socket}
      end

      @impl Slipstream
      def handle_call({:join, topic}, from, socket) do
        socket =
          socket
          |> update(:join_requests, &Map.put(&1, topic, from))
          |> join(topic)

        # note: not replying here so we can provide a synchronous call to a
        # topic being joined
        {:noreply, socket}
      end

      @impl Slipstream
      def handle_join(topic, response, socket) do
        case Map.fetch(socket.assigns.join_requests, topic) do
          {:ok, from} -> GenServer.reply(from, {:ok, response})
          :error -> :ok
        end

        {:ok, socket}
      end
  """
  # again, can't defdelegate/2 because of the socket module being different
  # but see the `Phoenix.LiveView.update/3` implementation for the original
  # source
  @doc since: "0.5.0"
  @spec update(t(), key :: atom(), func :: (value :: any() -> value :: any())) ::
          t()
  def update(%__MODULE__{assigns: assigns} = socket, key, func)
      when is_atom(key) and is_function(func, 1) do
    case Map.fetch(assigns, key) do
      {:ok, value} -> assign(socket, [{key, func.(value)}])
      :error -> raise KeyError, key: key, term: assigns
    end
  end

  @doc """
  Checks if a channel is currently joined

  ## Examples

      iex> joined?(socket, "room:lobby")
      true
  """
  @doc since: "0.1.0"
  @spec joined?(t(), topic :: String.t()) :: boolean()
  def joined?(%__MODULE__{} = socket, topic) when is_binary(topic) do
    join_status(socket, topic) == :joined
  end

  @doc """
  Checks the status of a join request

  When a join is requested with `Slipstream.join/3`, the join request is
  considered to be in the `:requested` state. Once the topic is successfully
  joined, it is considered `:joined` until closed. If there is a failure to
  join the topic, if the topic crashes, or if the topic is left after being
  joined, the status of the join is considered `:closed`. Finally, if a topic
  has not been requested in a join so far for a socket, the status is `nil`.

  Notably, the status of a join will not automatically change to `:joined` once
  the remote server replies with successful join. Either the join must be
  awaited with `Slipstream.await_join/2` or the status may be checked later
  in the `c:Slipstream.handle_join/3` callback.

  ## Examples

      iex> socket = join(socket, "room:lobby")
      iex> join_status(socket, "room:lobby")
      :requested
      iex> {:ok, socket, _join_response} = await_join(socket, "room:lobby")
      iex> join_status(socket, "room:lobby")
      :joined
  """
  @doc since: "0.1.0"
  @spec join_status(t(), topic :: String.t()) ::
          :requested | :joined | :closed | nil
  def join_status(%__MODULE__{} = socket, topic) when is_binary(topic) do
    case Map.fetch(socket.joins, topic) do
      {:ok, %Join{status: status}} -> status
      :error -> nil
    end
  end

  @doc """
  Checks if a socket is connected to a remote websocket host

  ## Examples

      iex> socket = connect(socket, uri: "ws://example.org")
      iex> socket = await_connect!(socket)
      iex> connected?(socket)
      true
  """
  @doc since: "0.1.0"
  @spec connected?(t()) :: boolean()
  def connected?(%__MODULE__{} = socket),
    do: socket |> channel_pid() |> is_pid()

  @doc """
  Gets the process ID of the connection

  The slipstream implementor module is not the same process as the GenServer
  which interfaces with the remote server for websocket communication. This
  other process, the Slipstream.Connection process, interfaces with the
  low-level WebSocket connection and communicates with the implementor module
  by puassing messages (mostly with `Kernel.send/2`).

  It can be useful to have access to this pid for testing or debugging
  purposes, such as sending a fake disconnect message or for getting state
  with `:sys.get_state/1`.

  ## Examples

      iex> Slipstream.Socket.channel_pid(socket)
      #PID<0.1.2>
  """
  @doc since: "0.1.0"
  @spec channel_pid(t()) :: pid() | nil
  def channel_pid(%__MODULE__{channel_pid: pid}) do
    if is_pid(pid) and Process.alive?(pid), do: pid, else: nil
  end

  ## helper functions for implementing Slipstream

  @doc false
  def send(%__MODULE__{} = socket, message) do
    if pid = channel_pid(socket), do: Kernel.send(pid, message)

    socket
  end

  @doc false
  def call(%__MODULE__{} = socket, message, timeout) do
    if pid = channel_pid(socket) do
      {:ok, GenServer.call(pid, message, timeout)}
    else
      {:error, :not_connected}
    end
  end

  @doc false
  def put_join_config(%__MODULE__{} = socket, topic, params) do
    join = Join.new(topic, params)

    %__MODULE__{socket | joins: Map.put_new(socket.joins, topic, join)}
  end

  # potentially changes a socket by applying an event to it
  @doc false
  @spec apply_event(t(), struct()) :: t()
  def apply_event(socket, event)

  def apply_event(%__MODULE__{} = socket, %Events.ChannelConnected{} = event) do
    socket = TelemetryHelper.conclude_connect(socket, event)
    channel_config = event.config || socket.channel_config

    %{
      socket
      | channel_pid: event.pid,
        channel_monitor:
          monitor_channel(socket.channel_monitor, event.pid, channel_config),
        channel_config: channel_config,
        reconnect_counter: 0
    }
  end

  def apply_event(
        %__MODULE__{} = socket,
        %Events.TopicJoinSucceeded{topic: topic} = event
      ) do
    socket
    |> TelemetryHelper.conclude_join(event)
    |> put_in([Access.key(:joins), topic, Access.key(:status)], :joined)
    |> put_in([Access.key(:joins), topic, Access.key(:rejoin_counter)], 0)
  end

  def apply_event(%__MODULE__{} = socket, %event{topic: topic})
      when event in [
             Events.TopicLeft,
             Events.TopicJoinFailed,
             Events.TopicJoinClosed
           ] do
    put_in(socket, [Access.key(:joins), topic, Access.key(:status)], :closed)
  end

  def apply_event(%__MODULE__{} = socket, %Events.ChannelClosed{}) do
    demonitor_channel(socket.channel_monitor)

    %{
      socket
      | channel_pid: nil,
        channel_monitor: nil,
        joins:
          Enum.into(socket.joins, %{}, fn {topic, %Join{} = join} ->
            {topic, %{join | status: :closed}}
          end)
    }
  end

  def apply_event(socket, _event), do: socket

  # The client monitors the connection process so that it is told about the
  # connection exiting without first sending a ChannelClosed event, e.g. by
  # crashing. In test mode the "connection" is the test process, so it is
  # not monitored.
  defp monitor_channel(previous_monitor, pid, config) do
    demonitor_channel(previous_monitor)

    case config do
      %Slipstream.Configuration{test_mode?: true} -> nil
      _ -> Process.monitor(pid)
    end
  end

  defp demonitor_channel(nil), do: :ok

  defp demonitor_channel(ref) do
    Process.demonitor(ref, [:flush])

    :ok
  end

  @doc false
  @spec next_reconnect_time(t()) :: {non_neg_integer(), t()}
  def next_reconnect_time(%__MODULE__{} = socket) do
    socket = update_in(socket, [Access.key(:reconnect_counter)], &(&1 + 1))

    time =
      retry_time(
        socket.channel_config.reconnect_after_msec,
        socket.reconnect_counter - 1
      )

    {time, socket}
  end

  @doc false
  @spec next_rejoin_time(t(), String.t()) :: {non_neg_integer(), t()}
  def next_rejoin_time(socket, topic) do
    socket =
      update_in(
        socket,
        [Access.key(:joins), topic, Access.key(:rejoin_counter)],
        &(&1 + 1)
      )

    time =
      retry_time(
        socket.channel_config.rejoin_after_msec,
        socket.joins[topic].rejoin_counter - 1
      )

    {time, socket}
  end

  defp retry_time(backoff_times, try_number) do
    # when we hit the end of the list, we repeat the last value in the list
    default = Enum.at(backoff_times, -1)

    Enum.at(backoff_times, try_number, default)
  end
end
