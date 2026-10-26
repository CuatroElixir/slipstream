defmodule Slipstream.Connection.State do
  @moduledoc false

  alias Slipstream.TelemetryHelper

  @type t :: %__MODULE__{}

  # a struct for storing the internal state of a Slipstream.Connection
  # process

  # the module which implements the Slipstream behaviour is hereby known
  # as the implementor

  defstruct [
    :connection_id,
    :trace_id,
    :client_pid,
    :client_ref,
    :config,
    :conn,
    :websocket,
    :request_ref,
    :join_params,
    :upgrade_timer,
    :heartbeat_timer,
    :heartbeat_ref,
    :metadata,
    status: :opened,
    joins: %{},
    leaves: %{},
    current_ref: 0,
    current_ref_str: "0"
  ]

  @doc """
  Creates a new State data structure

  The life-cycle of a `Slipstream.Connection` matches that of a websocket
  connection between client and remote websocket server, so a new `State`
  data structure represents a new connection (and therefore generates a new
  connection_id for telemetry purposes).
  """
  @spec new(config :: Slipstream.Configuration.t(), client_pid :: pid()) ::
          %__MODULE__{}
  def new(config, client_pid) do
    %__MODULE__{
      config: config,
      client_pid: client_pid,
      client_ref: Process.monitor(client_pid),
      trace_id: TelemetryHelper.trace_id(),
      connection_id: TelemetryHelper.id()
    }
  end

  @doc """
  Gets the next ref and increments the ref counter in state

  The `ref` passed between a client and phoenix server is a marker which
  can be used to link pushes to their replies. E.g. a heartbeat message from
  the client will include a ref which will match the associated heartbeat
  reply from the server, when the heartbeat is successful.

  Refs are simply strings of incrementing integers.
  """
  def next_ref(%__MODULE__{} = state) do
    ref = state.current_ref + 1

    {to_string(ref),
     %{state | current_ref: ref, current_ref_str: to_string(ref)}}
  end

  # coveralls-ignore-start
  def next_heartbeat_ref(%__MODULE__{} = state) do
    {ref, state} = next_ref(state)

    %{state | heartbeat_ref: ref}
  end

  # coveralls-ignore-stop

  @doc """
  Resets the heartbeat ref to nil

  This is used to clear out a pending heartbeat. If the
  `Slipstream.Commands.SendHeartbeat` command is received and the heartbeat_ref
  in state is nil, that means we have not received a reply to our heartbeat
  request and that the server is potentially stuck or otherwise not responding.
  """
  def reset_heartbeat(%__MODULE__{} = state) do
    %{state | heartbeat_ref: nil}
  end

  @doc """
  Detects if a ref is one used to join a topic
  """
  def join_ref?(%__MODULE__{joins: joins}, ref), do: ref in Map.values(joins)

  @doc """
  Detects if a ref was used to request a topic leave
  """
  def leave_ref?(%__MODULE__{leaves: leaves}, ref),
    do: ref in Map.values(leaves)
end
