defmodule Slipstream.Events do
  @moduledoc false

  require Logger

  alias Slipstream.Message

  alias __MODULE__.{
    NoOp,
    MessageReceived,
    ReplyReceived,
    HeartbeatAcknowledged,
    TopicJoinSucceeded,
    TopicJoinFailed,
    TopicJoinClosed,
    TopicLeft,
    TopicLeaveAccepted,
    PingReceived,
    PongReceived,
    ChannelClosed
  }

  alias Slipstream.Connection.State

  @doc """
  Maps a message from the remote websocket server to an internally known
  Slipstream event

  The connection state may need to be taken into consideration. It holds
  information about the `ref` fields on messages and whether or not they belong
  to joins or leaves.

  `server_message` is either a `%Slipstream.Message{}` or `:ping` or `:pong`.
  `connection_state` is the GenServer state of the connection process.
  """
  @spec map(atom() | Message.t(), State.t()) :: struct()
  def map(server_message, connection_state)

  def map({:ping, data}, _state), do: %PingReceived{data: data}
  def map({:pong, data}, _state), do: %PongReceived{data: data}

  # coveralls-ignore-start
  def map({:close, _, _}, _state), do: %ChannelClosed{reason: :closed_by_remote}

  # coveralls-ignore-stop

  # RFC 6455 requires the connection to be failed when a frame is invalid
  def map({:error, reason}, _state),
    do: %ChannelClosed{reason: {:invalid_frame, reason}}

  def map(
        %Message{
          topic: topic,
          event: event,
          payload: payload,
          ref: ref
        },
        _state
      )
      when ref == nil do
    %MessageReceived{topic: topic, event: event, payload: payload}
  end

  # coveralls-ignore-start
  def map(
        %Message{
          topic: "phoenix",
          event: "phx_reply",
          payload: %{"response" => response, "status" => "ok"},
          ref: ref
        },
        _state
      )
      when map_size(response) == 0 do
    %HeartbeatAcknowledged{ref: ref}
  end

  # coveralls-ignore-stop

  # a reply in which join_ref == ref is a reply to a request to join a topic
  def map(
        %Message{
          topic: topic,
          event: "phx_reply",
          payload: %{"response" => response, "status" => status},
          ref: ref,
          join_ref: join_ref
        },
        _state
      )
      when ref != nil and ref == join_ref do
    type = if status == "ok", do: TopicJoinSucceeded, else: TopicJoinFailed

    struct(type, topic: topic, ref: ref, response: response)
  end

  def map(
        %Message{
          topic: topic,
          event: "phx_reply",
          payload: %{"response" => _response, "status" => _status} = payload,
          ref: ref
        },
        state
      )
      when ref != nil do
    if State.leave_ref?(state, ref) do
      %TopicLeaveAccepted{topic: topic}
    else
      %ReplyReceived{
        topic: topic,
        reply: ReplyReceived.to_reply(payload),
        ref: ref
      }
    end
  end

  def map(
        %Message{
          topic: topic,
          event: "phx_error",
          payload: payload,
          ref: ref
        },
        state
      ) do
    if State.join_ref?(state, ref) do
      %TopicJoinClosed{topic: topic, reason: {:error, payload}, ref: ref}
    else
      # the error is for a join which is no longer tracked, e.g. a late
      # message for a topic which has since been closed or rejoined
      %NoOp{}
    end
  end

  def map(
        %Message{
          topic: topic,
          event: "phx_close",
          payload: payload,
          ref: _ref
        },
        _state
      )
      when map_size(payload) == 0 do
    %TopicLeft{topic: topic}
  end

  def map(message, _state) do
    # coveralls-ignore-start
    Logger.debug(
      "#{inspect(__MODULE__)} received unknown message #{inspect(message)}"
    )

    # coveralls-ignore-stop

    nil
  end
end
