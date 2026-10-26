defmodule Slipstream.CommandRouter do
  @moduledoc false

  # Implements how a command is routed.
  # Note that many commands are simply forwarded to the connection process
  # for further handling.
  # For other commands like OpenConnection, there is no connection process to
  # send to

  alias Slipstream.Commands.{
    OpenConnection,
    JoinTopic,
    LeaveTopic,
    PushMessage,
    CollectGarbage,
    CloseConnection
  }

  alias Slipstream.Socket

  alias Slipstream.Configuration, as: Config

  import Slipstream.Signatures, only: [command: 1]

  @forwarded_command_types [
    JoinTopic,
    LeaveTopic,
    CollectGarbage,
    CloseConnection
  ]

  @spec route_command(struct()) :: any()
  def route_command(command)

  def route_command(%OpenConnection{config: %Config{test_mode?: true}}) do
    :ok
  end

  def route_command(%OpenConnection{} = cmd) do
    DynamicSupervisor.start_child(
      Slipstream.ConnectionSupervisor,
      {Slipstream.Connection, cmd}
    )
  end

  # forward the commands to the connection process
  def route_command(%command_type{socket: socket} = cmd)
      when command_type in @forwarded_command_types do
    Socket.send(socket, command(cmd))
  end

  # this one is a synchronous push with a reply from the connection process:
  # either the push's ref or an error
  def route_command(%PushMessage{socket: socket} = cmd) do
    case Socket.call(socket, command(cmd), cmd.timeout) do
      ref when is_binary(ref) -> {:ok, ref}
      {:error, _reason} = error -> error
    end
  end
end
