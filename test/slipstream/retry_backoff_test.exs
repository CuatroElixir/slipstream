defmodule Slipstream.RetryBackoffTest do
  use ExUnit.Case, async: true

  # test the reconnect/1 and rejoin/3 functionality

  import Slipstream
  import Slipstream.Socket
  import Slipstream.PidHelpers
  import Slipstream.Signatures, only: [command: 1]
  alias Slipstream.{Commands, CommandRouter}

  @moduletag :capture_log

  setup do
    [config: Application.fetch_env!(:slipstream, Slipstream.GoodExample)]
  end

  test "a fresh socket cannot request reconnection" do
    socket = new_socket()

    refute connected?(socket)
    assert reconnect(socket) == {:error, :no_config}
  end

  describe "given an open connection to a websocket server" do
    setup c do
      [socket: c.config |> connect!() |> await_connect!()]
    end

    test "when disconnected, a connection may be reestablished with reconnect/1",
         c do
      assert {:ok, socket} =
               c.socket
               |> disconnect()
               |> await_disconnect()

      refute connected?(socket)

      assert {:ok, socket} = socket |> reconnect()

      assert_receive command(%Commands.OpenConnection{} = command)

      CommandRouter.route_command(command)

      assert socket |> await_connect!() |> connected?()
    end

    test "if a socket is connected it cannot reconnect", c do
      assert connected?(c.socket)
      assert reconnect(c.socket) == {:error, :connected}

      refute_receive command(%Commands.OpenConnection{})
    end
  end

  describe "given a socket that is joined to a good topic" do
    setup c do
      topic = "test:good"
      params = %{test_pid: pid_string()}

      {:ok, socket} =
        c.config
        |> connect!()
        |> await_connect!()
        |> join(topic, params)
        |> await_join(topic)

      [socket: socket, topic: topic, params: params]
    end

    test "rejoining cannot be performed on a topic which is currently joined",
         c do
      socket = c.socket

      assert joined?(socket, c.topic)
      assert {:ok, ^socket} = rejoin(socket, c.topic)

      refute_receive command(%Commands.JoinTopic{})
    end

    test "a topic that has yet to be joined cannot be rejoined", c do
      socket = c.socket
      topic = "test:bad"

      refute joined?(socket, topic)
      assert rejoin(socket, topic) == {:error, :never_joined}

      refute_receive command(%Commands.JoinTopic{})
    end

    test "once left, a topic's join is gone: rejoin answers never_joined and join joins it again",
         c do
      topic = c.topic
      payload = c.params

      {:ok, socket} =
        c.socket
        |> leave(topic)
        |> await_leave(topic)

      refute joined?(socket, topic)
      refute Map.has_key?(socket.joins, topic)

      assert {:error, :never_joined} = rejoin(socket, topic)

      assert socket
             |> join(topic, payload)
             |> await_join!(topic)
             |> joined?(topic)
    end
  end
end
