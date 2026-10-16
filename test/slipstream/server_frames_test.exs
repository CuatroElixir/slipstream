defmodule Slipstream.ServerFramesTest do
  use ExUnit.Case, async: true

  @moduledoc """
  Frames and messages which a remote server may send and which the
  connection process must handle without crashing
  """

  alias Slipstream.FakeWebSocketServer, as: Server

  @client Slipstream.GoodExample
  @topic "rooms:lobby"

  setup do
    [test_proc: self()]
  end

  defp start_client(uri) do
    start_supervised!(
      {@client, pid: self(), uri: uri, heartbeat_interval_msec: 0}
    )
  end

  # accepts the upgrade and then waits for the test process to say `:go` so
  # that frames are not sent before the client has finished connecting
  defp start_server(test_proc, after_connect) do
    Server.start(fn sock ->
      Server.handshake(sock)
      send(test_proc, {:server, self()})
      receive(do: (:go -> after_connect.(sock)))
      Process.sleep(:infinity)
    end)
  end

  defp connect(uri) do
    start_client(uri)
    assert_receive {@client, :connected}
    assert_receive {:server, server}
    send(server, :go)
  end

  test "a ping from the server is answered with a pong", c do
    uri =
      start_server(c.test_proc, fn sock ->
        Server.send_frame(sock, :ping, "hello")
        send(c.test_proc, {:server_received, Server.recv_frame(sock)})
      end)

    connect(uri)

    assert_receive {:server_received, {:ok, 0xA, "hello", _rest}}
    refute_receive {@client, :disconnected, _reason}
  end

  test "an unsolicited pong from the server is ignored", c do
    uri =
      start_server(c.test_proc, fn sock ->
        Server.send_frame(sock, :pong, "")
        Server.send_message(sock, [nil, nil, @topic, "still alive", %{}])
      end)

    connect(uri)

    assert_receive {@client, :received_message, @topic, "still alive", %{}}
    refute_receive {@client, :disconnected, _reason}
  end

  test "an invalid frame from the server fails the connection", c do
    uri =
      start_server(c.test_proc, fn sock ->
        Server.send_frame(sock, :text, <<0xFF, 0xFE>>)
      end)

    connect(uri)

    assert_receive {@client, :disconnected,
                    {:invalid_frame, {:invalid_utf8, <<0xFF, 0xFE>>}}}
  end

  describe "given a joined topic" do
    setup do
      uri =
        Server.start(fn sock ->
          Server.serve(sock, fn
            sock, {join_ref, ref, topic, "phx_join", _params} ->
              Server.reply(sock, join_ref, ref, topic, "ok")
              # a phx_error for a join ref the client is not tracking
              Server.send_message(sock, ["999", "999", topic, "phx_error", %{}])
              :ok

            sock, {join_ref, ref, topic, "accept me", _params} ->
              Server.reply(sock, join_ref, ref, topic, "accepted")

            sock, {join_ref, ref, topic, "queue me", _params} ->
              Server.reply(sock, join_ref, ref, topic, "queued", %{"n" => 3})

            sock, {_join_ref, _ref, topic, "broadcast", _params} ->
              Server.send_message(sock, [nil, nil, topic, "broadcast", %{}])

            _sock, _message ->
              :default
          end)
        end)

      client = start_client(uri)
      assert_receive {@client, :connected}
      GenServer.cast(client, {:join, @topic, %{}})
      assert_receive {@client, :joined, @topic, %{}}

      [client: client]
    end

    test "replies with statuses other than ok and error are received", c do
      GenServer.cast(c.client, {:push, @topic, "accept me", %{}})
      assert_receive {@client, :received_reply, _ref, "accepted"}

      GenServer.cast(c.client, {:push, @topic, "queue me", %{}})
      assert_receive {@client, :received_reply, _ref, {"queued", %{"n" => 3}}}

      refute_receive {@client, :disconnected, _reason}
    end

    test "a phx_error for an unknown join ref is ignored", c do
      GenServer.cast(c.client, {:push, @topic, "broadcast", %{}})
      assert_receive {@client, :received_message, @topic, "broadcast", %{}}

      refute_received {@client, :topic_closed, @topic, _reason}
      refute_receive {@client, :disconnected, _reason}
    end
  end
end
