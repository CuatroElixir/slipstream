defmodule Slipstream.SendTimeoutTest do
  use ExUnit.Case, async: true

  @moduledoc """
  Writes to the socket are bounded so that a stalled peer cannot block the
  connection process forever
  """

  alias Slipstream.Configuration
  alias Slipstream.Connection.Impl
  alias Slipstream.FakeWebSocketServer, as: Server

  @client Slipstream.GoodExample
  @topic "rooms:lobby"

  describe "mint options" do
    defp config(opts) do
      Configuration.validate!([uri: "ws://localhost:4000/socket"] ++ opts)
    end

    test "a send timeout is added to the transport options by default" do
      assert config([]) |> Impl.mint_opts() |> Keyword.get(:transport_opts) ==
               [send_timeout: 30_000, send_timeout_close: true]
    end

    test "transport options given in mint_opts take precedence" do
      opts =
        config(
          send_timeout_msec: 5_000,
          mint_opts: [
            protocols: [:http1],
            transport_opts: [send_timeout: 1_000, sndbuf: 1_024]
          ]
        )
        |> Impl.mint_opts()

      assert opts[:protocols] == [:http1]

      assert Enum.sort(opts[:transport_opts]) == [
               send_timeout: 1_000,
               send_timeout_close: true,
               sndbuf: 1_024
             ]
    end

    test "a send timeout of 0 leaves the mint options as given" do
      assert config(send_timeout_msec: 0) |> Impl.mint_opts() ==
               [protocols: [:http1]]
    end
  end

  test "a peer which stops reading causes a disconnect" do
    uri =
      Server.start(
        fn sock ->
          Server.handshake(sock)
          {:ok, _opcode, join, _rest} = Server.recv_frame(sock)
          [join_ref, ref, topic, "phx_join", _params] = Jason.decode!(join)
          Server.reply(sock, join_ref, ref, topic, "ok")
          # never read from the socket again
          Process.sleep(:infinity)
        end,
        recbuf: 1_024
      )

    client =
      start_supervised!(
        {@client,
         pid: self(),
         uri: uri,
         heartbeat_interval_msec: 100,
         send_timeout_msec: 300,
         mint_opts: [protocols: [:http1], transport_opts: [sndbuf: 1_024]]}
      )

    assert_receive {@client, :connected}
    GenServer.cast(client, {:join, @topic, %{}})
    assert_receive {@client, :joined, @topic, %{}}

    # more than the socket buffers can hold
    GenServer.cast(
      client,
      {:push, @topic, "upload", %{"data" => String.duplicate("a", 8_000_000)}}
    )

    assert_receive {@client, :disconnected,
                    {:send_failure, %Mint.TransportError{reason: :timeout}}},
                   2_000
  end
end
