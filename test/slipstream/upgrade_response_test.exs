defmodule Slipstream.UpgradeResponseTest do
  use ExUnit.Case, async: true

  @moduledoc """
  The response to the WebSocket upgrade request may arrive in any number of
  TCP segments
  """

  alias Slipstream.FakeWebSocketServer, as: Server

  @client Slipstream.GoodExample

  defp start_client(uri) do
    start_supervised!(
      {@client, pid: self(), uri: uri, heartbeat_interval_msec: 0}
    )
  end

  test "an upgrade response which arrives a byte at a time is accepted" do
    uri =
      Server.start(
        fn sock ->
          key = Server.read_request(sock)

          for <<byte <- Server.upgrade_response(key)>> do
            :ok = :gen_tcp.send(sock, <<byte>>)
            Process.sleep(1)
          end

          Process.sleep(:infinity)
        end,
        nodelay: true
      )

    start_client(uri)

    assert_receive {@client, :connected}, 2_000
  end

  test "frames in the same packet as the upgrade response are received" do
    message = Jason.encode!([nil, nil, "rooms:lobby", "hello", %{}])

    uri =
      Server.start(fn sock ->
        key = Server.read_request(sock)

        :ok =
          :gen_tcp.send(
            sock,
            Server.upgrade_response(key) <> Server.frame(:text, message)
          )

        Process.sleep(:infinity)
      end)

    start_client(uri)

    assert_receive {@client, :connected}
    assert_receive {@client, :received_message, "rooms:lobby", "hello", %{}}
  end
end
