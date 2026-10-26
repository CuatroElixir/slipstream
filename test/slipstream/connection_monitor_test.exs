defmodule Slipstream.ConnectionMonitorTest do
  use ExUnit.Case, async: true

  @moduledoc """
  The client process is told when its connection process exits, even when
  the connection process does not get a chance to send a ChannelClosed event
  """

  @client Slipstream.GoodExample
  @uri "ws://localhost:4001/socket/websocket"

  setup do
    client = start_supervised!({@client, pid: self(), uri: @uri})
    assert_receive {@client, :connected}
    %Slipstream.Socket{channel_pid: conn} = :sys.get_state(client)

    [client: client, conn: conn]
  end

  test "a connection which exits unexpectedly is a disconnect", c do
    Process.exit(c.conn, :kill)

    assert_receive {@client, :disconnected, {:connection_exited, :killed}}

    socket = :sys.get_state(c.client)
    refute Slipstream.Socket.connected?(socket)
    assert socket.channel_monitor == nil
  end

  test "a normal disconnect is only reported once", c do
    GenServer.cast(c.client, :disconnect)

    assert_receive {@client, :disconnected, :client_disconnect_requested}
    refute_receive {@client, :disconnected, _reason}
    assert :sys.get_state(c.client).channel_monitor == nil
  end
end
