defmodule Slipstream.FakeWebSocketServer do
  @moduledoc """
  A minimal raw-TCP WebSocket server for tests that need byte-level control
  over what the server sends (or does not send)

  `start/2` listens on a random port and calls `handler` with the accepted
  socket for every connection. The listener and every handler are torn down
  when the calling (test) process exits.

      uri =
        FakeWebSocketServer.start(fn socket ->
          FakeWebSocketServer.handshake(socket)
          FakeWebSocketServer.send_frame(socket, :ping, "hello")
          Process.sleep(:infinity)
        end)
  """

  @guid "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"
  @opcodes %{text: 0x1, binary: 0x2, close: 0x8, ping: 0x9, pong: 0xA}

  @doc """
  Starts a listener and returns the `ws://` URI to connect to
  """
  def start(handler, listen_opts \\ []) when is_function(handler, 1) do
    owner = self()

    spawn_link(fn ->
      Process.flag(:trap_exit, true)

      {:ok, lsock} =
        :gen_tcp.listen(
          0,
          [:binary, active: false, reuseaddr: true, packet: :raw] ++
            listen_opts
        )

      {:ok, port} = :inet.port(lsock)
      send(owner, {__MODULE__, :port, port})
      accept_loop(lsock, handler, owner)
    end)

    receive do
      {__MODULE__, :port, port} -> "ws://127.0.0.1:#{port}/socket/websocket"
    end
  end

  defp accept_loop(lsock, handler, owner) do
    receive do
      {:EXIT, ^owner, _reason} -> exit(:shutdown)
      {:EXIT, _handler, _reason} -> accept_loop(lsock, handler, owner)
    after
      0 -> :ok
    end

    case :gen_tcp.accept(lsock, 50) do
      {:ok, sock} ->
        pid = spawn_link(fn -> receive(do: (:go -> handler.(sock))) end)
        :ok = :gen_tcp.controlling_process(sock, pid)
        send(pid, :go)

      {:error, :timeout} ->
        :ok
    end

    accept_loop(lsock, handler, owner)
  end

  @doc """
  Reads the HTTP upgrade request and returns the `sec-websocket-key`
  """
  def read_request(sock, acc \\ "") do
    {:ok, data} = :gen_tcp.recv(sock, 0, 5_000)
    acc = acc <> data

    if String.contains?(acc, "\r\n\r\n") do
      [_, key] = Regex.run(~r/sec-websocket-key:\s*(\S+)/i, acc)
      key
    else
      read_request(sock, acc)
    end
  end

  @doc """
  The full `101 Switching Protocols` response for a request key
  """
  def upgrade_response(key) do
    accept = Base.encode64(:crypto.hash(:sha, key <> @guid))

    "HTTP/1.1 101 Switching Protocols\r\n" <>
      "upgrade: websocket\r\n" <>
      "connection: Upgrade\r\n" <>
      "sec-websocket-accept: #{accept}\r\n\r\n"
  end

  @doc """
  Reads the upgrade request and accepts it
  """
  def handshake(sock) do
    key = read_request(sock)
    :ok = :gen_tcp.send(sock, upgrade_response(key))
  end

  @doc """
  Encodes an unmasked server-to-client frame
  """
  def frame(opcode, payload) when is_atom(opcode),
    do: frame(Map.fetch!(@opcodes, opcode), payload)

  def frame(opcode, payload) when is_integer(opcode) do
    len = byte_size(payload)

    header =
      cond do
        len < 126 -> <<1::1, 0::3, opcode::4, 0::1, len::7>>
        len < 65_536 -> <<1::1, 0::3, opcode::4, 0::1, 126::7, len::16>>
        true -> <<1::1, 0::3, opcode::4, 0::1, 127::7, len::64>>
      end

    header <> payload
  end

  def send_frame(sock, opcode, payload),
    do: :gen_tcp.send(sock, frame(opcode, payload))

  @doc """
  Sends a Phoenix V2 JSON message (a list) as a text frame
  """
  def send_message(sock, message) when is_list(message),
    do: send_frame(sock, :text, Jason.encode!(message))

  def reply(sock, join_ref, ref, topic, status, response \\ %{}) do
    send_message(sock, [
      join_ref,
      ref,
      topic,
      "phx_reply",
      %{"status" => status, "response" => response}
    ])
  end

  @doc """
  Receives one (masked) client frame

  Returns `{:ok, opcode, payload, rest}` or `{:error, reason}`
  """
  def recv_frame(sock, buffer \\ "") do
    case parse(buffer) do
      {:ok, _op, _payload, _rest} = ok ->
        ok

      :more ->
        case :gen_tcp.recv(sock, 0, 30_000) do
          {:ok, data} -> recv_frame(sock, buffer <> data)
          {:error, reason} -> {:error, reason}
        end
    end
  end

  defp parse(
         <<_::4, op::4, 1::1, 127::7, len::64, mask::binary-4, rest::binary>>
       ),
       do: unmask(op, mask, rest, len)

  defp parse(
         <<_::4, op::4, 1::1, 126::7, len::16, mask::binary-4, rest::binary>>
       ),
       do: unmask(op, mask, rest, len)

  defp parse(<<_::4, op::4, 1::1, len::7, mask::binary-4, rest::binary>>)
       when len < 126,
       do: unmask(op, mask, rest, len)

  defp parse(_), do: :more

  defp unmask(op, mask, data, len) when byte_size(data) >= len do
    <<payload::binary-size(len), rest::binary>> = data
    key = mask |> :binary.copy(div(len, 4) + 1) |> binary_part(0, len)
    {:ok, op, :crypto.exor(payload, key), rest}
  end

  defp unmask(_op, _mask, _data, _len), do: :more

  @doc """
  Accepts the upgrade and then behaves like a tiny Phoenix server: joins and
  leaves are accepted and heartbeats are acknowledged

  `override` is called with `(socket, {join_ref, ref, topic, event, payload})`
  for every text message from the client; return `:default` to fall back to
  the default behaviour.
  """
  def serve(sock, override \\ fn _sock, _message -> :default end) do
    handshake(sock)
    serve_loop(sock, "", override)
  end

  defp serve_loop(sock, buffer, override) do
    case recv_frame(sock, buffer) do
      {:ok, 0x1, payload, rest} ->
        [join_ref, ref, topic, event, params] = Jason.decode!(payload)
        message = {join_ref, ref, topic, event, params}

        if override.(sock, message) == :default,
          do: default_reply(sock, message)

        serve_loop(sock, rest, override)

      {:ok, _opcode, _payload, rest} ->
        serve_loop(sock, rest, override)

      {:error, _reason} ->
        :ok
    end
  end

  defp default_reply(sock, {join_ref, ref, topic, "phx_join", _params}),
    do: reply(sock, join_ref, ref, topic, "ok")

  defp default_reply(sock, {join_ref, ref, topic, "phx_leave", _params}) do
    reply(sock, join_ref, ref, topic, "ok")
    send_message(sock, [join_ref, join_ref, topic, "phx_close", %{}])
  end

  defp default_reply(sock, {_join_ref, ref, "phoenix", "heartbeat", _params}),
    do: reply(sock, nil, ref, "phoenix", "ok")

  defp default_reply(_sock, _message), do: :ok
end
