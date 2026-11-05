# we should use Application.compile_env/3 because it's available to us, but
# this is a library which may be used by projects with elixir versions <= 1.9.0
# and we really only use the application-config-to-module-attribute thing for
# testing. In addition, we really _want_ this to be a compile-time config.
# credo:disable-for-this-file Credo.Check.Warning.ApplicationConfigInModuleAttribute
defmodule Slipstream.Connection.Impl do
  @moduledoc false

  alias Slipstream.Connection.State
  alias Slipstream.Message
  alias Slipstream.Serializer

  import Slipstream.Signatures, only: [event: 1]

  @spec route_event(State.t(), event :: struct()) :: term()
  def route_event(%State{client_pid: pid}, event) do
    send(pid, event(event))
  end

  def http_connect(%{uri: %URI{host: {:local, _path}, port: 0}} = config) do
    local_connect(config)
  end

  def http_connect(config) do
    Mint.HTTP.connect(
      map_http_scheme(config.uri.scheme),
      config.uri.host,
      config.uri.port,
      config.mint_opts
    )
  end

  # Mint's success typing narrows the address to a binary, but Mint accepts
  # any :inet.socket_address/0 such as the Unix domain socket address
  # `{:local, path}` as long as the :hostname option is given.
  @dialyzer {:nowarn_function, local_connect: 1}

  defp local_connect(config) do
    Mint.HTTP.connect(
      map_http_scheme(config.uri.scheme),
      config.uri.host,
      0,
      Keyword.put_new(config.mint_opts, :hostname, "localhost")
    )
  end

  # coveralls-ignore-start
  defp map_http_scheme("wss"), do: :https
  # coveralls-ignore-stop
  defp map_http_scheme(_), do: :http

  # coveralls-ignore-start
  defp map_ws_scheme("wss"), do: :wss
  # coveralls-ignore-stop
  defp map_ws_scheme(_), do: :ws

  def websocket_upgrade(conn, config) do
    Mint.WebSocket.upgrade(
      map_ws_scheme(config.uri.scheme),
      conn,
      path(config.uri),
      upgrade_headers(config),
      extensions: config.extensions
    )
  end

  # Mint would send `Host: localhost:0` for a Unix domain socket address, which
  # some servers reject, so send the bare hostname unless one was given.
  defp upgrade_headers(%{uri: %URI{host: {:local, _path}, port: 0}} = config) do
    if Enum.any?(config.headers, fn {key, _} ->
         String.downcase(key) == "host"
       end) do
      config.headers
    else
      hostname = Keyword.get(config.mint_opts, :hostname, "localhost")
      [{"host", hostname} | config.headers]
    end
  end

  defp upgrade_headers(config), do: config.headers

  # ---

  def push_message(frame, %State{} = state) when is_tuple(frame) do
    case Mint.WebSocket.encode(state.websocket, frame) do
      {:ok, websocket, data} ->
        case Mint.WebSocket.stream_request_body(
               state.conn,
               state.request_ref,
               data
             ) do
          {:ok, conn} ->
            {:ok, %{state | conn: conn, websocket: websocket}}

          # coveralls-ignore-start
          {:error, conn, reason} ->
            {:error, put_in(state.conn, conn), reason}
            # coveralls-ignore-stop
        end

      # coveralls-ignore-start
      {:error, websocket, reason} ->
        {:error, put_in(state.websocket, websocket), reason}
        # coveralls-ignore-stop
    end
  end

  def push_message(%Message{payload: {:binary, _}} = message, state) do
    push_message({:binary, encode(message, state)}, state)
  end

  def push_message(message, state) do
    message =
      case encode(message, state) do
        {:binary, message} -> {:binary, message}
        message when is_binary(message) -> {:text, message}
      end

    push_message(message, state)
  end

  # coveralls-ignore-start
  def push_heartbeat(state) do
    %Message{
      topic: "phoenix",
      event: "heartbeat",
      ref: state.heartbeat_ref,
      payload: %{}
    }
    |> push_message(state)
  end

  # coveralls-ignore-stop

  defp encode(%Message{} = message, state) do
    module = state.config.serializer
    module.encode!(message, json_parser: state.config.json_parser)
  end

  def decode_message({encoding, message}, state)
      when encoding in [:text, :binary] and is_binary(message) do
    module = state.config.serializer

    try do
      module.decode!(message,
        opcode: encoding,
        json_parser: state.config.json_parser
      )
    rescue
      # coveralls-ignore-start
      _ in [Serializer.DecodeError] ->
        message
        # coveralls-ignore-stop
    end
  end

  # coveralls-ignore-start
  def decode_message(:ping, _state), do: :ping
  def decode_message(:pong, _state), do: :pong
  def decode_message({:close, _, _} = message, _state), do: message
  # coveralls-ignore-stop

  @dialyzer {:nowarn_function, path: 1}

  # this method of getting the path of a URI (including query) is maybe a bit
  # unorthodox, but I think it's better than string manipulation
  @spec path(URI.t()) :: String.t()
  def path(%URI{} = uri) do
    # select the v2 JSON serialization pattern
    query = URI.decode_query(uri.query || "", %{"vsn" => "2.0.0"})

    URI.to_string(%{
      uri
      | host: nil,
        port: nil,
        scheme: nil,
        userinfo: nil,
        path: uri.path || "/",
        query: URI.encode_query(query),
        # NOTE: this field is deprecated. It must be blanked out here
        # so that `to_string/1` gives just the path. But it makes the
        # dialyzer unhappy.
        authority: nil
    })
  end
end
