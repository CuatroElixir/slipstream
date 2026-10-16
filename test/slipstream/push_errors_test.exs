defmodule Slipstream.PushErrorsTest do
  use ExUnit.Case, async: true

  @moduledoc """
  Failed pushes and joins are returned as errors: they neither exit the
  client process nor crash the connection process
  """

  alias Slipstream.FakeWebSocketServer, as: Server
  alias Slipstream.Serializer.EncodeError

  @client Slipstream.GoodExample
  @topic "rooms:lobby"

  defmodule PushClient do
    @moduledoc false
    use Slipstream

    def start_link(opts), do: Slipstream.start_link(__MODULE__, opts)

    @impl Slipstream
    def init(opts) do
      {test_proc, opts} = Keyword.pop!(opts, :pid)

      {:ok, opts |> connect!() |> assign(:test_proc, test_proc)}
    end

    @impl Slipstream
    def handle_connect(socket) do
      send(socket.assigns.test_proc, {__MODULE__, :connected})

      {:ok, join(socket, "rooms:lobby")}
    end

    @impl Slipstream
    def handle_join(topic, _response, socket) do
      send(socket.assigns.test_proc, {__MODULE__, :joined, topic})

      {:ok, socket}
    end

    @impl Slipstream
    def handle_call({:push, params, timeout}, _from, socket) do
      {:reply, push(socket, "rooms:lobby", "event", params, timeout), socket}
    end

    def handle_call(:socket, _from, socket), do: {:reply, socket, socket}
  end

  setup do
    uri = Server.start(&Server.serve/1)

    [uri: uri]
  end

  describe "given a joined client" do
    setup c do
      client =
        start_supervised!({PushClient, pid: self(), uri: c.uri},
          restart: :temporary
        )

      assert_receive {PushClient, :connected}
      assert_receive {PushClient, :joined, @topic}
      %Slipstream.Socket{channel_pid: conn} = GenServer.call(client, :socket)

      [client: client, conn: conn]
    end

    test "a payload which cannot be encoded is an error", c do
      assert {:error, %EncodeError{}} =
               GenServer.call(c.client, {:push, %{pid: self()}, 1_000})

      assert {:ok, _ref} = GenServer.call(c.client, {:push, %{}, 1_000})
      assert Process.alive?(c.conn)
    end

    test "a push which times out is an error", c do
      :ok = :sys.suspend(c.conn)

      assert {:error, :timeout} =
               GenServer.call(c.client, {:push, %{}, 100})

      :ok = :sys.resume(c.conn)
      assert Process.alive?(c.client)
      assert {:ok, _ref} = GenServer.call(c.client, {:push, %{}, 1_000})
    end

    test "a connection which exits during a push is an error", c do
      :ok = :sys.suspend(c.conn)
      push = Task.async(fn -> GenServer.call(c.client, {:push, %{}, 5_000}) end)
      Process.sleep(50)
      Process.exit(c.conn, :kill)

      assert Task.await(push) == {:error, :not_connected}
      assert Process.alive?(c.client)
    end
  end

  test "join params which cannot be encoded fail the join", c do
    client =
      start_supervised!(
        {@client, pid: self(), uri: c.uri, heartbeat_interval_msec: 0}
      )

    assert_receive {@client, :connected}
    %Slipstream.Socket{channel_pid: conn} = :sys.get_state(client)

    GenServer.cast(client, {:join, @topic, %{pid: self()}})

    assert_receive {@client, :topic_closed, @topic,
                    {:failed_to_join, {:encode_error, %EncodeError{}}}}

    assert Process.alive?(conn)
  end
end
