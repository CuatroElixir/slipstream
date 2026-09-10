defmodule Slipstream.IntegrationTest do
  use ExUnit.Case, async: true

  import Slipstream.PidHelpers

  # tests a slipstream example fixture against the phoenix client running in
  # test mode

  @moduletag :capture_log

  @timeout 500

  @client Slipstream.GoodExample
  @server SlipstreamWeb.TestChannel

  describe "given c:Phoenix.Socket.connect/3 returns :error" do
    setup do
      [config: [uri: "ws://localhost:4001/socket/websocket?reject=yes"]]
    end

    test "the socket is disconnected with :upgrade_failure reason", c do
      import Slipstream

      assert {:error, {:upgrade_failure, %{status_code: 403}}} =
               c.config
               |> connect!()
               |> await_connect(15_000)
    end
  end

  describe "given the server never responds to the WebSocket upgrade request" do
    setup do
      {:ok, listen} = :gen_tcp.listen(0, [:binary, active: false])
      {:ok, port} = :inet.port(listen)
      test_pid = self()

      spawn_link(fn ->
        {:ok, socket} = :gen_tcp.accept(listen)
        {:ok, _upgrade_request} = :gen_tcp.recv(socket, 0)
        # never respond, but report when the client closes the connection
        send(test_pid, {:server_recv, :gen_tcp.recv(socket, 0)})
      end)

      [
        config: [
          uri: "ws://localhost:#{port}/socket/websocket",
          upgrade_timeout_msec: 100
        ]
      ]
    end

    test "the socket is disconnected with :upgrade_timeout reason", c do
      import Slipstream

      assert {:error, :upgrade_timeout} =
               c.config
               |> connect!()
               |> await_connect(15_000)

      assert_receive {:server_recv, {:error, :closed}}
    end
  end

  describe "given the server responds to the WebSocket upgrade request" do
    setup do
      [
        config: [
          uri: "ws://localhost:4001/socket/websocket",
          upgrade_timeout_msec: 500
        ]
      ]
    end

    test "the upgrade timeout does not close the connected socket", c do
      import Slipstream

      socket =
        c.config
        |> connect!()
        |> await_connect!(15_000)

      Process.sleep(1_000)

      assert Process.alive?(socket.channel_pid)
      refute_received _
    end
  end

  defmodule EtfSerializer do
    alias Slipstream.Message

    @behaviour Slipstream.Serializer

    @impl true
    def encode!(%Message{} = msg, _opts) do
      data = [
        msg.join_ref,
        msg.ref,
        msg.topic,
        msg.event,
        stringify_keys(msg.payload)
      ]

      {:binary, :erlang.term_to_binary(data)}
    end

    @impl true
    def decode!(binary, _opts) do
      [join_ref, ref, topic, event, payload] = :erlang.binary_to_term(binary)

      payload =
        case payload do
          {:binary, data} -> {:binary, data}
          other -> stringify_keys(other)
        end

      %Message{
        join_ref: join_ref,
        ref: ref,
        topic: topic,
        event: event,
        payload: payload
      }
    end

    defp stringify_keys(payload), do: Jason.encode!(payload) |> Jason.decode!()
  end

  for message_type <- [:normal, :binary] do
    describe "#{message_type}: given a connection has been established through the #{@client}" do
      @describetag message_type: message_type
      setup c do
        opts =
          case c.message_type do
            :normal ->
              [uri: "ws://localhost:4001/socket/websocket", pid: self()]

            :binary ->
              [
                uri: "ws://localhost:4001/socket/etf/websocket",
                pid: self(),
                serializer: EtfSerializer
              ]
          end

        pid = start_supervised!({@client, opts})
        assert_receive {@client, :connected}, @timeout

        [pid: pid, good_topic: "test:good", bad_topic: "test:bad"]
      end

      test "joining a good channel works", c do
        topic = c.good_topic

        :ok = join(c.pid, topic)
        assert_receive {@client, :joined, ^topic, _reply}, @timeout
      end

      test "duplicate joins do not result in an actual duplicate join", c do
        topic = c.good_topic

        :ok = join(c.pid, topic)
        assert_receive {@client, :joined, ^topic, _reply}, @timeout

        :ok = join(c.pid, topic)
        refute_receive {@client, :joined, ^topic, _reply}, @timeout
      end

      test "once a channel is joined it can be left", c do
        topic = c.good_topic

        :ok = join(c.pid, topic)
        assert_receive {@client, :joined, ^topic, _reply}, @timeout

        :ok = GenServer.cast(c.pid, {:leave, topic})
        assert_receive {@client, :left, ^topic}, @timeout
      end

      test "once a channel is left, you cannot duplicate the leave", c do
        topic = c.good_topic

        :ok = join(c.pid, topic)
        assert_receive {@client, :joined, ^topic, _reply}, @timeout

        :ok = GenServer.cast(c.pid, {:leave, topic})
        assert_receive {@client, :left, ^topic}, @timeout

        :ok = GenServer.cast(c.pid, {:leave, topic})
        refute_receive {@client, :left, ^topic}, @timeout
      end

      test "a message may be pushed to the remote", c do
        topic = c.good_topic

        :ok = join(c.pid, topic)
        assert_receive {@client, :joined, ^topic, _reply}, @timeout

        :ok = GenServer.cast(c.pid, {:push, topic, "quicksand", %{}})
        assert_receive {@server, :in, ^topic, "quicksand", %{}}, @timeout
      end

      test "if a topic is not yet joined, you may not push a message", c do
        topic = c.good_topic

        :ok = GenServer.cast(c.pid, {:push, topic, "quicksand", %{}})
        refute_receive {@server, :in, ^topic, "quicksand", %{}}, @timeout
      end

      test "a message may be received from the server", c do
        topic = c.good_topic

        :ok = join(c.pid, topic)
        assert_receive {@client, :joined, ^topic, _reply}, @timeout

        :ok = GenServer.cast(c.pid, {:push, topic, "push to me", %{}})

        assert_receive {@client, :received_message, ^topic, "foo",
                        %{"bar" => "baz"}},
                       @timeout
      end

      test "a reply may be received from the server", c do
        topic = c.good_topic

        :ok = join(c.pid, topic)
        assert_receive {@client, :joined, ^topic, _reply}, @timeout

        :ok = GenServer.cast(c.pid, {:push, topic, "ping", %{}})

        assert_receive {@client, :received_reply, _ref,
                        {:ok, %{"pong" => "pong"}}},
                       @timeout
      end

      test "a regular broadcast may be received from the server", c do
        topic = c.good_topic

        :ok = join(c.pid, topic)
        assert_receive {@client, :joined, ^topic, _reply}, @timeout

        :ok = GenServer.cast(c.pid, {:push, topic, "broadcast", %{}})

        assert_receive {@client, :received_message, ^topic, "broadcast event",
                        %{"hello" => "everyone!"}},
                       @timeout
      end

      test "a binary broadcast may be received from the server", c do
        topic = c.good_topic

        :ok = join(c.pid, topic)
        assert_receive {@client, :joined, ^topic, _reply}, @timeout

        :ok = GenServer.cast(c.pid, {:push, topic, "binary broadcast", %{}})

        assert_receive {@client, :received_message, ^topic, "broadcast event",
                        {:binary, "🏴‍☠️"}},
                       @timeout
      end

      test "a connection may be disconnected", c do
        topic = c.good_topic

        :ok = join(c.pid, topic)
        assert_receive {@client, :joined, ^topic, _reply}, @timeout

        :ok = GenServer.cast(c.pid, :disconnect)
        assert_receive {@client, :disconnected, reason}, @timeout
        assert reason == :client_disconnect_requested
      end

      test "if the remote server raises, we handle a topic disconnect event",
           c do
        topic = c.good_topic

        :ok = join(c.pid, topic)
        assert_receive {@client, :joined, ^topic, _reply}, @timeout

        :ok = GenServer.cast(c.pid, {:push, topic, "raise", %{}})
        assert_receive {@client, :topic_closed, ^topic, {:error, %{}}}, @timeout
      end

      test "if the remote server stops, we handle a left event", c do
        topic = c.good_topic

        :ok = join(c.pid, topic)
        assert_receive {@client, :joined, ^topic, _reply}, @timeout

        :ok = GenServer.cast(c.pid, {:push, topic, "stop", %{}})
        assert_receive {@client, :left, ^topic}, @timeout
      end

      test "trying to join a non-existent topic fails", c do
        topic = "test:no function clause matching"

        :ok = join(c.pid, topic)

        assert_receive {@client, :topic_closed, ^topic,
                        {:failed_to_join, %{"reason" => "join crashed"}}},
                       @timeout
      end

      test "trying to join a bad topic fails", c do
        topic = c.bad_topic

        :ok = join(c.pid, topic)

        assert_receive {@client, :topic_closed, ^topic,
                        {:failed_to_join, %{"bad" => "join"}}},
                       @timeout
      end

      test "we may receive a reply which is just an atom", c do
        topic = c.good_topic

        :ok = join(c.pid, topic)
        assert_receive {@client, :joined, ^topic, _reply}, @timeout

        :ok = GenServer.cast(c.pid, {:push, topic, "ack", %{}})

        assert_receive {@client, :received_reply, _ref, :ok}, @timeout
      end
    end
  end

  defp join(pid, topic) do
    GenServer.cast(pid, {:join, topic, %{test_pid: pid_string()}})
  end
end
