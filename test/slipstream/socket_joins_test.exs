defmodule Slipstream.SocketJoinsTest do
  use ExUnit.Case, async: true

  alias Slipstream.{Events, Socket}

  defp socket_with(topics) do
    Enum.reduce(topics, Socket.new(), fn topic, socket ->
      Socket.put_join_config(socket, topic, %{})
    end)
  end

  test "a left topic's join is dropped" do
    socket =
      socket_with(["a", "b"])
      |> Socket.apply_event(%Events.TopicJoinSucceeded{
        topic: "a",
        response: %{}
      })
      |> Socket.apply_event(%Events.TopicLeft{topic: "a"})

    assert Map.keys(socket.joins) == ["b"]
  end

  test "a fresh topic per task does not grow the joins" do
    socket =
      Enum.reduce(1..500, socket_with([]), fn i, socket ->
        topic = "task:#{i}"

        socket
        |> Socket.put_join_config(topic, %{})
        |> Socket.apply_event(%Events.TopicJoinSucceeded{
          topic: topic,
          response: %{}
        })
        |> Socket.apply_event(%Events.TopicLeft{topic: topic})
      end)

    assert socket.joins == %{}
  end

  test "events for a topic whose join is gone change nothing" do
    socket =
      socket_with(["a"]) |> Socket.apply_event(%Events.TopicLeft{topic: "a"})

    for event <- [
          %Events.TopicLeft{topic: "a"},
          %Events.TopicJoinSucceeded{topic: "a", response: %{}},
          %Events.TopicJoinFailed{topic: "a", response: %{}},
          %Events.TopicJoinClosed{topic: "a", reason: :error}
        ] do
      assert Socket.apply_event(socket, event).joins == %{}
    end
  end

  test "a topic closed by an error keeps its join for rejoin" do
    socket =
      socket_with(["a"])
      |> Socket.apply_event(%Events.TopicJoinSucceeded{
        topic: "a",
        response: %{}
      })
      |> Socket.apply_event(%Events.TopicJoinClosed{topic: "a", reason: :error})

    assert %{status: :closed} = socket.joins["a"]
  end

  test "rejoining a left topic answers never_joined" do
    socket =
      socket_with(["a"]) |> Socket.apply_event(%Events.TopicLeft{topic: "a"})

    assert Slipstream.rejoin(socket, "a") == {:error, :never_joined}
  end
end
