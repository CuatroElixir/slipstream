defmodule SlipstreamWeb.TestChannel do
  use SlipstreamWeb, :channel

  import Slipstream.PidHelpers

  @moduledoc false

  def join("test:good", %{"test_pid" => proc_string}, socket) do
    socket =
      socket
      |> assign(:test_proc, pid(proc_string))
      |> assign(:topic, "test:good")

    {:ok, socket}
  end

  def join("test:bad", _payload, _socket) do
    {:error, %{"bad" => "join"}}
  end

  # just swallows the request
  def handle_in("quicksand", params, socket) do
    send(
      socket.assigns.test_proc,
      {__MODULE__, :in, socket.assigns.topic, "quicksand", params}
    )

    {:noreply, socket}
  end

  # responds to the request
  def handle_in("ping", {:binary, _}, socket) do
    {:reply, {:ok, {:binary, <<2, 3>>}}, socket}
  end

  def handle_in("ping", _params, socket) do
    {:reply, {:ok, %{"pong" => "pong"}}, socket}
  end

  # responds, but not with a reply
  # just an async send
  def handle_in("push to me", {:binary, _}, socket) do
    push(socket, "foo", {:binary, <<2, 3>>})

    {:noreply, socket}
  end

  def handle_in("push to me", _params, socket) do
    push(socket, "foo", %{"bar" => "baz"})

    {:noreply, socket}
  end

  def handle_in("error tuple", _params, socket) do
    {:reply, {:error, %{"failure?" => true}}, socket}
  end

  def handle_in("ack", _params, socket) do
    {:reply, :ok, socket}
  end

  def handle_in("raise", _params, _socket) do
    raise "oooooooohnnnnnnnnnnnnnoooooooooooooooooooooooooooooo"
  end

  def handle_in("broadcast", _params, socket) do
    :ok =
      SlipstreamWeb.Endpoint.broadcast!(
        socket.assigns.topic,
        "broadcast event",
        %{hello: "everyone!"}
      )

    {:noreply, socket}
  end

  def handle_in("binary broadcast", _params, socket) do
    :ok =
      SlipstreamWeb.Endpoint.broadcast!(
        socket.assigns.topic,
        "broadcast event",
        {:binary, "🏴‍☠️"}
      )

    {:noreply, socket}
  end

  def handle_in("stop", _params, socket) do
    {:stop, :normal, socket}
  end
end
