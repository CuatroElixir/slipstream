defmodule SlipstreamWeb.InteractiveChannel do
  @moduledoc false

  use SlipstreamWeb, :channel

  def join("rooms:lobby", _params, socket) do
    {:ok, socket}
  end

  # just swallows the request
  def handle_in("quicksand", _params, socket) do
    {:noreply, socket}
  end

  # responds to the request
  def handle_in("ping", _params, socket) do
    {:reply, {:ok, %{"pong" => "pong"}}, socket}
  end

  # responds, but not with a reply
  # just an async send
  def handle_in("push to me", _params, socket) do
    push(socket, "foo", %{"bar" => "baz"})

    {:noreply, socket}
  end
end
