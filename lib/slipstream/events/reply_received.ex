defmodule Slipstream.Events.ReplyReceived do
  @moduledoc false

  @type t :: %__MODULE__{}

  # a message that says that a push from the client has been replied-to by
  # the server

  defstruct [:topic, :reply, :ref]

  def to_reply(%{"status" => status, "response" => response})
      when status in ["ok", "error"] and map_size(response) == 0 do
    String.to_atom(status)
  end

  def to_reply(%{"status" => status, "response" => response})
      when status in ["ok", "error"] do
    {String.to_atom(status), response}
  end

  # Phoenix channels may reply with any atom status, e.g.
  # `{:reply, {:accepted, %{}}, socket}`. The status comes from the remote
  # server so it is not converted to an atom.
  def to_reply(%{"status" => status, "response" => response})
      when is_map(response) and map_size(response) == 0 do
    status
  end

  def to_reply(%{"status" => status, "response" => response}) do
    {status, response}
  end
end
