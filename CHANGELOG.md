# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a
Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## 1.3.0 - 2026-10-26

### Added

- Added an `upgrade_timeout_msec` configuration option which limits how long
  the connection waits for the server to respond to the WebSocket upgrade
  request ([#88](https://github.com/NFIBrokerage/slipstream/pull/88))
    - Previously a server which accepted the TCP connection but never
      responded to the upgrade request would cause the connection to wait
      forever.
    - The timeout defaults to ten seconds. When it elapses, the connection is
      closed and `c:Slipstream.handle_disconnect/2` is invoked with
      `{:error, :upgrade_timeout}`. Set the option to `0` to disable the
      timeout.

### Fixed

- Fixed a `CaseClauseError` crash in the connection process when the server
  sends a WebSocket close frame after the HTTP/1 request has completed
  ([#86](https://github.com/NFIBrokerage/slipstream/pull/86))
    - The error is now handled as a disconnect and passed to
      `c:Slipstream.handle_disconnect/2`.

## 1.2.2 - 2025-12-15

### Fixed

- Added topic's join ref to pushed messages.
    - This fixes compatibility with Phoenix 1.8.3+.

## 1.2.1 - 2025-12-08

### Fixed

- Fixed sample code in `c:Slipstream:init/1` ([#77](https://github.com/NFIBrokerage/slipstream/pull/77))
- Fixed struct update compatibility with Elixir 1.19+ ([#82](https://github.com/NFIBrokerage/slipstream/pull/82))

## 1.2.0 - 2025-02-04

### Added

- Added support for `{:binary, binary()}` types in the serializer encode
  callback for pushing binary WebSocket frames to the server.

## 1.1.3 - 2024-12-04

### Fixed

- Fixed callback return types to include `{:noreply, new_socket}` and
  `{:noreply, new_socket, _rest}` (for returning timeouts or triggering
  `handle_continue/2` for example). These return types were always
  supported but not declared in the callback, so the dialyzer would
  complain if these values were returned.
    - `c:Slipstream.handle_connect/1`
    - `c:Slipstream.handle_disconnect/2`
    - `c:Slipstream.handle_join/3`
    - `c:Slipstream.handle_message/4`
    - `c:Slipstream.handle_reply/3`
    - `c:Slipstream.handle_topic_close/3`
    - `c:Slipstream.handle_leave/2`

## 1.1.2 - 2024-09-26

### Added

- Clarified and enhanced `Slipstream.push/4` docs
- Clarified and enhanced `Slipstream` module docs around retries

### Fixed

- Fixed a type error on Elixir 1.17+ about an unknown `.message` key.

## 1.1.1 - 2024-01-26

### Fixed

- Added deserialization clauses to the default (de)serializer for binary
  broadcast messages.
    - Broadcasts like `MyAppWeb.Endpoint.broadcast!(topic, event, {:binary, <<1, 2, 3>>})`
      improperly arrived as a message in `c:Slipstream.handle_info/2` prior to this fix.

## 1.1.0 - 2023-06-12

### Added

- Added configuration for a serializer module with the default module
  implementing `Phoenix.Socket.V2.JSONSerializer` (Phoenix's default
  serializer).
- Added support for sending and receiving binary data.

### Changed

- The `:json_parser` key previously required a module that exported
  `encode!/1` and `decode/1`. This key now requires a module that
  implements `encode!/1` and `decode!/1`.
    - This was previously documented as requiring `encode/1` but this
      was a typo in the documentation.
    - Note that many popular JSON parser/generator libraries share
      these function signatures (`encode/1`, `encode!/1`, `decode/1` and
      `decode!/1`), so it is unlikely that you will need to make any
      changes in your code.

## 1.0.4 - 2023-04-02

### Fixed

- Added the `join_ref` to the `phx_leave` message.
    - This fixes compatibility with Phoenix 1.6.16+ when using
      `Slipstream.leave/2`.

## 1.0.3 - 2023-03-06

### Fixed

- `nimble_options` is now allowed at `~> 1.0 or ~> 0.1`.
    - NimbleOptions' v1.0.0 release contains no breaking changes from the 0.x
      release series.

## 1.0.2 - 2023-03-06

### Fixed

- Fixed a memory growth issue that could occur when a socket failed to join a
  channel repeatedly.
    - Memory usage of the Slipstream.Connection process and the client process
      could grow sharply as the socket tried to rejoin the failed topic, with
      higher memory usage per retry and per unjoined topic.

## 1.0.1 - 2022-06-22

### Fixed

- Any `Mint.TransportError`s received will now close the WebSocket connection

## 1.0.0 - 2022-05-11

This release represents stability in the API. There are no functional changes
between this release and v0.8.5.

## 0.8.5 - 2022-04-26

### Changed

- Widened compatibility with `mint_web_socket` to `~> 1.0` or `~> 0.2`

## 0.8.4 - 2022-02-17

### Fixed

- Fixed compatibility with `mint_web_socket` v0.2.0

## 0.8.3 - 2021-10-07

### Fixed

- Fixed connection closing when `Mint.HTTP.stream/2` returns errors
    - [#42](https://github.com/NFIBrokerage/slipstream/42) thanks [`@parallel588`](https://github.com/parallel588)!

## 0.8.2 - 2021-10-01

### Changed

- Expanded dependency on `:telemetry` to allow `~> 1.0`
    - [#41](https://github.com/NFIBrokerage/slipstream/pull/41) thanks [`@fhunleth`](https://github.com/fhunleth)!

## 0.8.1 - 2021-07-06

### Fixed

- Properly emit the channel-closed event when `Mint.HTTP.stream/2` returns an
  error tuple about the connection being closed

## 0.8.0 - 2021-07-01

### Changed

- Switched out `:gun` for Mint.WebSocket as the low-level websocket client

### Removed

- Removed the `:gun_open_options` key from configuration
    - Use the new `:mint_opts` key instead to configure TLS options

### Added

- Added the `:mint_opts` key to configuration for controlling the options
  passed to `Mint.HTTP.connect/4`
- Added the `:extensions` key to configuration for specifying
  `Mint.WebSocket.Extension` configuration passed to `Mint.WebSocket.upgrade/4`

## 0.7.0 - 2021-06-21

### Added

- Added `c:Slipstream.handle_leave/2`
    - This is invoked when the server acknowledges that the client has left
      the topic
    - This was previously invoked as the more generic
      `c:Slipstream.handle_topic_close/3`

### Changed

- Topic leaves are now handled not in `c:Slipstream.handle_topic_close/3`
  but `c:Slipstream.handle_leave/2`
    - **this is a breaking change**

In order to migrate existing code, change any implementations of
`c:Slipstream.handle_topic_close/3` with the `reason` of `:left` from

```elixir
def handle_topic_close(_topic, :left, socket)
```

To

```elixir
def handle_leave(_topic, socket)
```

Note that this callback has a default behavior of performing a no-op.

### Fixed

- Failures to connect (via an `:error` return from `c:Phoenix.Socket.connect/3`)
  now correctly trigger a `Slipstream.Events.ChannelConnectFailed` event
    - when using the synchronous API, this will result in an error tuple with
      `Slipstream.await_connect/2` in the format of
      `{:error, {:connect_failure, %{resp_headers: resp_headers, status_code: status_code}}}`
      where `status_code` will be `403`.
    - when using the module-based API, this will invoke the
      `c:Slipstream.handle_disconnect/2` callback with the same error tuple

## 0.6.2 - 2021-03-01

### Fixed

- `await_join!/2` and `await_leave!/2` have been changed to `await_join!/3`
  and `await_leave!/3` matching their intended usage
    - the `topic` parameter has been added which coincides with the API of
      `await_join/3` and `await_leave/3`

## 0.6.1 - 2021-02-27

### Fixed

- Fixed dialyzer warnings arising from some code macro-injected by
  `Slipstream.__using__/1`
    - thanks [`@jjcarstens`](https://github.com/jjcarstens)!

## 0.6.0 - 2021-02-24

### Removed

- Removed dependency on Phoenix

## 0.5.4 - 2021-02-22

### Fixed

- Slipstream will now gracefully handle failures from `:gun.open/3`
    - errors will result in an invocation of a client's
      `c:Slipstream.handle_disconnect/2` callback (if the client is a module)
      or return an error tuple from `await_connect/2` in the case of a
      synchronous client

## 0.5.3 - 2021-02-11

### Changed

- Synchronous functions are now grouped under "Synchronous Functions" in the
  documentation

## 0.5.2 - 2021-02-10

### Fixed

- The timer reference for the heartbeat interval is now properly matched upon
  in the connection process
    - this fixes some behavior where the timer reference would not be canceled
      (with `:timer.cancel/1`) upon disconnection
    - this has not manifested itself as a bug as far as we are aware, but this
      fix should properly clean up the timer when it is no longer needed

## 0.5.1 - 2021-02-08

### Added

- Added an example guide on how to use GenServer operations for a client
- Added an example on using Slipstream to script interactions with remote
  `Phoenix.Channel`s

## 0.5.0 - 2021-02-07

### Added

- Added `Slipstream.Socket.update/3` which emulates `Phoenix.LiveView.update/3`
- Added an example on the rejoin-after-reconnect pattern for clients which
  dynamically join topics

## 0.4.4 - 2021-02-07

### Fixed

- Link to post-cutover-metrics image made absolute

## 0.4.3 - 2021-02-07

### Added

- Added examples to the documentation
    - "Graceful Startup" and "Repeater-style Client"
- Added a note about post-cutover performance changes after we (NFIBrokerage)
  cutover to Slipstream in our stack
    - This is a new section in the "Why :gun?" guide
- Added documentation on counting the number of Slipstream connection processes
  to `Slipstream.ConnectionSupervisor`

## 0.4.2 - 2021-02-05

### Fixed

- Added channel config to socket on `Slipstream.connect/2` or
  `Slipstream.connect!/2`
    - this fixes an issue reconnecting when faking a connection through
      `Slipstream.SocketTest.accept_connect/1`

## 0.4.1 - 2021-02-04

### Fixed

- Changed the default behavior of `c:Slipstream.terminate/2` to disconnect
  gracefully with `Slipstream.disconnect/1`

### Added

- Added better documentation on the `c:Slipstream.terminate/2` callback and
  how to disconnect in it to leave a connection gracefully

## 0.4.0 - 2021-02-04

### Added

- Added telemetry support for clients
    - see the telemetry guide for more details

## 0.3.4 - 2021-02-04

### Fixed

- Fixed spelling and grammar mistakes in the implementation guide

## 0.3.3 - 2021-02-04

### Added

- increased documentation around retry mechanisms
- added a page on the choice of `:gun` as the low-level webscket client

## 0.3.2 - 2021-02-03

### Fixed

- Fixed edge case where the remote server shutting down would not emit a
  `ChannelClosed` event
    - this and heartbeats caused the odd `:gun_error` atom from
      [#12](https://github.com/NFIBrokerage/slipstream/issues/12)

## 0.3.1 - 2021-02-03

### Fixed

- The telemetry event for message handles in the connection process should now
  correctly publish the `:events` key, not the `:event` key
- The telemetry guide now correctly states that the `:start_state` key holds
  the connection state for handle-style events

### Added

- Published the connection state at the end of the message handle for
  handle-type telemetry events in the connection process
    - this is published in the `:end_state` key

## 0.3.0 - 2021-02-03

### Added

- `:telemetry` events are executed for each message handled by the
  `Slipstream.Connection` process
    - this should aide in debugging in scenarios in which one wants to see the
      full event history of a connection

## 0.2.4 - 2021-02-02

### Fixed

- Emit `ChannelClosed` event on HTTP close from remote websocket server
    - See [#11](https://github.com/NFIBrokerage/slipstream/issues/11)

## 0.2.3 - 2021-02-02

### Changed

- docs: the main page now shows the `Slipstream` module
    - frankly, I just want people to see the Basic Usage section as soon as
      possible so they get a taste for the library

## 0.2.2 - 2021-02-01

### Fixed

- The connection now properly disconnects when the server's heartbeat replies
  timeout with requests
    - disconnects for this reason will send have `:heartbeat_timeout` as the
      reason referenced in `c:Slipstream.handle_disconnect/2`

## 0.2.1 - 2021-02-01

### Fixed

- Connection now correctly emits ChannelClosed events for `:gun` events which
  occur when a service's network access is terminated

## 0.2.0 - 2021-02-01

### Added

- Added a testing framework `Slipstream.SocketTest`

## 0.1.4 - 2021-01-31

### Fixed

- Published license to hex

## 0.1.3 - 2021-01-30

### Changed

- Widened dependency on `:gun` from `~> 1.3` to `~> 1.0`
    - as far as I can tell, there aren't any real changes to the websocket
      interface, so any of these versions should be OK

## 0.1.2 - 2021-01-30

### Added

- More documentation
    - including a Basic Usage section with a minimalistic example

## 0.1.1 - 2021-01-30

### Removed

- Removed direct dependency on `:telemetry`
    - see #4

## 0.1.0 - 2021-01-30

### Added

- Initial implementation and initial publish

## 0.0.0 - 2021-01-22

### Added

- This project was generated by Gaas

<!-- # Generated by Elixir.Gaas.Generators.Simple.Library.Changelog -->
