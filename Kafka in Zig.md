# 0. Setup
We will be using Zig 0.15.2 in this tutorial.
Assume you have zig installed on you machine, along with [zls](https://github.com/zigtools/zls). Let's setup a zig project called `zig_kafka`. 
```
mkdir zig_kafka
cd zig_kafka
zig init
```
In `src/main.zig`, you can find the following main function:
```zig
const std = @import("std");
const zig_kafka = @import("zig_kafka");

pub fn main() !void {
    // Prints to stderr, ignoring potential errors.
    std.debug.print("All your {s} are belong to us.\n", .{"codebase"});
    try zig_kafka.bufferedPrint();
}
```
Since we're working with network, we can make use of `std.net` and alias it:
```zig
const std = @import("std");
const net = std.net;
const zig_kafka = @import("zig_kafka");

pub fn main() !void {
	...
}
```

# 1. Distributed messaging
The first step is to send and receive messages from another machine. Suppose you have 2 machines, you can:
- On machine A, create a TCP server with an address (“IP1”) on a machine, which accepts a connection.
- On machine B, connect to an address (“IP1”) using TCP protocol.
- Send and receive messages via the established stream.

Since we don't have 2 machine (great if you do!), we will simulate different machine using different port / processes.


## Create a TCP server
First, let’s define a function to start the server:
```zig
pub fn startServer() !void {
	const address = try net.Address.parseIp4("127.0.0.1", 1234);
	var server = try address.listen(.{}); // TCP server
	const connection = try server.accept(); // Accept a connection
}
```
- We defined an IP address `127.0.0.1` with port `1234`
- The `listen` function returned a `Server` with an open `stream`
- On calling `server.accept()`, the program will block until the server is connected.

## Connect to the server
The following function will connect to the server:
```zig
pub fn clientConnectTCP() !void {
    const address = try net.Address.parseIp4("127.0.0.1", 1234);
    var stream = try net.tcpConnectToAddress(address);
}
```
- We also defined an IP address `127.0.0.1` with port `1234`
- After connect to the TCP server using `net.tcpConnectToAddress`, we will receive a `Stream`.

### The `Stream` object, reader and writer
Under the document for [std.net.Stream](https://ziglang.org/documentation/0.15.2/std/#std.net.Stream), you can find that it has only 2 functions that are not deprecated:
- `pub fn reader(stream: Stream, buffer: []u8) Reader`
- `pub fn writer(stream: Stream, buffer: []u8) Writer`

These 2 return the respective `Reader` and `Writer`  type: A buffered reader / writer that we can use to read data from and write data to our stream.
- Reader implement the [std.Io.Reader](https://ziglang.org/documentation/0.15.2/std/#std.Io.Reader) type, providing primitive for reading like `peek`, `take`, `takeDelimiter`, `stream`, ...
- Writer implement the [std.Io.Writer](https://ziglang.org/documentation/0.15.2/std/#std.Io.Writer) type with `write`, `writeByte`, `flush`, ...

In Zig, `stdin` also provided a `Reader` and `stdout` provide a writer, so it's a very good idea to get familiarized with them and how to read/write.

### I/O Multiplexing
- `epoll`
- `kqueue`
- `io_uring`
- Create stream / decode / encode

## Our first program: Echo
Our first goal is for the server to receive a message from the client, and then output it in the stdout.

### Message layout
To assist reading and writing, our message will follow this format:
- First byte is the message length: `n` (currently limit to 255)
- Next `n` byte will contain the message in bytes

 ### Client ping

Let's modify the `startServer()` function as follow:

```zig
pub fn startServer() !void {
    const address = try net.Address.parseIp4("127.0.0.1", 1234);
    var server = try address.listen(.{ .reuse_address = true }); // TCP server
    const connection = try server.accept(); // Accept a connection
    
    // Read data from the client and output
    var stream_read_buff: [1024]u8 = undefined;
    var stream_rd = connection.stream.reader(&stream_read_buff);
    while (true) {
        const header = try stream_rd.file_reader.interface.takeByte();
        if (header != 0) {
            const data = try stream_rd.file_reader.interface.take(header);
            std.debug.print("Receive message from client: {s}\n", .{data});
        }
    }
    defer server.stream.close();
}
```
- First, we use `takeByte()` to read 1 byte, which is the message length.
- Next, we use `take(n)` to read n byte.
- Lastly, write debug.

Let's modify our `clientConnectTCP()`:

```zig
pub fn clientConnectTCP() !void {
    const address = try net.Address.parseIp4("127.0.0.1", 1234);
    var stream = try net.tcpConnectToAddress(address);
    // Read input from stdin and write to stream.
    var stdin_buf: [1024]u8 = undefined;
    var rd = std.fs.File.stdin().reader(&stdin_buf);
    var stream_write_buff: [1024]u8 = undefined;
    var stream_wr = stream.writer(&stream_write_buff);
    while (true) {
        const line = rd.interface.takeSentinel('\n') catch |err| {
            switch (err) {
                error.EndOfStream => {
                    // Do nothing here...
                    break;
                },
                else => {
                    return err;
                },
            }
        };
        std.debug.print("Sent to server: {s}\n", .{line});
        try stream_wr.interface.writeByte(@intCast(line.len));
        try stream_wr.interface.writeAll(line);
        try stream_wr.interface.flush();
    }
}
```

- The `var rd = std.fs.File.stdin().reader(&stdin_buf);` create a reader from `stdin` which allow us to input anything that can send to server.
- `rd.interface.takeSentinel('\n')` is used to take all bytes from the input until we meet a `'\n'` character, effectively input every `Enter`
- We then write the `len` of the input as first byte
- Then write all data in the `line`
- `flush()` is used to drain all data to the stream.

Let's modify the `main` function to include argument parsing:
```zig
pub fn main() !void {
    if (std.mem.eql(u8, std.mem.span(std.os.argv[1]), "server")) {
        try startServer();
    } else {
        try clientConnectTCP();
    }
}
```
- This help us figure out which function to run: if the first argument is `"server"`, then we will start the server, else we will be the client and connect to a server.

We can now build and run the project with 2 terminal open:
- Terminal 1:
```
zig build
./zig-out/bin/zig_kafka server
```
- Terminal 2:
```
./zig-out/bin/zig_kafka client
```
And input into terminal 2 + press enter to see what happens!

### Server pong
Next, we want the server to write back message that it has received. Since our stream is bidirectional, we can make use of the `stream.writer` to write the message.

Let's modify the `startServer()`:
```zig
pub fn startServer() !void {
    const address = try net.Address.parseIp4("127.0.0.1", 1234);
    var server = try address.listen(.{ .reuse_address = true }); // TCP server
    const connection = try server.accept(); // Accept a connection
    // Read data from the client and output
    var stream_read_buff: [1024]u8 = undefined;
    var stream_write_buff: [1024]u8 = undefined;
    // Bidirectional read/write
    var stream_rd = connection.stream.reader(&stream_read_buff);
    var stream_wr = connection.stream.writer(&stream_write_buff);
    while (true) {
        const header = try stream_rd.file_reader.interface.takeByte(); // TODO: End of stream error handling
        if (header != 0) {
            const data = try stream_rd.file_reader.interface.take(header);
            std.debug.print("Receive message from client: {s}\n", .{data});
            if (std.mem.eql(u8, data, "bye")) {
                // Allow exit on bye
                break;
            }
            const sent_data = try std.fmt.allocPrint(std.heap.page_allocator, "I have received: {s}", .{data});
            // Send back to the client: I have seen it
            try stream_wr.interface.writeByte(@intCast(sent_data.len)); // Send how many byte written
            try stream_wr.interface.writeAll(sent_data);
            try stream_wr.interface.flush();
        }
    }
    defer server.stream.close();
}
```
- We add a new `stream_wr` to the server. 
- Exit utility: `std.mem.eql(u8, data, "bye")`: If the received data is `"bye"`, then we exit the program.
-  We use `std.fmt.allocPrint` to create a string that say `"I have received: [data]"`
- Sent back to the stream with the same message format.

In `clientConnectTCP()`, we can add the input reading from stream like so:
```zig
pub fn clientConnectTCP() !void {
    const address = try net.Address.parseIp4("127.0.0.1", 1234);
    var stream = try net.tcpConnectToAddress(address);
    // Read input from stdin and write to stream.
    var stdin_buf: [1024]u8 = undefined;
    var rd = std.fs.File.stdin().reader(&stdin_buf);
    var stream_read_buff: [1024]u8 = undefined;
    var stream_write_buff: [1024]u8 = undefined;
    var stream_rd = stream.reader(&stream_read_buff);
    var stream_wr = stream.writer(&stream_write_buff);
    while (true) {
        const line = rd.interface.takeSentinel('\n') catch |err| {
            switch (err) {
                error.EndOfStream => {
                    // Do nothing here...
                    break;
                },
                else => {
                    return err;
                },
            }
        };
        std.debug.print("Sent to server: {s}\n", .{line});
        try stream_wr.interface.writeByte(@intCast(line.len));
        try stream_wr.interface.writeAll(line);
        try stream_wr.interface.flush();
        if (std.mem.eql(u8, line, "bye")) {
            return;
        }
        // Try to read back from the stream
        const header = try stream_rd.file_reader.interface.takeByte(); // TODO: End of stream error handling
        if (header != 0) {
            const data = try stream_rd.file_reader.interface.take(header);
            std.debug.print("Received from server: {s}\n", .{data});
        }
    }
}
```
- Add `stream_rd`: A reader to read from the stream
- Read the message like the input format.

Since this is kinda repetitive, let's create function for read and write from stream, and refactor code a bit:
```zig
pub fn readLineFromStdin(stdin_rd: *std.fs.File.Reader) !?[]u8 {
    const line = stdin_rd.interface.takeSentinel('\n') catch |err| {
        switch (err) {
            error.EndOfStream => {
                // EOF done, nothing to do.
                return null;
            },
            else => {
                return err;
            },
        }
    };
    return line;
}

pub fn readFromStream(stream_rd: *net.Stream.Reader) !?[]u8 {
    const header = try stream_rd.file_reader.interface.takeByte();
    if (header != 0) {
        const data = try stream_rd.file_reader.interface.take(header);
        return data;
    } else {
        return null;
    }
}

pub fn writeToStream(stream_wr: *net.Stream.Writer, data: []u8) !void {
    try stream_wr.interface.writeByte(@intCast(data.len)); // Send how many byte written
    try stream_wr.interface.writeAll(data);
    try stream_wr.interface.flush();
}

pub fn startServer() !void {
    const address = try net.Address.parseIp4("127.0.0.1", 1234);
    var server = try address.listen(.{ .reuse_address = true }); // TCP server
    const connection = try server.accept(); // Accept a connection
    // Read data from the client and output
    var stream_read_buff: [1024]u8 = undefined;
    var stream_write_buff: [1024]u8 = undefined;
    // Bidirectional read/write
    var stream_rd = connection.stream.reader(&stream_read_buff);
    var stream_wr = connection.stream.writer(&stream_write_buff);
    while (true) {
        if (try readFromStream(&stream_rd)) |data| {
            std.debug.print("Receive message from client: {s}\n", .{data});
            if (std.mem.eql(u8, data, "bye")) {
                // Allow exit on bye
                break;
            }
            // Send back to the client: I have seen it
            const sent_data = try std.fmt.allocPrint(std.heap.page_allocator, "I have received: {s}", .{data});
            try writeToStream(&stream_wr, sent_data);
        }
    }
    defer server.stream.close();
}

pub fn clientConnectTCP() !void {
    const address = try net.Address.parseIp4("127.0.0.1", 1234);
    var stream = try net.tcpConnectToAddress(address);
    // Read input from stdin and write to stream.
    var stdin_buf: [1024]u8 = undefined;
    var rd = std.fs.File.stdin().reader(&stdin_buf);
    var stream_read_buff: [1024]u8 = undefined;
    var stream_write_buff: [1024]u8 = undefined;
    var stream_rd = stream.reader(&stream_read_buff);
    var stream_wr = stream.writer(&stream_write_buff);
    while (try readLineFromStdin(&rd)) |line| {
        std.debug.print("Sent to server: {s}\n", .{line});
        try writeToStream(&stream_wr, line);
        if (std.mem.eql(u8, line, "bye")) {
            return;
        }
        // Try to read back from the stream
        if (try readFromStream(&stream_rd)) |data| {
            std.debug.print("Received from server: {s}\n", .{data});
        }
    }
}
```

### Multiple connection
Next, we would like to explore how to work with multiple connections via threading. What we aim to have:
- One server process, handle multiple TCP server on different addresses.
- Multiple client processes, each connect to a different TCP server, and ping pong with it.

#### Why not async I/O?
Our server program spend most of it time waiting on I/O operations from different stream reader, so a natural approach is to use async to improve performance. 
I opted to save it for later.

#### Multithreading in Zig
Our library for threading in Zig is [std.Thread](https://ziglang.org/documentation/0.15.2/std/#std.Thread). This provide some primitives:
- Thread pool and wait group
- Lock primitives: Mutex, RwLock and Semaphore
- Some functions like `spawn` and `join`

First, let's modify the program a bit to spawn 2 thread that create a TCP server on port 10001 and 10002:
```zig
pub fn main() !void {
    if (std.mem.eql(u8, std.mem.span(std.os.argv[1]), "server")) {
        try spawnServer();
    }
    ...
}


pub fn spawnServer() !void {
    const thread_1 = try std.Thread.spawn(.{}, startServer, .{@as(u16, 10001)});
    const thread_2 = try std.Thread.spawn(.{}, startServer, .{@as(u16, 10002)});
    std.Thread.join(thread_1);
    std.Thread.join(thread_2);
}

pub fn startServer(port: u16) !void {
    std.debug.print("Start server on port {}\n", .{port});
    ...
    while (true) {
        if (try readFromStream(&stream_rd)) |data| {
            std.debug.print("Receive message from client port = {}: {s}\n", .{ port, data });
		...
}
```
On the client side, we allow another argument to indicate the port you want to connect to:
```zig
pub fn main() !void {
    if (std.mem.eql(u8, std.mem.span(std.os.argv[1]), "server")) {
        try spawnServer();
    } else {
        const port_str = std.mem.span(std.os.argv[2]); // 2nd argument is the port
        const port_int = try std.fmt.parseInt(u16, port_str, 10);
        try clientConnectTCP(port_int);
    }
}

pub fn clientConnectTCP(port: u16) !void {
    const address = try net.Address.parseIp4("127.0.0.1", port);
...
```
You can test it out using the following setup:
- Terminal 1:
```
./zig-out/bin/zig_kafka server
```

- Terminal 2:
```
./zig-out/bin/zig_kafka client 10001
```

- Terminal 3:
```
./zig-out/bin/zig_kafka client 10002
```
Then you can try to send messages from any client to the server and receive back the echo.

# 2. Distributed message queue design
Now that we have some way to send message to TCP stream in another machine, let's design our distributed message queue based on these streams.

## Increasingly complex design


## Concepts
Our design will be based on Kafka, along with it comes with some concepts (other than a message ofc):
- Producer: Produce messages
- Consumer: Consume messages
- Topic: A group of messages, distinguished from other topic. 
- Consumer group: A group of consumers, subscribe to one topic. There can be multiple consumer group subscribe to the same topic.

### Message delivery
A message produced by a producer (topic A) will be replicated to multiple Consumer groups (topic A). Each consumer group will consist of multiple consumer, and the message will only be consumed by one of them 

Per [delivery semantic](https://docs.confluent.io/kafka/design/delivery-semantics.html#producer-delivery), there are 3 mains type of message delivery guarantee, each has its own usage scenario. To keep our design simple, we will only support "At most once" guarantee.


### Consumer group design
Kafka consumer group maintain a log of delivered messages and require to keep track of each consumer's current position. We only care about some additional data:
- Last committed offset: All consumer already seen and delivered this message.

You can read more in https://docs.confluent.io/kafka/design/consumer-design.html#consumer-position-illustrated

#### Fine grain control over message commitment
#### Fine grain control over message retention

## Overall design

Our design is a simplified version with the following design:
- Our guarantee is "at most once": Simply try to send to any of the alive consumer, lost message if failed
- We have a central process called `kadmin`: Handle all communication and replication to all other processes via TCP stream.
- Producers and consumers are separated processes (communicate with `kadmin` via TCP stream)
- Each consumer group maintain its own queue of messages, messages are put in the queue by the `kadmin`
- Each consumer also maintain its own queue, message are pop out when sent to other programs.

## Implementing `kadmin`
Our `kadmin` will handle:
- Registering a new producer (to a topic)
- Registering a new consumer group
- Registering a new consumer under a group
- Various other tasks on messaging

Needless to say this is the most important part of our system. We will prepare for subsequence features by grouping our functionality under a common structure.

First let's define a structure in a new file: `src/admin.zig`
```zig
const std = @import("std");
const net = std.net;

const KAdmin = struct {
    const Self = @This();

    pub fn init() Self {
        return Self{};
    }
};
```

### Spawn an admin TCP server
We allow other processes to communicate with our admin via TCP connection: Upon sending a message to the admin, it will receive an ACK back that the command it sent is effective.

Since we cannot multiplex a port for TCP, we will do the following:
- Open a TCP server on port `10000`
- Accept a connection
- Upon connect, receive a message
- Process the message then return the status to the sender
- Close the connection

Put everything in a while loop and we have can communicate with our admin from any machine.

Note: This design basically block on every admin message, but it's easier. We can do optimization later.

Implementation is as follow:
```zig
const std = @import("std");
const net = std.net;

pub fn readFromStream(stream_rd: *net.Stream.Reader) !?[]u8 {
		... Copy from the main file
}

pub fn writeToStream(stream_wr: *net.Stream.Writer, data: []u8) !void {
    ... Copy from the main file
}

const ADMIN_PORT: u16 = 10000;

const KAdmin = struct {
    const Self = @This();

    admin_address: net.Address,
    read_buffer: [1024]u8,
    write_buffer: [1024]u8,

    /// Init accept a buffer that will be used for all allocation and processing.
    pub fn init() Self {
        const address = try net.Address.parseIp4("127.0.0.1", ADMIN_PORT);
        return Self{
            .admin_address = address,
            .read_buffer = undefined,
            .write_buffer = undefined,
        };
    }

    /// Main function to start an admin server and wait for a message
    pub fn startAdminServer(self: *Self) !void {
        // Create a server on the address and wait for a connection
        var server = try self.admin_address.listen(.{ .reuse_address = true }); // TCP server
        const connection = try server.accept(); // Block until got a connection
        defer server.stream.close(); // Close the stream after

        // Init the read/write stream.
        var stream_rd = connection.stream.reader(&self.read_buffer);
        var stream_wr = connection.stream.writer(&self.write_buffer);

        // Read and process message
        if (try readFromStream(&stream_rd)) |message| {
            const return_data = self.processAdminMessage(message);
            try writeToStream(&stream_wr, return_data);
        }
    }

    /// Process a message sent to the admin process
    fn processAdminMessage(_: *Self, message: []u8) ![]u8 {
        // TODO: Process a message here, rather than just pong it back.
        const return_data = try std.fmt.allocPrint(std.heap.page_allocator, "I have received: {s}", .{message});
        return return_data;
    }
};
```
- Internally, we use 2 buffer: `read_buffer` and `write_buffer` to handle read and write.
- Upon init, create a constant address on the `ADMIN_PORT`
- `startAdminServer` will start the server, accept a connection, call the process function and close upon done.
- We will handle messages in `processAdminMessage`.

#### A note on allocation
We will often use `std.heap.page_allocator` as the default allocator anywhere required. This will make a syscall to the OS for every allocation and free and have some performance impact. This is deliberate to keep the program easier to follow, and we can replace these with another allocator of choice later.

### Multiple message type
 Currently, our `processAdminMessage` function just echo back the message to the client. We need to change our message format to support multiple message types:

- First byte byte will be the message length `n` (0->255)
- The next will be the message type (1 -> 255, sending 0 is a malformed message)
- Next n bytes will be the whole message (maximum 254 bytes).

Let's modify the `src/admin.zig` to reflect these changes:

```zig
const std = @import("std");
const net = std.net;

const ADMIN_PORT: u16 = 10000;
const MessageType = enum(u8) {
    ECHO = 1,
    // Other message type here
};

const Message = union(MessageType) {
    ECHO: []u8,
};

...

pub const KAdmin = struct {
		...
    /// Main function to start an admin server and wait for a message
    pub fn startAdminServer(self: *Self) !void {
        ...
        // Read and process message
        if (try readFromStream(&stream_rd)) |message| {
            const parsed_message = self.parseAdminMessage(message);
            if (parsed_message) |m| {
                if (try self.processAdminMessage(m)) |response_message| {
                    try writeToStream(&stream_wr, response_message);
                }
            } else {}
        }
    }

    /// Parse message into formatted Message
    fn parseAdminMessage(_: *Self, message: []u8) ?Message {
        switch (message[0]) {
            @intFromEnum(MessageType.ECHO) => {
                // Remove the first byte (it's type anyway)
                return Message{ .ECHO = message[1..] };
            },
            else => {
                // Do nothing here
                return null;
            },
        }
    }

    /// Parse a message sent to the admin process and call the correct processing function
    fn processAdminMessage(self: *Self, message: Message) !?[]u8 {
        switch (message) {
            MessageType.ECHO => |echo_message| {
                return try self.processEchoMessage(echo_message);
            },
        }
    }

    fn processEchoMessage(_: *Self, message: []u8) ![]u8 {
        const return_data = try std.fmt.allocPrint(std.heap.page_allocator, "I have received: {s}", .{message});
        return return_data;
    }
};
```

And then in `src/main.zig`, modify the main function as the following for testing:
```zig
const std = @import("std");
const net = std.net;
const kadmin = @import("admin.zig");

pub fn main() !void {
    if (std.mem.eql(u8, std.mem.span(std.os.argv[1]), "server")) {
        var admin = try kadmin.KAdmin.init();
        try admin.startAdminServer();
        // try spawnServer();
    } else {
        const port_str = std.mem.span(std.os.argv[2]); // 2nd argument is the port
        const port_int = try std.fmt.parseInt(u16, port_str, 10);
        // try clientConnectTCP(port_int);
        try clientConnectTCPAndEcho(port_int);
    }
}

pub fn writeEchoToStream(stream_wr: *net.Stream.Writer, data: []u8) !void {
    try stream_wr.interface.writeByte(@intCast(data.len + 1)); // Send how many byte written
    try stream_wr.interface.writeByte(1); // Send echo command
    try stream_wr.interface.writeAll(data);
    try stream_wr.interface.flush();
}

/// Test echo function
pub fn clientConnectTCPAndEcho(port: u16) !void {
    const address = try net.Address.parseIp4("127.0.0.1", port);
    var stream = try net.tcpConnectToAddress(address);
    // Read input from stdin and write to stream.
    var stdin_buf: [1024]u8 = undefined;
    var rd = std.fs.File.stdin().reader(&stdin_buf);
    var stream_read_buff: [1024]u8 = undefined;
    var stream_write_buff: [1024]u8 = undefined;
    var stream_rd = stream.reader(&stream_read_buff);
    var stream_wr = stream.writer(&stream_write_buff);
    while (try readLineFromStdin(&rd)) |line| {
        std.debug.print("Sent to server and echo command: {s}\n", .{line});
        try writeEchoToStream(&stream_wr, line);
        // Try to read back from the stream
        if (try readFromStream(&stream_rd)) |data| {
            std.debug.print("Received from server: {s}\n", .{data});
        }
    }
}
```

- Terminal 1, run the following:
```
zig build
./zig-out/bin/zig_kafka server
```
- Terminal 2, run:
```
./zig-out/bin/zig_kafka client 10000
```

Send a message in terminal 2 and check that it work like normal, but upon receive and echo the message, the server process just exited.

# 3. Register a producer
Our goal for the `producer` will be to send message without being blocked. This require us to create another TCP connection to the `kadmin` and send message via this channel:
- On init `producer`, create a TCP server on the input port.
- Send this info to the `kadmin` process by TCP connect to the `kadmin` and send the `PRODUCER_REGISTER` message that contain an address (currently only a port).
- Upon received a message to produce, send this message to the created TCP connection and wait for the ACK from `kadmin`.
- Connection is alive until the `producer` is gone.

Each `producer` will be creating its own TCP connection to the `kadmin` and act as a server.

## Message utility
First, we will create a utility to deal with message parsing and reading / writing with a stream: `src/message.zig`:
```zig
/// Utility to read/write to message from a stream
const std = @import("std");
const net = std.net;

pub const MessageType = enum(u8) {
    ECHO = 1,
    P_REG = 2, // Register a producer
    // Return message start at 100
    R_ECHO = 101,
    R_P_REG = 102,
};

pub const Message = union(MessageType) {
    ECHO: []u8,
    P_REG: []u8, // A string contain the port number
    R_ECHO: []u8, // Echo back the message
    R_P_REG: u8, // Just return a number as ack
};

fn parseMessage(message: []u8) ?Message {
    switch (message[0]) {
        @intFromEnum(MessageType.ECHO) => {
            return Message{ .ECHO = message[1..] };
        },
        @intFromEnum(MessageType.R_ECHO) => {
            return Message{ .R_ECHO = message[1..] };
        },
        @intFromEnum(MessageType.P_REG) => {
            return Message{ .P_REG = message[1..] };
        },
        @intFromEnum(MessageType.R_P_REG) => {
            return Message{ .R_P_REG = message[1] };
        },
        else => {
            // Do nothing here
            return null;
        },
    }
}

fn readFromStream(stream_rd: *net.Stream.Reader) !?[]u8 {
    const header = try stream_rd.file_reader.interface.takeByte();
    if (header != 0) {
        const data = try stream_rd.file_reader.interface.take(header);
        return data;
    } else {
        return null;
    }
}

/// Read a message from the stream
pub fn readMessageFromStream(stream_rd: *net.Stream.Reader) !?Message {
    const data = try readFromStream(stream_rd);
    if (data) |m| {
        return parseMessage(m);
    } else {
        return null;
    }
}

fn writeDataToStreamWithType(stream_wr: *net.Stream.Writer, mtype: u8, data: []u8) !void {
    try stream_wr.interface.writeByte(@intCast(data.len + 1)); // Send how many byte written
    try stream_wr.interface.writeByte(mtype); // Send the type
    try stream_wr.interface.writeAll(data);
    try stream_wr.interface.flush();
}

/// Write a message to the stream
pub fn writeMessageToStream(stream_wr: *net.Stream.Writer, message: Message) !void {
    switch (message) {
        MessageType.ECHO => |data| {
            try writeDataToStreamWithType(stream_wr, @intFromEnum(MessageType.ECHO), data);
        },
        MessageType.P_REG => |data| {
            try writeDataToStreamWithType(stream_wr, @intFromEnum(MessageType.P_REG), data);
        },
        MessageType.R_ECHO => |data| {
            try writeDataToStreamWithType(stream_wr, @intFromEnum(MessageType.R_ECHO), data);
        },
        MessageType.R_P_REG => |ack_byte| {
            var data: [1]u8 = [1]u8{ack_byte};
            try writeDataToStreamWithType(stream_wr, @intFromEnum(MessageType.R_P_REG), &data);
        },
    }
}
```

This is essentially just some utility functions to:
- `parseMessage`: Turn an `[]u8` into `Message`
- `readMessageFromStream`: Given a stream, read into a message format.
- `writeMessageToStream`: Write the message to stream.

We can refactor our `src/admin.zig` as follow:
```zig
const std = @import("std");
const net = std.net;
const message_util = @import("message.zig");

const ADMIN_PORT: u16 = 10000;

pub const KAdmin = struct {
    const Self = @This();

    admin_address: net.Address,
    read_buffer: [1024]u8,
    write_buffer: [1024]u8,

    /// Init accept a buffer that will be used for all allocation and processing.
    pub fn init() !Self {
        const address = try net.Address.parseIp4("127.0.0.1", ADMIN_PORT);
        return Self{
            .admin_address = address,
            .read_buffer = undefined,
            .write_buffer = undefined,
        };
    }

    /// Main function to start an admin server and wait for a message
    pub fn startAdminServer(self: *Self) !void {
        // Create a server on the address and wait for a connection
        var server = try self.admin_address.listen(.{ .reuse_address = true }); // TCP server
        const connection = try server.accept(); // Block until got a connection
        defer server.stream.close(); // Close the stream after

        // Init the read/write stream.
        var stream_rd = connection.stream.reader(&self.read_buffer);
        var stream_wr = connection.stream.writer(&self.write_buffer);

        // Read and process message
        if (try message_util.readMessageFromStream(&stream_rd)) |message| {
            if (try self.processAdminMessage(message)) |response_message| {
                try message_util.writeMessageToStream(&stream_wr, response_message);
            }
        }
    }

    /// Parse a message sent to the admin process and call the correct processing function
    fn processAdminMessage(self: *Self, message: message_util.Message) !?message_util.Message {
        switch (message) {
            message_util.MessageType.ECHO => |echo_message| {
                const response_data = try self.processEchoMessage(echo_message);
                return message_util.Message{
                    .R_ECHO = response_data,
                };
            },
            else => {
                // TODO: Process another message.
                return null;
            },
        }
    }

    fn processEchoMessage(_: *Self, message: []u8) ![]u8 {
        const return_data = try std.fmt.allocPrint(std.heap.page_allocator, "I have received: {s}", .{message});
        return return_data;
    }
};
```

And in `src/main.zig`, change to the following for testing:
```zig
const std = @import("std");
const net = std.net;
const kadmin = @import("admin.zig");
const message_util = @import("message.zig");

pub fn main() !void {
    if (std.mem.eql(u8, std.mem.span(std.os.argv[1]), "server")) {
        var admin = try kadmin.KAdmin.init();
        try admin.startAdminServer();
        // try spawnServer();
    } else {
        const port_str = std.mem.span(std.os.argv[2]); // 2nd argument is the port
        const port_int = try std.fmt.parseInt(u16, port_str, 10);
        // try clientConnectTCP(port_int);
        try clientConnectTCPAndEcho(port_int);
    }
}

/// Test echo function
pub fn clientConnectTCPAndEcho(port: u16) !void {
    const address = try net.Address.parseIp4("127.0.0.1", port);
    var stream = try net.tcpConnectToAddress(address);
    // Read input from stdin and write to stream.
    var stdin_buf: [1024]u8 = undefined;
    var rd = std.fs.File.stdin().reader(&stdin_buf);
    var stream_read_buff: [1024]u8 = undefined;
    var stream_write_buff: [1024]u8 = undefined;
    var stream_rd = stream.reader(&stream_read_buff);
    var stream_wr = stream.writer(&stream_write_buff);
    while (try readLineFromStdin(&rd)) |line| {
        std.debug.print("Sent to server and echo command: {s}\n", .{line});
        try message_util.writeMessageToStream(&stream_wr, message_util.Message{
            .ECHO = line,
        });
        // Try to read back from the stream
        if (try message_util.readMessageFromStream(&stream_rd)) |data| {
            std.debug.print("Received from server: {s}\n", .{data.R_ECHO});
        }
    }
}
```

## Register producer with `kadmin`

Let's create our `src/producer.zig`:
```zig
const std = @import("std");
const net = std.net;
const message_util = @import("message.zig");

const ADMIN_PORT: u16 = 10000;

pub const Producer = struct {
    const Self = @This();

    port: u16,
    read_buffer: [1024]u8,
    write_buffer: [1024]u8,

    pub fn init(port: u16) !Self {
        return Self{
            .port = port,
            .read_buffer = undefined,
            .write_buffer = undefined,
        };
    }
}
```
- `init` function allow input a port to initiate the TCP server.

Now, we send the init data to the `kadmin`:
```zig
pub const Producer = struct {
...
    pub fn sendPortDataToKAdmin(self: *Self) !void {
        // Connect to kadmin process
        const address = try net.Address.parseIp4("127.0.0.1", ADMIN_PORT);
        var stream = try net.tcpConnectToAddress(address);

        // Send register message to kadmin
        var stream_rd = stream.reader(&self.read_buffer);
        var stream_wr = stream.writer(&self.write_buffer);
        const port_str = try std.fmt.allocPrint(std.heap.page_allocator, "{}", .{self.port});
        std.debug.print("Sent to server the port: {s}\n", .{port_str});
        try message_util.writeMessageToStream(&stream_wr, message_util.Message{
            .P_REG = port_str,
        });
        // Try to read back the response from kadmin
        if (try message_util.readMessageFromStream(&stream_rd)) |res| {
            std.debug.print("Received ACK from server: {}\n", .{res.R_P_REG});
        }
        // Stream should be closed by the kadmin, no need to close ourselve.
    }
    ...
}
```

We can modify the `src/admin.zig` to add a producer to a local array list and ACK it back:
```zig
...
pub const KAdmin = struct {
    const Self = @This();

    admin_address: net.Address,
    read_buffer: [1024]u8,
    write_buffer: [1024]u8,

    // A list of producer address (port).
    producer_ports: std.ArrayList(u16),

    /// Init accept a buffer that will be used for all allocation and processing.
    pub fn init() !Self {
        const address = try net.Address.parseIp4("127.0.0.1", ADMIN_PORT);
        return Self{
            .admin_address = address,
            .read_buffer = undefined,
            .write_buffer = undefined,
            .producer_ports = try std.ArrayList(u16).initCapacity(std.heap.page_allocator, 10),
        };
    }
...
    /// Parse a message sent to the admin process and call the correct processing function
    fn processAdminMessage(self: *Self, message: message_util.Message) !?message_util.Message {
        switch (message) {
            message_util.MessageType.ECHO => |echo_message| {
                const response_data = try self.processEchoMessage(echo_message);
                return message_util.Message{
                    .R_ECHO = response_data,
                };
            },
            message_util.MessageType.P_REG => |producer_register_message| {
                const response = try self.processProducerRegisterMessage(producer_register_message);
                return message_util.Message{
                    .R_P_REG = response,
                };
            },
            else => {
                // TODO: Process another message.
                return null;
            },
        }
    }
...
    // 0 is good, 1 is bad
    fn processProducerRegisterMessage(self: *Self, port_str: []u8) !u8 {
        const port_int = std.fmt.parseInt(u16, port_str, 10) catch |err| {
            std.debug.print("Error parsing port from string, err = {any}", .{err});
            return 1;
        };
        self.producer_ports.append(std.heap.page_allocator, port_int) catch |err| {
            std.debug.print("Error registering producer, err = {any}", .{err});
            return 1;
        };
        // Debug print the list of registered producer:
        std.debug.print("Registered a producer, list of producer: {any}\n", .{self.producer_ports.items});
        return 0;
    }
};
```

In `src/main.zig`, add another start up processing for `producer` process:
```zig
const std = @import("std");
const net = std.net;
const kadmin = @import("admin.zig");
const message_util = @import("message.zig");
const producer = @import("producer.zig");

pub fn main() !void {
    if (std.mem.eql(u8, std.mem.span(std.os.argv[1]), "server")) {
        var admin = try kadmin.KAdmin.init();
        try admin.startAdminServer();
        // try spawnServer();
    } else if (std.mem.eql(u8, std.mem.span(std.os.argv[1]), "producer")) {
        const port_str = std.mem.span(std.os.argv[2]); // 2nd argument is the port
        const port_int = try std.fmt.parseInt(u16, port_str, 10);
        var p = try producer.Producer.init(port_int);
        try p.sendPortDataToKAdmin();
    } else {
        const port_str = std.mem.span(std.os.argv[2]); // 2nd argument is the port
        const port_int = try std.fmt.parseInt(u16, port_str, 10);
        // try clientConnectTCP(port_int);
        try clientConnectTCPAndEcho(port_int);
    }
}
```

You can test by the following run:
- Terminal 1 (Admin):
```
zig build
./zig-out/bin/zig_kafka server
```
- Terminal 2 (Producer 10001):
```
./zig-out/bin/zig_kafka producer 10001
```
- Terminal 3 (Producer 10002):
```
./zig-out/bin/zig_kafka producer 10002
```

## Start TCP server in `producer`
To allow non-blocking communication, `producer` will start a TCP server and allow `kadmin` to connect to it. This should be done before register a producer.
- Producer messages are sent via this stream instead of the main `kadmin` stream.

Start by modifying `src/producer.zig`:
```zig
pub const Producer = struct {
    const Self = @This();

    port: u16,
    read_buffer: [1024]u8,
    write_buffer: [1024]u8,

    // Local var after creating a TCP server
    server: net.Server,
    connection: net.Server.Connection,
...
    pub fn startProducerServer(self: *Self) !void {
        // Open the server
        const address = try net.Address.parseIp4("127.0.0.1", self.port);
        self.server = try address.listen(.{ .reuse_address = true }); // TCP server

        // If no error, then send the port to admin
        try self.sendPortDataToKAdmin();

        // After that accept a connection.
        self.connection = try self.server.accept(); // Block until got a connection

        // Later, we can use the self.connection to read / write message.
    }

    pub fn writeTestMessage(self: *Self, message: []u8) !void {
        // Init the read/write stream.
        var stream_rd = self.connection.stream.reader(&self.read_buffer);
        var stream_wr = self.connection.stream.writer(&self.write_buffer);
        // Write echo message
        try message_util.writeMessageToStream(&stream_wr, message_util.Message{
            .ECHO = try std.fmt.allocPrint(std.heap.page_allocator, "Producer port {}, message = {s}", .{ self.port, message }),
        });
        // Read back response echo message
        if (try message_util.readMessageFromStream(&stream_rd)) |m| {
            std.debug.print("Got back from the admin: {s}\n", .{m.R_ECHO});
        }
    }

    pub fn close(self: *Self) void {
        self.server.stream.close();
    }

}
```
- `startProducerServer` start the TCP server on the input port. Only after it success, it will send the port to `kadmin` to connect to it.
- After sent to `kadmin` and receive the ACK, will accept a connection and block until we have a connection.
- Upon having a connection, store it in `self.connection` , and it can be use to send any data to the `kadmin` without having to connect to the admin port, thus non-blocking.
- Also create a utility function `writeTestMessage` to test sending echo via the saved connection.

Next, we change to `src/admin.zig` to store the connected stream and process each register call.
```zig
pub const KAdmin = struct {
    const Self = @This();

    admin_address: net.Address,
    read_buffer: [1024]u8,
    write_buffer: [1024]u8,

    // A list of producer address (port).
    producer_ports: std.ArrayList(u16),
    producer_streams: std.ArrayList(net.Stream),
    producer_streams_state: std.ArrayList(u8),

    /// Init accept a buffer that will be used for all allocation and processing.
    pub fn init() !Self {
        const address = try net.Address.parseIp4("127.0.0.1", ADMIN_PORT);
        return Self{
            .admin_address = address,
            .read_buffer = undefined,
            .write_buffer = undefined,
            // Producer storage init
            .producer_ports = try std.ArrayList(u16).initCapacity(std.heap.page_allocator, 10),
            .producer_streams = try std.ArrayList(net.Stream).initCapacity(std.heap.page_allocator, 10),
            .producer_streams_state = try std.ArrayList(u8).initCapacity(std.heap.page_allocator, 10),
        };
    }
...
    /// Read from a connected producer at index.
    pub fn readFromProducer(self: *Self, index: usize) !void {
        // Don't have to read if it's already reading or closed previously.
        if (self.producer_streams_state.items[index] != 0) {
            return;
        }
        self.producer_streams_state.items[index] = 1;
        // Use the registered stream.
        var stream_read_buff: [1024]u8 = undefined;
        var stream_write_buff: [1024]u8 = undefined;
        var stream_rd = self.producer_streams.items[index].reader(&stream_read_buff);
        var stream_wr = self.producer_streams.items[index].writer(&stream_write_buff);
        // Read from the stream: Blocking until the stream is closed.
        while (true) {
            const read_result = message_util.readMessageFromStream(&stream_rd) catch |err| {
                switch (err) {
                    error.EndOfStream => {
                        // Producer closed the stream, no need to read again.
                        break;
                    },
                    else => {
                        return err;
                    },
                }
            };
            if (read_result) |message| {
                if (try self.processMessage(message)) |response_message| {
                    try message_util.writeMessageToStream(&stream_wr, response_message);
                }
            }
        }
        std.debug.print("Producer on port {} is gone\n", .{self.producer_ports.items[index]});
    }
...
    fn processProducerRegisterMessage(self: *Self, port_str: []u8) !u8 {
        // Parsing the port
        const port_int = try std.fmt.parseInt(u16, port_str, 10);
        // Connect to the server and add a stream to the list:
        const address = try net.Address.parseIp4("127.0.0.1", port_int);
        const stream = try net.tcpConnectToAddress(address);
        // Put into a list of producer
        try self.producer_ports.append(std.heap.page_allocator, port_int);
        try self.producer_streams.append(std.heap.page_allocator, stream);
        try self.producer_streams_state.append(std.heap.page_allocator, 0);
        // Debug print the list of registered producer:
        std.debug.print("Registered a producer, list of producer: {any}\n", .{self.producer_ports.items});
        return 0;
    }
}
```
- `readFromProducer` use the stored stream in `self.producer_streams.items[index]` and try to read message from it until it's closed (read return `EndOfStream` error)
- `processProducerRegisterMessage` will:
	- Connect to the port in the message
	- Append the port, stream, state to the internal `ArrayList`

We can modify the `src/main.zig` to test the new producer:
```zig
fn startProducer(admin: *kadmin.KAdmin, pos: usize) !void {
    try admin.readFromProducer(pos);
}

pub fn initKAdmin() !void {
    var admin = try kadmin.KAdmin.init();
    while (true) {
        try admin.startAdminServer();
        for (admin.producer_streams_state.items, 0..) |state, i| {
            if (state != 0) {
                continue;
            }
            // Spawn a thread to read from it.
            const thread_1 = try std.Thread.spawn(.{}, startProducer, .{ @as(*kadmin.KAdmin, &admin), @as(usize, i) });
            // TODO: Join it somewhere, but it's still auto clean up upon end of stream.
            _ = thread_1;
        }
    }
}

pub fn initProducer() !void {
    const port_str = std.mem.span(std.os.argv[2]); // 2nd argument is the port
    const port_int = try std.fmt.parseInt(u16, port_str, 10);
    var p = try producer.Producer.init(port_int);
    try p.startProducerServer();
    // Read input from stdin and write to the producer.
    var stdin_buf: [1024]u8 = undefined;
    var rd = std.fs.File.stdin().reader(&stdin_buf);
    while (try readLineFromStdin(&rd)) |line| {
        try p.writeTestMessage(line);
    }
    p.close();
}

pub fn main() !void {
    if (std.mem.eql(u8, std.mem.span(std.os.argv[1]), "server")) {
        try initKAdmin();
    } else if (std.mem.eql(u8, std.mem.span(std.os.argv[1]), "producer")) {
        try initProducer();
    } else {
        // TODO: Init other type of process
    }
}
```
- `initKAdmin` start the admin TCP server. Every time it close, we know that we already received an admin message.
- After that, loop through a list of registered producer, and for each of them spawn a thread to read from a producer.
- `initProducer` is used to initiate a producer on an input port. For testing purpose, we can write a line on stdin and it will be sent to the `kadmin` process.

You can test by the following run:
- Terminal 1 (Admin):
```
zig build
./zig-out/bin/zig_kafka server
```
- Terminal 2 (Producer 10001):
```
./zig-out/bin/zig_kafka producer 10001
```
- Terminal 3 (Producer 10002):
```
./zig-out/bin/zig_kafka producer 10002
```

And then input text into the Producer 10001 and Producer 10002 to see the effect. Input `Ctrl+C` on any of the producer to observer that the connection is gone.

With this setup, we can have:
- A `kadmin` process running on some machine,
- Some `producer`s on some other machine that can register itself and send message to the `kadmin`.

## Registering producer topic

First of all, our topic is identified by an `u32`. There's no need for special structure to store it (yet).

To simplify our design, each `producer` register message will be redesign to include the topic:
- First byte is still the message length
- Next byte is the message type
- Next 4 bytes is the topic
- The rest is the port.

We reflect this change first in the `src/message.zig`:
```zig
pub const MessageType = enum(u8) {
    ECHO = 1,
    P_REG = 2, // Register a producer
    // Return message start at 100
    R_ECHO = 101,
    R_P_REG = 102,
};

pub const ProducerRegisterMessage = struct {
    topic: u32,
    port: u16,

    const Self = @This();

    pub fn new(data: []u8) Self {
        // First 4 bytes is the topic
        const topic: u32 = std.mem.readInt(u32, data[0..4], .big);
        // Next 2 bytes is the port
        const port: u16 = std.mem.readInt(u16, data[4..6], .big);
        return ProducerRegisterMessage{
            .topic = topic,
            .port = port,
        };
    }

    pub fn convertToBytes(self: *const Self, buffer: []u8) ![]u8 {
        std.mem.writeInt(u32, buffer[0..4], self.topic, .big);
        std.mem.writeInt(u16, buffer[4..6], self.port, .big);
        return buffer[0..6];
    }
};

pub const Message = union(MessageType) {
    ECHO: []u8,
    P_REG: ProducerRegisterMessage, // A string contain the port number
    ...
};

fn parseMessage(message: []u8) ?Message {
    switch (message[0]) {
        ...
        @intFromEnum(MessageType.P_REG) => {
            return Message{ .P_REG = ProducerRegisterMessage.new(message[1..]) };
        },
        ...
    }
}
...
/// Write a message to the stream
pub fn writeMessageToStream(stream_wr: *net.Stream.Writer, message: Message) !void {
    switch (message) {
        ...
        MessageType.P_REG => |rm| {
            var buf: [1024]u8 = undefined;
            try writeDataToStreamWithType(stream_wr, @intFromEnum(MessageType.P_REG), try rm.convertToBytes(&buf));
        },
        ...
    }
}
```
- We introduce the `ProducerRegisterMessage` structure, with utility function to convert from and to bytes

Let's also refactor `src/producer.zig`:
```zig
pub const Producer = struct {
    const Self = @This();

    topic: u32,
    port: u16,
    read_buffer: [1024]u8,
    write_buffer: [1024]u8,

    // Local var after creating a TCP server
    server: net.Server,
    connection: net.Server.Connection,

    pub fn init(port: u16, topic: u32) !Self {
        return Self{
            .topic = topic,
            .port = port,
            .read_buffer = undefined,
            .write_buffer = undefined,
            .server = undefined,
            .connection = undefined,
        };
    }

    fn sendPortDataToKAdmin(self: *Self) !void {
        // Connect to kadmin process
        const address = try net.Address.parseIp4("127.0.0.1", ADMIN_PORT);
        var stream = try net.tcpConnectToAddress(address);

        // Send register message to kadmin
        var stream_rd = stream.reader(&self.read_buffer);
        var stream_wr = stream.writer(&self.write_buffer);
        std.debug.print("Sent to server the port: {}, topic: {}\n", .{ self.port, self.topic });
        try message_util.writeMessageToStream(&stream_wr, message_util.Message{
            .P_REG = message_util.ProducerRegisterMessage{
                .topic = self.topic,
                .port = self.port,
            },
        });
        // Try to read back the response from kadmin
        if (try message_util.readMessageFromStream(&stream_rd)) |res| {
            std.debug.print("Received ACK from server: {}\n", .{res.R_P_REG});
        }
        // Stream should be closed by the kadmin, no need to close ourselve.
    }
...
}
```
- Add the `topic` to the structure and `init` function, also send the correct data in `sendPortDataToKAdmin`.

For the `kadmin` to get and store the correct port + topic for a producer, we need to refactor `src/admin.zig` a bit:
```zig
pub const KAdmin = struct {
    const Self = @This();
    ...
    // A list of producer address (port).
    ...
    producer_topics: std.ArrayList(u32),

    /// Init accept a buffer that will be used for all allocation and processing.
    pub fn init() !Self {
        const address = try net.Address.parseIp4("127.0.0.1", ADMIN_PORT);
        return Self{
            ...
            .producer_topics = try std.ArrayList(u32).initCapacity(std.heap.page_allocator, 10),
        };
    }
...
    /// Parse a message and call the correct processing function
    fn processMessage(self: *Self, message: message_util.Message) !?message_util.Message {
        switch (message) {
            ...
            message_util.MessageType.P_REG => |producer_register_message| {
                const response = try self.processProducerRegisterMessage(&producer_register_message);
                return message_util.Message{
                    .R_P_REG = response,
                };
            },
            ...
        }
    }
...
    fn processProducerRegisterMessage(self: *Self, rm: *const message_util.ProducerRegisterMessage) !u8 {
        ...
        // Put into a list of producer
        ...
        try self.producer_topics.append(std.heap.page_allocator, rm.topic);
        ...
    }
};
```
- Register a topic in a separated `ArrayList`

Finally, support another CLI argument when starting up the producer in `src/main.zig`:
```zig
pub fn initProducer() !void {
    const port_str = std.mem.span(std.os.argv[2]); // 2nd argument is the port
    const port_int = try std.fmt.parseInt(u16, port_str, 10);
    const topic_str = std.mem.span(std.os.argv[3]); // 3rd argument is the topic
    const topic_int = try std.fmt.parseInt(u32, topic_str, 10);
    var p = try producer.Producer.init(port_int, topic_int);
    try p.startProducerServer();
    ...
}
```

You can test by the following run:
- Terminal 1 (Admin):
```
zig build
./zig-out/bin/zig_kafka server
```
- Terminal 2 (Producer 10001, topic 1):
```
./zig-out/bin/zig_kafka producer 10001 1
```
- Terminal 3 (Producer 10002, topic 2):
```
./zig-out/bin/zig_kafka producer 10002 2
```

# 4. Register a consumer group
In our design, each consumer group handle messages from a topic. 
- One topic can be managed by multiple consumer group.
- One topic can have many producer to that topic.

## Consumer group internal
A consumer group will consumer message from one topic. First, let's define it internal structure in `src/cgroup.zig`
```zig
const std = @import("std");

pub const CGroup = struct {
    const Self = @This();

    group_id: u32,
    group_topic: u32,

    pub fn new(id: u32, topic: u32) Self {
        return Self{
            .group_id = id,
            .group_topic = topic,
        };
    }
};
```
- Each consumer group are identified by a `group_id` and the `group_topic`.

Each consumer group should keep track list of consumer that it managed. Let's add that in:

```zig
const std = @import("std");
const net = std.net;

pub const CGroup = struct {
    ...
    // Consumers
    consumer_ports: std.ArrayList(u16),
    consumer_streams: std.ArrayList(net.Stream),
    consumer_streams_state: std.ArrayList(u8),

    pub fn new(id: u32, topic: u32) Self {
        return Self{
            ...
            // Consumers
            .consumer_ports = try std.ArrayList(u16).initCapacity(std.heap.page_allocator, 10),
            .consumer_streams = try std.ArrayList(net.Stream).initCapacity(std.heap.page_allocator, 10),
            .consumer_streams_state = try std.ArrayList(u8).initCapacity(std.heap.page_allocator, 10),
        };
    }
};
```

## Topic internal
Since messages on a topic are replicated to multiple consumer groups, each topic will have its own message queue, along with a list of consumer groups that consumer it.

### Message queue
A message queue is a Queue that store a lot of messages. Let's implement our own Queue and put it in a file `src/queue.zig`:

```zig
const std = @import("std");

/// Ring buffer with upfront max element it can hold.
/// Not growable and panic if not work
pub fn Queue(comptime T: type, max_n: comptime_int) type {
    return struct {
        arr: [max_n]?T,
        // [l..r)
        l: usize,
        r: usize,
        len: usize,

        const Self = @This();

        pub fn new() Self {
            return Self{
                // Remember this, very useful!
                .arr = [_]?T{null} ** max_n,
                .l = 0,
                .r = 0,
                .len = 0,
            };
        }

        pub fn front(self: *Self) ?T {
            return self.arr[self.l];
        }

        pub fn back(self: Self) ?T {
            const new_r = if (self.r == 0)
                max_n - 1
            else
                self.r - 1;
            return self.arr[new_r];
        }

        /// Push an element to the back of the deque
        pub fn push_back(self: *Self, element: *const T) void {
            // Add at r
            if (self.arr[self.r] != null) {
                @panic("Array filled and cannot add more element!");
            }
            self.arr[self.r] = element.*; // Deref to copy inside
            self.r += 1;
            self.len += 1;
            if (self.r >= max_n) self.r = 0;
        }

        /// Pop return an element from the front of the deque
        pub fn pop_front(self: *Self) ?T {
            // Get at l and move l forward
            if (self.arr[self.l] == null) {
                return null;
            }
            const pop_data = self.arr[self.l].?;
            self.arr[self.l] = null;
            self.len -= 1;
            self.l += 1;
            if (self.l >= max_n) {
                self.l = 0;
            }
            return pop_data;
        }

        /// Peek a position from the start
        pub fn peek(self: *const Self, pos: usize) ?T {
            if (pos >= self.len) {
                return null;
            }
            return self.arr[(self.l + pos) % max_n];
        }
    };
}
```
- We use `comptime` feature as a way to metaprogramming.
- The `Queue` is a function that return a type.

### Topic implementation

Let's define a topic structure in `src/topic.zig`:

```zig
const std = @import("std");
const queue = @import("queue.zig");

pub const Topic = struct {
    const Self = @This();
    const QueueType = queue.Queue([]u8, 10000);

    topic_id: u32,
    mq: QueueType,

    pub fn new(topic_id: u32) Self {
        return Self{
            .topic_id = topic_id,
            .mq = QueueType.new(),
        };
    }
};
```

We need to support registration of a consumer group. Suppose you have a `CGroup` structure, upon register you can push it into an ArrayList

```zig
const std = @import("std");
const queue = @import("queue.zig");
const CGroup = @import("cgroup.zig").CGroup;

pub const Topic = struct {
    const Self = @This();
    const QueueType = queue.Queue([]u8, 10000);

    topic_id: u32,
    mq: QueueType,
    cgroups: std.ArrayList(CGroup),
    cgroups_offset: std.ArrayList(usize),

    pub fn new(topic_id: u32) Self {
        return Self{
            .topic_id = topic_id,
            .mq = QueueType.new(),
            .cgroups = std.ArrayList(CGroup).initCapacity(std.heap.page_allocator, 10),
            .cgroups_offset = std.ArrayList(usize).initCapacity(std.heap.page_allocator, 10),
        };
    }

    /// Add a new consumer group that consume messages from this topic
    pub fn addCGroup(self: *Self, cgroup: *const CGroup) !void {
        try self.cgroups.append(std.heap.page_allocator, cgroup.*);
        try self.cgroups_offset.append(std.heap.page_allocator, 0); // First offset is 0
    }

    /// Push a new message to be consumed
    pub fn addMessage(self: *Self, message: []const u8) void {
        self.mq.push_back(&message);
    }
};
```

## Adding `topic` information to the `kadmin`
We first add the `topic` structure to the `src/admin.zig`:
```zig
const std = @import("std");
const net = std.net;
const message_util = @import("message.zig");
const topic = @import("topic.zig");

const ADMIN_PORT: u16 = 10000;

pub const KAdmin = struct {
    ...
    // A list of topic that the admin keeps track
    topics: std.ArrayList(topic.Topic),
    ...

    /// Init accept a buffer that will be used for all allocation and processing.
    pub fn init() !Self {
        const address = try net.Address.parseIp4("127.0.0.1", ADMIN_PORT);
        return Self{
            ...
            // Topics
            .topics = try std.ArrayList(topic.Topic).initCapacity(std.heap.page_allocator, 10),
            ...
        };
    }
...}
```

When a producer register with a topic, we can check if this topic existed in the list and add if not.

```zig
pub const KAdmin = struct {
...
    fn processProducerRegisterMessage(self: *Self, rm: *const message_util.ProducerRegisterMessage) !u8 {
        ...
        // Put into a list of producer
        ...
        // Add the topic if not exist
        var topic_exist = false;
        for (self.topics.items) |tp| {
            if (tp.topic_id == rm.topic) {
                topic_exist = true;
                break;
            }
        }
        if (!topic_exist) {
            self.topics.append(std.heap.page_allocator, topic.Topic.new(rm.topic));
        }
        ...
    }
...
}
```

# 5. Register a consumer
A consumer will need the following information:
- Consumer address (currently just a port)
- Consumer group (u32)
- Consumer topic (u32): A bit redundant, but will use it to simplify our design. 

Upon register a consumer on a topic, a consumer group can now send the message to one of them.

## Message design
Our message format will be as follow (keeping consistency with producer):
- First byte is the total data length
- Next 4 bytes is the topic,
- Next 2 bytes is the port,
- Last 4 bytes is the group ID.

Let's add a consumer register message in `src/message.zig`:
```zig
pub const MessageType = enum(u8) {
    ECHO = 1,
    P_REG = 2, // Register a producer
    C_REG = 3, // Register a consumer
    // Return message start at 100
    R_ECHO = 101,
    R_P_REG = 102,
    R_C_REG = 103,
};
...
pub const ConsumerRegisterMessage = struct {
    port: u16,
    topic: u32,
    group_id: u32,

    const Self = @This();

    /// Convert from a bytes message
    pub fn new(data: []u8) Self {
        // First 4 bytes is the topic
        const topic: u32 = std.mem.readInt(u32, data[0..4], .big);
        // Next 2 bytes is the port
        const port: u16 = std.mem.readInt(u16, data[4..6], .big);
        // Next 4 bytes is the group_id
        const group_id: u32 = std.mem.readInt(u32, data[6..10], .big);
        return ConsumerRegisterMessage{
            .topic = topic,
            .port = port,
            .group_id = group_id,
        };
    }
    
    pub fn convertToBytes(self: *const Self, buffer: []u8) ![]u8 {
        std.mem.writeInt(u32, buffer[0..4], self.topic, .big);
        std.mem.writeInt(u16, buffer[4..6], self.port, .big);
        std.mem.writeInt(u32, buffer[6..10], self.group_id, .big);
        return buffer[0..6];
    }
};

pub const Message = union(MessageType) {
    ECHO: []u8,
    P_REG: ProducerRegisterMessage,
    C_REG: ConsumerRegisterMessage,
    R_ECHO: []u8, // Echo back the message
    R_P_REG: u8, // Just return a number as ack
    R_C_REG: u8, // Just return a number as ack
};

fn parseMessage(message: []u8) ?Message {
    switch (message[0]) {
        ...
        @intFromEnum(MessageType.C_REG) => {
            return Message{ .C_REG = ConsumerRegisterMessage.new(message[1..]) };
        },
        @intFromEnum(MessageType.R_C_REG) => {
            return Message{ .R_C_REG = message[1] };
        },
        ...
    }
}
...
/// Write a message to the stream
pub fn writeMessageToStream(stream_wr: *net.Stream.Writer, message: Message) !void {
    switch (message) {
        ...
        MessageType.C_REG => |rm| {
            var buf: [1024]u8 = undefined;
            try writeDataToStreamWithType(stream_wr, @intFromEnum(MessageType.C_REG), try rm.convertToBytes(&buf));
        },
        MessageType.R_C_REG => |ack_byte| {
            var data: [1]u8 = [1]u8{ack_byte};
            try writeDataToStreamWithType(stream_wr, @intFromEnum(MessageType.R_C_REG), &data);
        },
    }
}
```

## Consumer structure
Similar to the `producer` we allow `consumer` to be a separated process. We defined its structure in `src/consumer.zig`:
```zig
const std = @import("std");
const net = std.net;
const message_util = @import("message.zig");

const ADMIN_PORT: u16 = 10000;

pub const Consumer = struct {
    const Self = @This();

    topic: u32,
    port: u16,
    group_id: u32,
    read_buffer: [1024]u8,
    write_buffer: [1024]u8,

    // Local var after creating a TCP server
    server: net.Server,
    connection: net.Server.Connection,

    pub fn init(port: u16, topic: u32, group_id: u32) !Self {
        return Self{
            .topic = topic,
            .port = port,
            .group_id = group_id,
            .read_buffer = undefined,
            .write_buffer = undefined,
            .server = undefined,
            .connection = undefined,
        };
    }

    fn sendInitDataToKAdmin(self: *Self) !void {
        // Connect to kadmin process
        const address = try net.Address.parseIp4("127.0.0.1", ADMIN_PORT);
        var stream = try net.tcpConnectToAddress(address);

        // Send register message to kadmin
        var stream_rd = stream.reader(&self.read_buffer);
        var stream_wr = stream.writer(&self.write_buffer);
        std.debug.print("Sent to server the port: {}, topic: {}, group_id: {}\n", .{ self.port, self.topic, self.group_id });
        try message_util.writeMessageToStream(&stream_wr, message_util.Message{
            .C_REG = message_util.ConsumerRegisterMessage{
                .topic = self.topic,
                .port = self.port,
                .group_id = self.group_id,
            },
        });
        // Try to read back the response from kadmin
        if (try message_util.readMessageFromStream(&stream_rd)) |res| {
            std.debug.print("Received ACK from server: {}\n", .{res.R_P_REG});
        }
        // Stream should be closed by the kadmin, no need to close ourselve.
    }

    pub fn startConsumerServer(self: *Self) !void {
        // Open the server
        const address = try net.Address.parseIp4("127.0.0.1", self.port);
        self.server = try address.listen(.{ .reuse_address = true }); // TCP server

        // If no error, then send the port to admin
        try self.sendInitDataToKAdmin();

        // After that accept a connection.
        self.connection = try self.server.accept(); // Block until got a connection

        // Later, we can use the self.connection to read / write message.
    }

    pub fn close(self: *Self) void {
        self.server.stream.close();
    }
};
```
This is essentially just a clone of the `src/producer.zig` with a few changes:
- Add `group_id` to init
- Change message sent to `kadmin` to `ConsumerRegisterMessage`

## Process the consumer register message
In `src/topic.zig`, we will support a function to add a consumer:
```zig
const std = @import("std");
const queue = @import("queue.zig");
const CGroup = @import("cgroup.zig").CGroup;
const net = std.net;

pub const Topic = struct {
...
    /// Add a new consumer to a consumer group with the given group ID.
    /// Assume exist (check outside not in this function)
    pub fn addConsumer(self: *Self, port: u16, stream: net.Stream, group_id: u32) !void {
        for (self.cgroups.items) |cg| {
            if (cg.group_id == group_id) {
                cg.consumer_ports.append(std.heap.page_allocator, port);
                cg.consumer_streams.append(std.heap.page_allocator, stream);
                cg.consumer_streams_state.append(std.heap.page_allocator, 0);
            }
        }
    }
...
}
```

In `src/admin.zig`, we want will first add a function to process the message:
```zig
pub const KAdmin = struct {
    /// Parse a message and call the correct processing function
    fn processMessage(self: *Self, message: message_util.Message) !?message_util.Message {
        switch (message) {
            ...
            message_util.MessageType.C_REG => |consumer_register_message| {
                const response = try self.processConsumerRegisterMessage(&consumer_register_message);
                return message_util.Message{
                    .R_C_REG = response,
                };
            },
            ...
        }
    }
...
    fn processConsumerRegisterMessage(self: *Self, rm: *const message_util.ConsumerRegisterMessage) !u8 {
        // Check if topic exist
        var exist = false;
        var topic_pos: usize = 0;
        for (self.topics.items, 0..) |tp, i| {
            if (tp.topic_id == rm.topic) {
                topic_pos = i;
                exist = true;
                break;
            }
        }
        if (!exist) {
            return 1; // We only accept known topic.
        }
        // Connect to the server
        const address = try net.Address.parseIp4("127.0.0.1", rm.port);
        const stream = try net.tcpConnectToAddress(address);
        // Add this data to the correct consumer group.
        // Check if consumer group with this ID exist, add if not
        exist = false;
        for (self.topics.items[topic_pos].cgroups.items) |cg| {
            if (cg.group_id == rm.group_id) {
                exist = true;
                break;
            }
        }
        if (!exist) {
            const new_group = cgroup.CGroup.new(rm.group_id, rm.topic);
            self.topics.items[topic_pos].addCGroup(&new_group);
        }
        // Add the port, stream and stream_state
        self.topics.items[topic_pos].addConsumer(rm.port, stream, rm.group_id);
        std.debug.print("Added a consumer with port: {}, topic: {}, group: {}", .{ rm.port, rm.topic, rm.group_id });
    }
};
```

## Start a consumer process
In `src/main.zig`, let's add a new process for `consumer`:
```zig
const std = @import("std");
const net = std.net;
const kadmin = @import("admin.zig");
const message_util = @import("message.zig");
const producer = @import("producer.zig");
const consumer = @import("consumer.zig");
...
pub fn initConsumer() !void {
    const port = try std.fmt.parseInt(u16, std.mem.span(std.os.argv[2]), 10); // 2nd argument is the port
    const topic = try std.fmt.parseInt(u32, std.mem.span(std.os.argv[3]), 10); // 3rd argument is the topic
    const group = try std.fmt.parseInt(u32, std.mem.span(std.os.argv[4]), 10); // 4th argument is the topic
    var c = try consumer.Consumer.init(port, topic, group);
    try c.startConsumerServer();
    // For now, immediately close it
}

pub fn main() !void {
    if (std.mem.eql(u8, std.mem.span(std.os.argv[1]), "server")) {
        try initKAdmin();
    } else if (std.mem.eql(u8, std.mem.span(std.os.argv[1]), "producer")) {
        try initProducer();
    } else if (std.mem.eql(u8, std.mem.span(std.os.argv[1]), "consumer")) {
        try initConsumer();
    } else {
        // TODO: Init other type of process
    }
}

```

You can test it with the following setup:
- Terminal 1 (`kadmin`):
```
zig build
./zig-out/bin/zig_kafka server
```

- Terminal 2 (`producer` port 10001, topic 1):
```
./zig-out/bin/zig_kafka producer 10001 1
```

- Terminal 3 (`consumer` port 10002, topic 1, group 100):
```
./zig-out/bin/zig_kafka consumer 10002 1 100
```

# 6. Sending messages (blocking)
Now we get to send messages from producer to our registered consumer! The setup:

- `producer` get the message from the `stdin`, then send it to `kadmin`
- `kadmin` replicate it and push it to many consumer group under the correct topic.
- Each consumer group send message to one of the `consumer`
- `consumer` write message to stdout.

## Message format

Currently, the `producer` is sending an ECHO message upon stdin, let's change that by introducing a new message type in `src/message.zig`
```zig
pub const MessageType = enum(u8) {
    ECHO = 1,
    P_REG = 2, // Register a producer
    C_REG = 3, // Register a consumer
    PCM = 4, // A message from producer that needs to be sent to consumer.
    // Return message start at 100
    R_ECHO = 101,
    R_P_REG = 102,
    R_C_REG = 103,
    R_PCM = 104,
};
...
pub const ProduceConsumeMessage = struct {
    producer_port: u16,
    timestamp: u64,
    message: []u8,

    const Self = @This();

    /// Convert from a bytes message
    pub fn new(data: []u8) Self {
        // First 2 bytes is the producer port
        const producer_port: u16 = std.mem.readInt(u16, data[0..2], .big);
        // Next 8 bytes is the timestamp in u64
        const ts: u64 = std.mem.readInt(u64, data[2..10], .big);
        // The rest is the message in []u8
        return ProduceConsumeMessage{
            .producer_port = producer_port,
            .timestamp = ts,
            .message = data[10..],
        };
    }

    pub fn convertToBytes(self: *const Self, buffer: []u8) ![]u8 {
        std.mem.writeInt(u16, buffer[0..2], self.producer_port, .big);
        std.mem.writeInt(u64, buffer[2..10], self.timestamp, .big);
        const message_len = self.message.len;
        @memcpy(buffer[10 .. 10 + message_len], self.message);
        return buffer[0 .. 10 + message_len];
    }
};

pub const Message = union(MessageType) {
    ECHO: []u8,
    P_REG: ProducerRegisterMessage,
    C_REG: ConsumerRegisterMessage,
    PCM: ProduceConsumeMessage,
    R_ECHO: []u8, // Echo back the message
    R_P_REG: u8, // Just return a number as ack
    R_C_REG: u8, // Just return a number as ack
    R_PCM: u8, // Just return a number as ack upon sent / receive
};

fn parseMessage(message: []u8) ?Message {
    switch (message[0]) {
        ...
        @intFromEnum(MessageType.PCM) => {
            return Message{ .PCM = ProduceConsumeMessage.new(message[1..]) };
        },
        @intFromEnum(MessageType.R_PCM) => {
            return Message{ .R_PCM = message[1] };
        },
        ...
    }
}
...
/// Write a message to the stream
pub fn writeMessageToStream(stream_wr: *net.Stream.Writer, message: Message) !void {
    switch (message) {
        ...
        MessageType.PCM => |pcm| {
            var buf: [1024]u8 = undefined;
            try writeDataToStreamWithType(stream_wr, @intFromEnum(MessageType.PCM), try pcm.convertToBytes(&buf));
        },
        MessageType.R_PCM => |ack_byte| {
            var data: [1]u8 = [1]u8{ack_byte};
            try writeDataToStreamWithType(stream_wr, @intFromEnum(MessageType.R_PCM), &data);
        },
    }
}
```
Our message format is:
- A producer that this message comes from (port)
- A timestamp of sent
- The rest are the message data.

## Sending message in `producer`
Upon receive a line from the `stdin`, the `producer` will then write the correct message to the stream. 
Let's reflect this change in the `src/producer.zig`:
```zig
pub const Producer = struct {
...
    /// Write the input message to the stream in the correct PCM format
    pub fn writeMessage(self: *Self, message: []u8) !void {
        // Create timestamp
        const ts: u64 = @intCast(std.time.timestamp());
        // Init the read/write stream.
        var stream_rd = self.connection.stream.reader(&self.read_buffer);
        var stream_wr = self.connection.stream.writer(&self.write_buffer);
        // Write echo message
        try message_util.writeMessageToStream(&stream_wr, message_util.Message{
            .PCM = message_util.ProduceConsumeMessage{
                .message = message,
                .producer_port = self.port,
                .timestamp = ts,
            },
        });
        // Read back response echo message
        if (try message_util.readMessageFromStream(&stream_rd)) |m| {
            std.debug.print("Got back from the admin: {s}\n", .{m.R_PCM});
        }
    }
...
}
```
- We create a new function called `writeMessage`: Write a message with the current timestamp, send to the `kadmin` and read back the response.

## Receive message in `kadmin`
We modify our code a bit in `src/admin.zig` to introduce functions that works with producer's messages:
```zig
pub const KAdmin = struct {
...
    /// Read from a connected producer at index.
    pub fn readFromProducer(self: *Self, index: usize) !void {
        ...
        // Read from the stream: Blocking until the stream is closed.
        while (true) {
            ...
            if (read_result) |message| {
                if (try self.processProducerMessage(message, index)) |response_message| {
                    try message_util.writeMessageToStream(&stream_wr, response_message);
                }
            }
        }
        ...
    }
    
    fn processProducerMessage(self: *Self, message: message_util.Message, producer_pos: usize) !?message_util.Message {
        switch (message) {
            message_util.MessageType.PCM => |pcm| {
                const response = try self.processPCM(&pcm, producer_pos);
                return message_util.Message{
                    .R_PCM = response,
                };
            },
            else => {
                // TODO: Process another message.
                return null;
            },
        }
    }
...
    fn processPCM(self: *Self, pcm: *const message_util.ProduceConsumeMessage, producer_pos: usize) !u8 {
        // Replicate this message to the correct topic
        for (self.topics.items) |*tp| {
            if (tp.topic_id == self.producer_topics.items[producer_pos]) {
                tp.addMessage(&pcm);
                return 0;
            }
        }
        return 1; // Cannot find the topic!
    }
}
```
- We make use of `processProducerMessage` and `processPCM` when a specific producer got a message. We also pass some information that can identify the producer in to these functions.
- Upon received the message from a `producer`, we put the message into the correct topic via `topic.addMessage`.

In `src/topic.zig`, we make a small modification:
```zig
...
    /// Push a new message to be consumed
    pub fn addMessage(self: *Self, message: *message_util.ProduceConsumeMessage) void {
        self.mq.push_back(message);
    }
...
```

Here's the bogus part: We're passing a slice of bytes to the topic, essentially a fat pointer (pointer with size). This pointer comes from the internal buffer when reading the producer message: `stream_read_buff`.
- Suppose you put this pointer to the queue, but then after several reads, this pointers got replaced by some other data. Then you are effectively reading wrong data!
- To solve this, allocate the data using an allocator and copy the content like so:
```zig
    fn processPCM(self: *Self, pcm: *const message_util.ProduceConsumeMessage, producer_pos: usize) !u8 {
        // Send this message to the correct topic
        for (self.topics.items) |*tp| {
            if (tp.topic_id == self.producer_topics.items[producer_pos]) {
                var copyData = try std.heap.page_allocator.create(message_util.ProduceConsumeMessage);
                copyData.producer_port = pcm.producer_port;
                copyData.timestamp = pcm.timestamp;
                copyData.message = try std.heap.page_allocator.alloc(u8, pcm.message.len);
                @memcpy(copyData.message, pcm.message);
                tp.addMessage(copyData);
                return 0;
            }
        }
        return 1; // Cannot find the topic!
    }
```
This leads to leak since we never free the `copyData`, and we will deal with it later.

## Send message to consumer
Our message is now sitting inside a queue in the topic, waiting to be sent replicated to a consumer group and send to a consumer.

When we process a consumer register message, we already opened the stream. When we need to send a message, simply write to the stream.

In the `src/cgroup.zig`, we can create a function to try and write to any of the consumer in the consumer group in a blocking manner:
```zig
const std = @import("std");
const net = std.net;
const message_util = @import("message.zig");

pub const CGroup = struct {
...
    /// Try to write a message to any consumer (block until consumed or error)
    pub fn writeMessageToAnyConsumer(self: *Self, message: message_util.ProducerRegisterMessage) !void {
        var global_err: ?anyerror = null;
        for (self.consumer_streams_state.items, 0..) |state, i| {
            if (state != 0) {
                continue; // Dead consumer
            }
            // Internal for write
            var write_buffer: [1024]u8 = undefined;
            var stream_wr = self.consumer_streams.items[i].writer(&write_buffer);
            message_util.writeMessageToStream(&stream_wr, message) catch |err| {
                // Cannot write somehow
                global_err = err;
                self.consumer_streams_state.items[i] = 1;
                continue;
            };
            // Internal for read: Have to read back the R_PCM
            var read_buffer: [1024]u8 = undefined;
            var stream_rd = self.consumer_streams.items[i].reader(&read_buffer);
            const response = message_util.readMessageFromStream(&stream_rd) catch |err| {
                // Cannot read back
                global_err = err;
                self.consumer_streams_state.items[i] = 1;
                continue;
            };
            if (response != null) {
                if (response.?.R_PCM == 0) {
                    return; // Written to one of them, done.
                }
            }
        }
        return global_err;
    }
};
```

## Replicate message to all consumer group
All consumer groups are in stored for a topic in `src/topic.zig`. We can defined a function to advance the offset of each of them in a blocking way:
```zig
const message_util = @import("message.zig");

pub const Topic = struct {
    const Self = @This();
    const QueueType = queue.Queue(message_util.ProduceConsumeMessage, 1000);
...
    fn advanceConsumerGroup(self: *Self, offset: usize, pos: usize) !void {
        const message = self.mq.peek(offset);
        if (message == null) {
            return;
        }
        // Write message at that offset
        self.cgroups.items[pos].writeMessageToAnyConsumer(message_util.Message{ .PCM = message.? }) catch |err| {
            return err;
        };
        // Advance the offset if good.
        self.cgroups_offset.items[pos] += 1;
    }

    pub fn advanceAllConsumerGroupBlocking(self: *Self) !void {
        if (self.cgroups_offset.items.len == 0) {
            return;
        }
        for (self.cgroups_offset.items, 0..) |offset, i| {
            try self.advanceConsumerGroup(offset, i);
        }
    }
```

## Consumer message
We add a function to consumer message in `src/consumer.zig`:
```zig
pub const Consumer = struct {
...
    /// Block until we received a PCM message.
    pub fn receiveMessage(self: *Self) !void {
        // Init the read/write stream.
        var stream_rd = self.connection.stream.reader(&self.read_buffer);
        var stream_wr = self.connection.stream.writer(&self.write_buffer);
        // Read PCM message
        if (try message_util.readMessageFromStream(&stream_rd)) |message| {
            // Debug print
            std.debug.print("Receive message {s} from producer {} at ts = {}", .{ message.PCM.message, message.PCM.producer_port, message.PCM.timestamp });
            // Write response message
            try message_util.writeMessageToStream(&stream_wr, message_util.Message{
                .R_PCM = 0,
            });
        }
    }
...
}
```

Let's start consume right away after start up for all consumer in `src/main.zig`
```zig
pub fn initConsumer() !void {
    ...
    try c.startConsumerServer();
    // Always try to receive message
    while (true) {
        try c.receiveMessage();
    }
    c.close();

}
```

In `src/admin.zig`, we make it so that after we received a consumer register message, we advance immediately with a `is_advancing` control flag:
```zig
/// Run this forever, so prefer to be in a thread.
pub fn advanceTopic(tp: *topic.Topic) !void {
    if (tp.is_advancing) {
        return;
    }
    std.debug.print("Start advancing... \n", .{});
    tp.is_advancing = true;
    while (true) {
        try tp.advanceAllConsumerGroupBlocking();
    }
}

pub const KAdmin = struct {
...
    fn processConsumerRegisterMessage(self: *Self, rm: *const message_util.ConsumerRegisterMessage) !u8 {
        // Check if topic exist
...
        // Add the port, stream and stream_state
        try self.topics.items[topic_pos].addConsumer(rm.port, stream, rm.group_id);
        // After start, can start advancing right away:
        const t = try std.Thread.spawn(.{}, advanceTopic, .{@as(*topic.Topic, &self.topics.items[topic_pos])});
        _ = t;
        return 0;
    }
...
}
```

You can start testing this setup by:
- Terminal 0 (`kadmin`):
```
./zig-out/bin/zig_kafka server
```
- Terminal 1 (`producer` port 11000, produce on topic 1):
```
./zig-out/bin/zig_kafka producer 11000 1
```
- Terminal 2 (`producer` port 10001, produce on topic 1):
```
./zig-out/bin/zig_kafka producer 10001 1
```
- Terminal 3 (`consumer` port 10002, topic 1 consumer group 100)
```
./zig-out/bin/zig_kafka consumer 10002 1 100
```
- Terminal 4 (`consumer` port 10003, topic 1 consumer group 101 to see that message replicate to this as well)
```
./zig-out/bin/zig_kafka consumer 10002 1 100
```

When you type a line into any of the `producer`, you can see that the 2 `consumers` shows messages like `Receive message Hello from producer 10001 at ts = 1766930660`...

# 7. Concurrency
## Concurrency design
Let's review the the `kadmin` process: Handling admin messages, get message from `producer`s and send to `consumer`s
	- On receive PCM messages from any `producer`, put these messages in a correct topic.
	- Each topic will try to send a message to any consumer in its own consumer group.

First, there are 3 jobs that needs to be done concurrently:
- Admin messages
- Receive producer messages
- Send messages to consumer group

### Concurrent producer processing
In `src/main.zig`, we did something pretty funny:
```zig
pub fn initKAdmin() !void {
    var admin = try kadmin.KAdmin.init();
    while (true) {
        try admin.startAdminServer();
        // Start all producer processes
        for (admin.producer_streams_state.items, 0..) |state, i| {
            if (state != 0) {
                continue;
            }
            // Spawn a thread to read from it.
            const thread_1 = try std.Thread.spawn(.{}, readProducer, .{ @as(*kadmin.KAdmin, &admin), @as(usize, i) });
            // TODO: Join it somewhere, but it's still auto clean up upon end of stream.
            _ = thread_1;
        }
    }
}
```
- Each time we receive a messages, we check if we need to spawn a new producer processing thread.

We can improve the design a bit: 
- When `kadmin` connected to a `producer` stream, immediately spawn a thread to read from it.
- When we're closing the `kadmin`, stop the all the stream. This would break the `readMessageFromStream`, allow the function to exit, and we can join all threads afterward.

Let's refactor `src/admin.zig` to reflect this change:
```zig
pub const KAdmin = struct {
...
    // A list of producer.
    ...
    producer_threads: std.ArrayList(std.Thread),
...
    pub fn init() !Self {
        const address = try net.Address.parseIp4("127.0.0.1", ADMIN_PORT);
        return Self{
            ...
            .producer_threads = try std.ArrayList(std.Thread).initCapacity(std.heap.page_allocator, 10),
        };
    }
...
    pub fn closeAdminServer(self: *Self) void {
        self.closeAllProducer();
    }

    fn closeAllProducer(self: *Self) void {
        // Close all open producer stream
        for (self.producer_streams_state.items, 0..) |st, i| {
            if (st == 0) {
                self.producer_streams.items[i].close();
            }
        }
        // Join all current open thread.
        for (self.producer_threads.items) |th| {
            th.join();
        }
    }
    /// Read from a connected producer at index.
    fn readFromProducer(self: *Self, index: usize) !void {
        // Don't have to read if it's already reading or closed previously.
        if (self.producer_streams_state.items[index] != 0) {
            return;
        }
        std.debug.print("Start reading from producer with port {}\n", .{self.producer_ports.items[index]});
...
    }
...
    fn processProducerRegisterMessage(self: *Self, rm: *const message_util.ProducerRegisterMessage) !u8 {
        ...
        // Debug print the list of registered producer:
        std.debug.print("Registered a producer, list of producer: {any}\n", .{self.producer_ports.items});
        // Upon register, just start consuming in another thread.
        const thread = try std.Thread.spawn(.{}, KAdmin.readFromProducer, .{ @as(*Self, self), @as(usize, self.producer_ports.items.len - 1) });
        try self.producer_threads.append(std.heap.page_allocator, thread);
        return 0;
    }
...
}
```

In `src/main.zig`, modify it to remove unused producer processing and add some close server process.
```zig
pub fn initKAdmin() !void {
    var admin = try kadmin.KAdmin.init();
    while (true) {
        try admin.startAdminServer();
    }
    defer admin.closeAdminServer();
}
```

#### Adding PCM message to the topic
Many producer can add PCM message to the same topic, so we need some way to sync the list of messages.

In `src/admin.zig`, our main process in the following function:
```zig
    fn processPCM(self: *Self, pcm: *const message_util.ProduceConsumeMessage, producer_pos: usize) !u8 {
        // Send this message to the correct topic
        for (self.topics.items) |*tp| {
            if (tp.topic_id == self.producer_topics.items[producer_pos]) {
                var copyData = try std.heap.page_allocator.create(message_util.ProduceConsumeMessage);
                copyData.producer_port = pcm.producer_port;
                copyData.timestamp = pcm.timestamp;
                copyData.message = try std.heap.page_allocator.alloc(u8, pcm.message.len);
                @memcpy(copyData.message, pcm.message);
                tp.addMessage(copyData);
                return 0;
            }
        }
        return 1; // Cannot find the topic!
    }
```
- `addMessage` push to a queue (ring buffer) that is not thread-safe

We can redesign it a bit:
- Try to acquire the topic lock
- After acquire, add it into the topic
- Release the lock

We will use a [std.Thread.RwLock](https://ziglang.org/documentation/0.15.2/std/#std.Thread.RwLock), it provide some functions:
- `pub fn lock(rwl: *RwLock) void`: Exclusive lock, use when want to acquire write lock, and corresponding `unlock`
- `pub fn lockShared(rwl: *RwLock) void`: Share lock, can use for reading case, and corresponding `unlockShared`

First, let's add a topic message lock in `src/topic.zig`:
```zig
pub const Topic = struct {
...
    is_advancing: bool,
    mq_lock: std.Thread.RwLock.Impl,
...
    pub fn new(topic_id: u32) !Self {
        return Self{
            ...
            .is_advancing = false,
            .mq_lock = std.Thread.RwLock.Impl{},
        };
    }
...
    /// Push a new message to be consumed
    pub fn addMessage(self: *Self, message: *message_util.ProduceConsumeMessage) void {
        self.mq_lock.lock(); // Block until acquire
        self.mq.push_back(message);
        self.mq_lock.unlock(); // Release
    }
...
```

### Concurrent consumer group processing
What should happens is:
- Each consumer group consume on their own without having to wait for other consumer group
- When consumed, the offset of this consumer group increased.
- After sometimes, another process should check all these offset and pop the message from the queue if all consumer group have move past it.

#### Queue redesign
Let's take a look at the last requirement:
- When popping message out of the queue, we have a acquire write lock on the queue.
- Then all offset should be -1 
	- That would require each consumer group's offset to be synced via a RWLock

Let's redesign our queue to make it more suitable and avoid too much lock: in `src/queue.zig`:
```zig
pub fn Queue(comptime T: type, max_n: comptime_int) type {
    return struct {
        arr: [max_n]?T,
        // [l..r)
        l: usize,
        r: usize,
        len: usize,
        pop_num: usize,
...
        /// Pop return an element from the front of the deque
        pub fn pop_front(self: *Self) ?T {
            // Get at l and move l forward
            if (self.arr[self.l] == null) {
                return null;
            }
            const pop_data = self.arr[self.l].?;
            self.arr[self.l] = null;
            self.len -= 1;
            self.l += 1;
            self.pop_num += 1;
            if (self.l >= max_n) {
                self.l = 0;
            }
            return pop_data;
        }

        /// Peek a position from the start
        pub fn peek(self: *const Self, pos: usize) ?T {
            const true_pos = pos - self.pop_num;
            if (true_pos >= self.len) {
                return null;
            }
            return self.arr[(self.l + true_pos) % max_n];
        }
    };
}
```

And in `src/topic.zig`:
```zig
    /// Add a new consumer group that consume messages from this topic
    pub fn addCGroup(self: *Self, cgroup: *const CGroup) !void {
        try self.cgroups.append(std.heap.page_allocator, cgroup.*);
        self.mq_lock.lockShared(); // Block until acquire
        try self.cgroups_offset.append(std.heap.page_allocator, self.mq.pop_num); // First offset is the number of popped element.
        self.mq_lock.unlockShared(); // Release
        std.debug.print("Added a consumer group: port = {}, topic = {} with offset 0\n", .{ cgroup.consumer_ports, self.topic_id });
    }
```

#### Advance each consumer group separately
Each of the consumer group advancement task needs to be put into a separated thread. We will start advancing immediately after adding consumer group (upon consumer register). In `src/admin.zig`:
```zig
pub const KAdmin = struct {
...
    // A list of topic that the admin keeps track
    topics: std.ArrayList(topic.Topic),
    topic_threads: std.ArrayList(std.Thread),
...
    pub fn closeAdminServer(self: *Self) void {
        self.closeAllProducer();
        self.closeAllTopic();
    }
...
    fn closeAllTopic(self: *Self) void {
        // Close all topic stream
        for (self.topics.items) |*tp| {
            for (tp.cgroups.items) |*cg| {
                for (cg.consumer_streams_state.items, 0..) |st, i| {
                    if (st == 0) {
                        cg.consumer_streams.items[i].close();
                    }
                }
            }
        }
        // Join all current open thread.
        for (self.topic_threads.items) |th| {
            th.join();
        }
    }
...
    fn processConsumerRegisterMessage(self: *Self, rm: *const message_util.ConsumerRegisterMessage) !u8 {
        ...
        // Add the port, stream and stream_state
        const pos = try self.topics.items[topic_pos].addConsumer(rm.port, stream, rm.group_id);
        // After start, can start advancing right away:
        const thread = try std.Thread.spawn(.{}, topic.Topic.advanceConsumerGroup, .{ @as(*topic.Topic, &self.topics.items[topic_pos]), pos });
        try self.topic_threads.append(std.heap.page_allocator, thread);
        return 0;
    }
...
}
```
- `processConsumerRegisterMessage` advance the consumer group right away with `advanceConsumerGroup` in another thread.

In `src/topic.zig`:
```zig
pub const Topic = struct {
    ...
    mq_lock: std.Thread.RwLock.Impl,
    topic_lock: std.Thread.RwLock.Impl,
...
    /// Add a new consumer group that consume messages from this topic
    pub fn addCGroup(self: *Self, cgroup: *const CGroup) !void {
        self.mq_lock.lockShared(); // Block until acquire
        defer self.mq_lock.unlockShared(); // Release on exit
        self.topic_lock.lock(); // Block cgroup for adding
        defer self.topic_lock.unlockShared(); // Release on exit
        try self.cgroups.append(std.heap.page_allocator, cgroup.*);
        try self.cgroups_offset.append(std.heap.page_allocator, self.mq.pop_num); // First offset is the number of popped element.
        std.debug.print("Added a consumer group: port = {}, topic = {} with offset 0\n", .{ cgroup.consumer_ports, self.topic_id });
    }


...
    pub fn advanceConsumerGroup(self: *Self, pos: usize) !void {
        while (true) {
            const offset = self.cgroups_offset.items[pos]; // Assume safe since only 1 thread can change the offset (this thread)
            const message = self.mq.peek(offset);
            if (message == null) {
                continue;
            }
            // Write message at that offset
            self.cgroups.items[pos].writeMessageToAnyConsumer(message_util.Message{ .PCM = message.? }) catch |err| {
                return err; // Return here since the error in unrecoverable
            };
            // Advance the offset if good.
            self.cgroups_offset.items[pos] += 1;
        }
    }
...
}
```
- `advanceConsumerGroup` always try to advance in a while loop. We use no lock here, since we can assume only 1 thread works on one consumer group always.
- `topic_lock` helps lock the topic when adding a new consumer group to the topic.

You can start testing this setup by:
- Terminal 0 (`kadmin`):
```
./zig-out/bin/zig_kafka server
```
- Terminal 1 (`producer` port 11000, produce on topic 1):
```
./zig-out/bin/zig_kafka producer 11000 1
```
- Terminal 2 (`producer` port 10001, produce on topic 1):
```
./zig-out/bin/zig_kafka producer 10001 1
```
- Terminal 3 (`consumer` port 10002, topic 1 consumer group 100)
```
./zig-out/bin/zig_kafka consumer 10002 1 100
```
- Terminal 4 (`consumer` port 10003, topic 1 consumer group 101 to see that message replicate to this as well)
```
./zig-out/bin/zig_kafka consumer 10002 1 100
```

#### Popping the queue
The above setup works, now we can try to remove the messages from the queue to save space. In `src/topic.zig`:
```zig
    pub fn tryPopMessage(self: *Self) !void {
        self.topic_lock.lock();
        if (self.is_advancing) {
            self.topic_lock.unlock();
            return;
        }
        self.is_advancing = true;
        self.topic_lock.unlock();
        while (true) {
            std.Thread.sleep(10 * 1000000000); // Every 10s
            self.topic_lock.lockShared(); // Need the number of consumer group to be stable
            var min_offset: usize = 1000000000;
            for (self.cgroups_offset.items) |offset| {
                min_offset = @min(min_offset, offset);
            }
            std.debug.print("Get to popping in topic {}, min_offset = {}, pop_num = {}\n", .{ self.topic_id, min_offset, self.mq.pop_num });
            self.mq_lock.lock(); // Lock to pop
            while (min_offset > self.mq.pop_num) {
                _ = self.mq.pop_front();
            }
            defer self.mq_lock.unlock();
            self.topic_lock.unlockShared(); // Unlock on done loop
        }
    }
```
- If already advancing, no need to do anything else.
- We first lock the topic to lock down the number of consumer groups.
- We find the minimum offset of all of them
	- Since the other thread can still consume message => increase the offset, our offset here is the "at least" number: The consumer group already consumed at least x messages.
	- This data race is desirable since we don't really need an exact number to operate and we can avoid locking each row.
- Run this every 10s
 
 When you add the topic, you can already start popping in another thread. In `src/admin.zig`:
```zig
     fn processConsumerRegisterMessage(self: *Self, rm: *const message_util.ConsumerRegisterMessage) !u8 {
     ...
        // After start, can start advancing right away:
        const thread = try std.Thread.spawn(.{}, topic.Topic.advanceConsumerGroup, .{ @as(*topic.Topic, &self.topics.items[topic_pos]), pos });
        try self.topic_threads.append(std.heap.page_allocator, thread);
        // Thread to start popping message from the topic queue
        const pop_thread = try std.Thread.spawn(.{}, topic.Topic.tryPopMessage, .{@as(*topic.Topic, &self.topics.items[topic_pos])});
        _ = pop_thread; // TODO: join with a way to cancel somehow on exit.
        return 0;
    }
```
- New `pop_thread` spawned for every topic to deal with popping the queue

## Simulation of fast produce slow consume
Let's first try to simulate a fast/slow consumption situation to check if it's working as intended:
- 1 producer, each send a "Ping" message after 1s.
- 2 Consumer groups
	- Group 100 consume at a rate 1 per 1s
	- Group 101 consume at a rate 1 per 2s

We will change the `src/main.zig` at a start of the `producer` / `consumer`:
```zig
pub fn initProducer() !void {
    const port_str = std.mem.span(std.os.argv[2]); // 2nd argument is the port
    const port_int = try std.fmt.parseInt(u16, port_str, 10);
    const topic_str = std.mem.span(std.os.argv[3]); // 3rd argument is the topic
    const topic_int = try std.fmt.parseInt(u32, topic_str, 10);
    var p = try producer.Producer.init(port_int, topic_int);
    try p.startProducerServer();
    // Don't read from stdin anymore! Just run forever!
    // Read input from stdin and write to the producer.
    // var stdin_buf: [1024]u8 = undefined;
    // var rd = std.fs.File.stdin().reader(&stdin_buf);
    // while (try readLineFromStdin(&rd)) |line| {
    //     try p.writeMessage(line);
    // }
    while (true) {
        std.Thread.sleep(1 * 1000000000);
        try p.writeMessage(try std.fmt.allocPrint(std.heap.page_allocator, "Ping from {}", .{port_int}));
    }
    p.close();
}

pub fn initConsumer() !void {
    const port = try std.fmt.parseInt(u16, std.mem.span(std.os.argv[2]), 10); // 2nd argument is the port
    const topic = try std.fmt.parseInt(u32, std.mem.span(std.os.argv[3]), 10); // 3rd argument is the topic
    const group = try std.fmt.parseInt(u32, std.mem.span(std.os.argv[4]), 10); // 4th argument is the topic
    const sleep_mili = try std.fmt.parseInt(u64, std.mem.span(std.os.argv[5]), 10); // 5th argument is the sleep time in milli
    var c = try consumer.Consumer.init(port, topic, group);
    try c.startConsumerServer();
    // Always try to receive message
    while (true) {
        std.Thread.sleep(sleep_mili * 1000 * 1000);
        try c.receiveMessage();
    }
    c.close();
}
```

Then you can play around with this setup:
- Terminal 0: `kadmin`
```
zig build
./zig-out/bin/zig_kafka server
```

- Terminal 1: `producer` on topic `1`
```
./zig-out/bin/zig_kafka producer 10001 1
```

- Terminal 2: `consumer` on topic `1`, group `100`, poll every `1s`
```
./zig-out/bin/zig_kafka consumer 10002 1 100 1000
```

- Terminal 3: `consumer` on topic `1`, group `101`, poll every `2s`
```
./zig-out/bin/zig_kafka consumer 10002 1 101 2000
```

- Terminal 4: `consumer` on topic `1`, group `101`, poll every `2s` (only different port)
```
./zig-out/bin/zig_kafka consumer 10003 1 101 2000
```

You might see something funny: Consumer group `101` only make use of 1 consumer, even if we have 2 consumers in that group!

# 8. Optimization (1)
## Pull model for consumer
### Current push model
In `src/cgroup.zig`, `writeMessageToAnyConsumer` will try to write to a consumer in sequential order:
```zig
    /// Try to write a message to any consumer (block until consumed or error)
    pub fn writeMessageToAnyConsumer(self: *Self, message: message_util.Message) !void {
        var global_err: ?anyerror = null;
        for (self.consumer_streams_state.items, 0..) |state, i| {
            if (state != 0) {
                continue; // Dead consumer
            }
...
```

The problem is each consumer is stream is free to write, but then we want it to ACK us back to keep processing. This is a "Push" model: We push message to the consumer and expect it to consume right away, block until they do.

### Pull model design
In pull model, consumer will say that "I'm ready" to the `kadmin`, and then we can start sending messages. We can design this flow with a "ready consumers queue":
- Each consumer send a message to the stream (2 bytes) on ready.
- In `kadmin`, each time we receive a consumer register, we also spawn a thread to check for it ready status.
	- Just take 2 bytes on the stream (blocking)
	- On take, ACK it back to the consumer, push it to the "ready consumers queue".
	- Return from the thread, the thread will be gone / clean up
- When we want to send message, we simply use one of the "ready consumers queue" consumer
	- Upon sent successfully (got the ack), immediately spawn a thread to check for status.

Let's first redesign the `src/consumer.zig`:
```zig
    /// Block until the kadmin accept our ready message.
    pub fn sendReadyMessage(self: *Self) !void {
        // Init the read/write stream.
        var stream_rd = self.connection.stream.reader(&self.read_buffer);
        var stream_wr = self.connection.stream.writer(&self.write_buffer);
        // Send the ready message
        try message_util.writeMessageToStream(&stream_wr, message_util.Message{
            .C_RD = 0,
        });
        std.debug.print("Sent ready to kadmin for consumer on port {}", .{self.port});
        // Read ACK message
        if (try message_util.readMessageFromStream(&stream_rd)) |_| {
            // Debug print
            std.debug.print("Admin ack the ready", .{});
        }
    }
```

We can add a process function in `src/cgroup.zig`:
```zig
    pub fn processReadyMessageFromConsumer(self: *CGroup, c_pos: usize) !void {
        // Internal for read and write
        var read_buffer: [1024]u8 = undefined;
        var write_buffer: [1024]u8 = undefined;
        var stream_rd = self.consumer_streams.items[c_pos].reader(&read_buffer);
        var stream_wr = self.consumer_streams.items[c_pos].writer(&write_buffer);
        // This will block until read
        if (try message_util.readMessageFromStream(&stream_rd)) |_| {
            std.debug.print("Got a ready from consumer {}\n", .{self.consumer_ports.items[c_pos]});
            self.ready_lock.lock();
            defer self.ready_lock.unlock();
            std.debug.print("Locked the ready queue\n", .{});
            self.ready_consumer_mq.push_back(&c_pos);
            try message_util.writeMessageToStream(&stream_wr, message_util.Message{
                .R_C_RD = 0,
            });
            std.debug.print("Admin ACK the ready for consumer {}\n", .{self.consumer_ports.items[c_pos]});
        }
    }

    /// Try to write a message to any consumer (block until consumed or error)
    pub fn writeMessageToAnyConsumer(self: *Self, message: message_util.Message) !void {
        var global_err: ?anyerror = null;
        // Loop forever and get the first ready in the queue
        while (true) {
            self.ready_lock.lock();
            const maybe_p = self.ready_consumer_mq.pop_front();
            if (maybe_p) |i| {
                std.debug.print("Start writing to consumer at pos {}\n", .{i});
                // Internal for write
                var write_buffer: [1024]u8 = undefined;
                var stream_wr = self.consumer_streams.items[i].writer(&write_buffer);
                message_util.writeMessageToStream(&stream_wr, message) catch |err| {
                    // Cannot write somehow
                    global_err = err;
                    self.consumer_streams_state.items[i] = 1;
                    continue;
                };
                // Internal for read: Have to read back the R_PCM
                var read_buffer: [1024]u8 = undefined;
                var stream_rd = self.consumer_streams.items[i].reader(&read_buffer);
                const response = message_util.readMessageFromStream(&stream_rd) catch |err| {
                    // Cannot read back
                    global_err = err;
                    self.consumer_streams_state.items[i] = 1;
                    continue;
                };
                if (response != null) {
                    if (response.?.R_PCM == 0) {
                        std.debug.print("Got a R_PCM ack back from {}\n", .{i});
                        self.ready_lock.unlock();
                        // Spawn a thread to process ready message again.
                        const th = try std.Thread.spawn(.{}, CGroup.processReadyMessageFromConsumer, .{ self, i });
                        _ = th; // No need to join
                        return; // Written to one of them, done.
                    }
                } else {
                    // TODO: Process error here
                    self.ready_lock.unlock();
                    return;
                }
            } else {
                self.ready_lock.unlock();
            }
        }
        return global_err.?;
    }
```

In `src/topic.zig`, when adding a consumer, can start the ready check right away:
```zig
    /// Add a new consumer to a consumer group with the given group ID and return the consumer position.
    /// Assume exist (check outside not in this function)
    pub fn addConsumer(self: *Self, port: u16, stream: net.Stream, group_id: u32) !usize {
        for (self.cgroups.items, 0..) |*cg, i| {
            if (cg.group_id == group_id) {
                std.debug.print("Added a consumer with port: {}, topic: {}, group: {}\n", .{ port, self.topic_id, group_id });
                try cg.consumer_ports.append(std.heap.page_allocator, port);
                try cg.consumer_streams.append(std.heap.page_allocator, stream);
                try cg.consumer_streams_state.append(std.heap.page_allocator, 0);
                // Spawn a thread to process ready message right after add.
                const th = try std.Thread.spawn(.{}, CGroup.processReadyMessageFromConsumer, .{ cg, cg.consumer_ports.items.len - 1 });
                _ = th; // No need to join
                return i;
            }
        }
        return 0;
    }
```

Let's test the previous setup again:
- Terminal 0: `kadmin`
```
zig build
./zig-out/bin/zig_kafka server
```

- Terminal 1: `producer` on topic `1`
```
./zig-out/bin/zig_kafka producer 10001 1
```

- Terminal 2: `consumer` on topic `1`, group `100`, poll every `1s`
```
./zig-out/bin/zig_kafka consumer 11001 1 100 1000
```

- Terminal 3: `consumer` on topic `1`, group `101`, poll every `2s`
```
./zig-out/bin/zig_kafka consumer 12001 1 101 2000
```

- Terminal 4: `consumer` on topic `1`, group `101`, poll every `2s` (only different port)
```
./zig-out/bin/zig_kafka consumer 12002 1 101 2000
```

You can observe that if you add more consumer to a consumer group, you get more consume throughput.

## Partitioning the consumer
Previous scheme used a round-robin approach: Whichever consumer is available, we will use it next. When a consumer becomes ready, we send it to the end of the ready-queue.

https://www.tigerdata.com/learn/data-partitioning-what-it-is-and-why-it-matters defined this as the "Round-robin partitioning".

Another way to partition the consumer is to use "Hash partitioning". We can have a simple strategy as follow:
TODO: 

## Refactor: Topic organization

# 9. Recovery
## Persistent
In our setup, everything is in memory. If the server is restarted (physical error), then our progress is lost.
To avoid this, we will have to:
- Persist our state to disk and track current progress
- And reload (start back from the current state)

### Persistent design
#### State
How do we write and load back the current state? Let's first figure out what consist of the current state:
- List of producers
- List of topics
- List of messages for each topic
- List of consumer group + offset
- List of consumer for each group

Since restart, what we lost most is the stream to the `producer` / `consumer`, so let's remove the list of producer / consumer: The stream is lost anyway, we can let the `consumer` / `producer` to send the register and start again.

What we're left with:
- Topics
	- Topic ID
	- Messages in the queue
	- List of consumer group
- Consumer groups
	- Group ID
	- Topic ID
	- Offset

#### Operation
Consumer group is simple: Each only contain a small amount of data, so we can just write back the state when offset is updated (consumed).
- However, since messages are consumed very fast, we can split the offset into its own store.
- And the consumer only have group ID + topic ID, which the topic already keeps track!

Topic data can split in 2 parts:
- Topic ID and List of consumer group (id only): This is supposed to not change frequently, so it make sense to persist it using state.
- List of messages

TODO: Research how to persist messages via operation.



### Persistent implementation
#### Storing non-frequent states
We will use a single file to store our states. Imagine a json format:
```json
{
	"topics": [
		{
			"topic_id": 123,
			"cgroup_ids": [1,2,3,...],
		},
		{...}
	],
}
```

This format store the state of our `topics`, which we can use to recreate a list of topics and consumer groups.

#### Storing operation
##### Offset operation
When you need to change an offset for a consumer group, you can append the new offset (4 bytes since it's an u32) to a file named `cg_offset_<cg_id>` like `cg_offset_100`.

##### Messages operation
TODO: Figure this out

### Reload on crash
#### Load from disk
TODO: Figure this out
#### Reconnect from client
TODO: ?

## Dead letter queue
When the consumer has a problem consuming a message, we support a mechanism to push this message to a special queue for a different processing path.

# 10. Produce and consume interface
Currently setup:
- `producer` get the message from the `stdin`, then send it to `kadmin`
- `kadmin` replicate it and push it to many consumer group.
- Each consumer group send message to one of the `consumer`
- `consumer` write message to stdout.

What we want is to allow user to producer a message and receive the message programmatically. For this, we provide an interface that allow users to:
- For `producer`: Produce a message.
- For `consumer`: Get a message.

For this to happens, we need to:
- Provide functions that create a producer / consumer on a random open port
- Send this message to a preconfig `kadmin` address
- Provide functions to write / read from the stream.
- For consumer, provide interfaces that call a callback function upon received a message.

## Producer interface
## Consumer interface
## Fault tolerant
### Error handling
We use `try` every where in the code, one error and the whole things are broken. Probably not a good idea.
### Graceful termination
SIGTERM will not join thread and close stream.

# 11. Optimization (2)
## Async I/O
First, move the language to 0.16.0-dev and change semantic to `std.Io` for all function.

### Async vs Multithreading
Async is best for I/O-bound tasks, while multithreading uses multiple OS threads for true parallel execution, better for CPU-bound tasks.

#### Multithreading
Suppose you have a list of tasks that needs to be completed in any order, upon completion of any task, you do something. You can spawn a thread for each tasks, work on it, and on completion put your result somewhere (usually a sync queue).

Imagine these tasks are I/O-bound tasks: It reads from other I/O source, blocking and wait for completion. It doesn't cost much CPU to work on it (just wait for the other side to complete), yet we have to spend so much time idling and cost the CPU cycle to just wait.

#### Async model
Async make use of an "event loop" that check for ready events and process them. I/O task are sent to the background, and upon completion, send a notification to us that a task has been completed.

The main different is that this task is sent to the background (kernel space) for **waiting**.

- NIO: Non-blocking I/O (epoll, kqueue): Readiness-based model
- AIO: True Async I/O (native aio, io_uring): Completion-based model

TODO: ???

### `io_uring`
High-performance completion-based asynchronous I/O framework for the Linux kernel.

In this model, there's a `sq` and a `cq`. TODO write more here

More information can be found in https://deepwiki.com/axboe/liburing/

#### Efficiency commands and setup
Linux 6. bring many new improvement to `io_uring`, specifically multishot ability.

https://unixism.net/loti/tutorial/sq_poll.html, `IORING_SETUP_SQPOLL` to avoid the `submit` / `enter`, but need to prep and register some file descriptor beforehand, which is more advance (but it reduce #syscall to make the system even more efficient).

### Simple TCP echo
#### Commands
A TCP server needs some ingredients:
- An IP address on a port -> Start a TCP server on it
	- Basically bind to this port, return a socket, which is a file descriptor that support read / write.
- Listen for incoming connection.
- `accept` an incoming connection. This `accept` call is a blocking I/O task, you just wait for the other side to connect, not much to do.

To work with `io_uring`, it's best to get to know the linux command. Our first command will be [accept(2)](https://man7.org/linux/man-pages/man2/accept.2.html). Let's take a look at the input / output:
- Input:
	- `sockfd`: File descriptor of the server socket.
	- ... other stuff lol
- Output:
	- A file descriptor of the accepted socket. This then can be use for read / write with client.

Next, we take a look at the library's `io_uring` capability in [std.os.linux.IoUring](https://ziglang.org/documentation/master/std/#std.os.linux.IoUring). There's an `accept` command with the same input and output, so we can use it.

Lastly, we will be using the `accept_multishot` version to avoid having to send the `accept` command again after done.

#### Flow of `io_uring`
1. Obviously we need to initiate the ring. You can use `init` or `init_params` for better control.
2. After init, you can queue a task. Our task can be queue using any of the "queue (but does not submit)"..., in our case it's `accept_multishot`.
	- Tasks are put into the `sq`
3. After queuing all your tasks, you can call either `submit` or `enter`. You can think of `enter` as a `submit` with better params control. 
	- This will let the kernel start waiting for these tasks, on completion will put into the `cq`.
4. In an event loop, you can get the completed tasks using `copy_cqe` or `copy_cqes` if you want batching.
5. You can queue then submit task again anywhere, even in the event loop should needed.

#### Put it together
```zig
fn uringStartEchoServer() !void {
    // Some init stuff
    const gpa = std.heap.smp_allocator;
    var threaded: std.Io.Threaded = .init(gpa, .{});
    defer threaded.deinit();
    const io = threaded.io();
    var read_buf: [1024]u8 = undefined;
    var write_buf: [1024]u8 = undefined;

    // IO Uring stuff
    var ring = try iou.init(1 << 7, 0);

    // Start server
    const address = try net.IpAddress.parseIp4("127.0.0.1", ADMIN_PORT);
    var server = try address.listen(io, .{ .reuse_address = true }); // TCP server
    std.debug.print("Server bind socket at {any}\n", .{server.socket});

    // accept: https://man7.org/linux/man-pages/man2/accept.2.html
    // fd: server.socket.handle: to accept from
    // addr and addrlen is to be filled with data. If provide, data will be fill when peer connect.
    // return error and a new file descriptor for the accepted socket
    // Multishot allows 1 submission -> multiple completion entry, so we don't have to resubmit.

    var addr: std.posix.sockaddr = undefined;
    var addr_len: std.posix.socklen_t = @sizeOf(std.posix.sockaddr);
    _ = try ring.accept_multishot(10, server.socket.handle, &addr, &addr_len, 0);
    const count = try ring.submit();
    std.debug.print("Tasks submitted: {}\n", .{count});

    // Event loop (wait until completion queue has something)
    while (true) {
        // Will wait until we have an entry.
        const comp_entry = try ring.copy_cqe();
        const err = comp_entry.err();
        if (err == .SUCCESS) {
            if (comp_entry.user_data == 10) {
                // The correct user data. Can be anything
                // You can use @intFromPtr and @ptrFromInt to pass in the pointer and check type.
                const fd = comp_entry.res; // The fd for the accepted socket (read / write using it)
                std.debug.print("Something connected: addr = {any}, the return FD = {}\n", .{ addr, fd });
                // New stream, with the accepted socket.
                var stream = net.Stream{ .socket = net.Socket{ .handle = fd, .address = address } };
                var rd = stream.reader(io, &read_buf);
                var wr = stream.writer(io, &write_buf);
                if (try message_util.readMessageFromStream(&rd)) |res| {
                    const data = res.ECHO;
                    std.debug.print("Got client message: {s}\n", .{data});
                    try message_util.writeMessageToStream(&wr, message_util.Message{
                        .R_ECHO = data,
                    });
                }
                stream.close(io); // Close and wait for another accept. Avoid create a new fd every time a new client connect.

            }
        } else {
            std.debug.print("Err = {any}\n", .{err});
        }
    }
}
```

### Simple TCP read until close
#### Commands
Already have a connection, now we want to read from the stream until it's closed.
The command is [recv(2)](https://man7.org/linux/man-pages/man2/recv.2.html)

#### `recv_multishot` flow
`recv(2)` will notify us when we read from an `fd`, and the task ended there. We either have to re-issue the `recv`, or we can use the multishot version:

1. Multishot version of `recv` required a `BufferGroup`.
2. On init the `BufferGroup`, it can queue a `recv_multishot`. You can `submit` or `enter` after queue.
3. Same as before, in an event loop, you can use `copy_cqe` to get the ready entry `cqe`
4. On ready, use `BufferGroup.get(cqe)` to get the buffer that has the data for this `cqe`. This get the buffer from the kernel space and put it in the user space for usage.
5. After you are done with the buffer, use `BufferGroup.put(cqe)` to put the buffer back to the kernel space.

#### Put it together
```zig
fn uringStartReadServer() !void {
    // Some init stuff
    const gpa = std.heap.smp_allocator;
    var threaded: std.Io.Threaded = .init(gpa, .{});
    defer threaded.deinit();
    const io = threaded.io();
    // var read_buf: [1024]u8 = undefined;
    var write_buf: [1024]u8 = undefined;

    // IO Uring stuff
    var ring = try iou.init(1 << 7, 0);

    // Start server
    const address = try net.IpAddress.parseIp4("127.0.0.1", ADMIN_PORT);
    var server = try address.listen(io, .{ .reuse_address = true }); // TCP server
    std.debug.print("Server bind socket at {any}\n", .{server.socket});
    const stream = try server.accept(io); // We just accept here, blocking till can.

    // recv(2): https://man7.org/linux/man-pages/man2/recv.2.html
    // user data, fd, buffer and flag.
    // return the number of byte received. 0 if EOF

    // Multishot version need a BufferGroup
    var bg = try iou.BufferGroup.init(&ring, gpa, 10, 1024, 1 << 3);
    _ = try bg.recv_multishot(15, stream.socket.handle, 0);
    const count = try ring.submit();
    std.debug.print("Tasks submitted: {}\n", .{count});

    // Event loop (wait until completion queue has something)
    while (true) {
        // Will wait until we have an entry.
        const comp_entry = try ring.copy_cqe();
        const err = comp_entry.err();
        if (err == .SUCCESS) {
            if (comp_entry.user_data == 15) {
                // The correct user data. Can be anything
                // You can use @intFromPtr and @ptrFromInt to pass in the pointer and check type.
                const num_read: usize = @intCast(comp_entry.res); // The fd for the accepted socket (read / write using it)
                if (num_read == 0) {
                    stream.close(io);
                    break;
                }
                const data_full = try bg.get(comp_entry); // Get result for this cqe
                // First is #bytes
                const data = data_full[1..];
                // const data = recv_buf[1..num_read];
                std.debug.print("Read the return #bytes = {}\n", .{num_read});
                std.debug.print("Data = {any}\n", .{data});
                var wr = stream.writer(io, &write_buf);
                if (message_util.parseMessage(data)) |m| {
                    switch (m) {
                        message_util.MessageType.ECHO => |e| {
                            std.debug.print("Received echo = {s}\n", .{e});
                            try message_util.writeMessageToStream(&wr, message_util.Message{
                                .R_ECHO = e,
                            });
                        },
                        // Per testing, we will not read back the message we just sent, so all is good.
                        message_util.MessageType.R_ECHO => |e| {
                            std.debug.print("Received my own r_echo = {s}\n", .{e});
                        },
                        else => {
                            // Ignore
                        },
                    }
                } else {
                    std.debug.print("No message can be parsed\n", .{});
                }
                try bg.put(comp_entry); // Put back the buffer to the kernel.
            }
        } else {
            std.debug.print("Err = {any}\n", .{err});
        }
    }
}
```

The best part is that the server won't read the data that it just write (due to I/O Multiplexing of TCP), so we can just `recv` again without worry.

#### Concat messages
`recv` might break messages into multiple parts, that where our `messages_size` come in to play: We will get and store messages, only process when the messages are fully sent.
- Messages for each stream will be stored in an array of `[]u8` to allow concat.
- Incoming messages are concat with the old messages of the same stream.
- When messages are full, process.

### Redesign our MQ with `io_uring` with `[]_multishot`.
We will consider and reference the techniques taken from https://arxiv.org/pdf/2512.04859

#### Design consideration
##### What to do in the event loop
There are 2 ways to handle events:
- Inline processing: Process event inside the loop.
- Dedicated worker pool: Each event are sent to the correct worker (or job queue) to avoid blocking the event loop.

We will use "Inline processing" method to reduce our implementation effort.

##### How many event loops
- One for accepting + read from any client (for admin receive)
- One to read / write to all producers
- One to read / write to all consumers
- To pop each topic, we will keep using each thread to handle it.

=> Total: 4 thread (main, event loop admin, event loop producer, event loop consumer) + #topic

##### `io_uring` setup and command usages
- We have to make use of all the `[]_multishot` functions, namely `accept_multishot` and `recv_multishot` for maximum benefit.
-  `SQPoll` mode allow us to not having to call `submit` manually (wasting syscall). But since we don't have that many syscall, we won't be using it for now.
- We will use [copy_cqes](https://ziglang.org/documentation/master/std/#std.os.linux.IoUring.copy_cqes) + `wait_nr` to allow getting multiple CQEs at once (reduce syscall).

##### Other consideration
- If message size > 1kb, we can use [send_zc](https://ziglang.org/documentation/master/std/#std.os.linux.IoUring.send_zc) instead of writing to stream for a even more efficiency.
- There is no ZC version of the `recv` available (Linux 6.15 needed...), so we're stuck with `recv` for now.

#### Admin message receive
The way to start the server is to use `accept_multishot` and inline processing:
```zig
    /// Main function to start an admin server and wait for a message
    pub fn startAdminServer(self: *Self, io: Io, gpa: Allocator) !void {
        // Create a server on the address and wait for a connection
        var server = try self.admin_address.listen(io, .{ .reuse_address = true }); // TCP server

        // io_uring accept
        _ = try self.aring.accept_multishot(0, server.socket.handle, null, null, 0);
        _ = try self.aring.submit();

        // Event loop to process admin accept
        // Use inline completion since these messages are supposed to be quick.
        // Also don't have to batch, these are very short and fast.
        std.debug.print("Starting admin server...\n", .{});
        while (true) {
            const comp_entry = try self.aring.copy_cqe();

            const err = comp_entry.err();
            if (err == .SUCCESS) {
                const fd = comp_entry.res; // The fd for the accepted socket (read / write using it)
                // New stream, with the accepted socket.
                var stream = net.Stream{ .socket = net.Socket{ .handle = fd, .address = self.admin_address } };
                // Init the read/write stream.
                var stream_rd = stream.reader(io, &self.read_buffer);
                var stream_wr = stream.writer(io, &self.write_buffer);
                // Read and process message
                if (try message_util.readMessageFromStream(&stream_rd)) |message| {
                    if (try self.processAdminMessage(io, gpa, message)) |response_message| {
                        try message_util.writeMessageToStream(&stream_wr, response_message);
                    }
                }
                stream.close(io); // Close the stream after
            } else {
                std.debug.print("Err = {any}\n", .{err});
            }
        }
    }
```
- Each client call `connect` to the admin port, and the server always queue an `accept`.
- Upon read and ack, close the stream.
- We don't concat cuz rarely do the messages got broken on first connect.

#### Producer message receive
When we receive a producer register, we can queue an `accept_multishot` and submit immediately:
```zig
    fn processProducerRegisterMessage(self: *Self, io: Io, gpa: Allocator, rm: *const message_util.ProducerRegisterMessage) !u8 {
...
        if (topic_o) |tp| {
            const pd: *ProducerData = try gpa.create(ProducerData);
            pd.* = ProducerData.new(tp, rm.port, stream);
            std.debug.print("Registered a producer on port {}, topic {}\n", .{ rm.port, rm.topic });
            // Upon register, put it to the queue.
            const pd_data = @intFromPtr(pd);
            _ = try self.pbg.recv_multishot(pd_data, stream.socket.handle, 0);
            _ = try self.pring.submit();
        }
        return 0;
    }
```

We will have another event loop for producer message read processing (inline style):
```zig
    /// Handle all producer messages in a loop of recv_multishot.
    /// These messages are supposed to be PCM.
    pub fn handleProducersLoop(self: *Self, io: Io, gpa: Allocator) !void {
        var write_buf: [1024]u8 = undefined;
        var cqes: [4]std.os.linux.io_uring_cqe = undefined;
        // In case messages are split, use this to collect + send.
        var port_data = try gpa.alloc(?[]u8, 65536);
        @memset(port_data, null);
        // Event loop (wait until completion queue has something)
        while (true) {
            // Multi recv, this can comes from multiple stream.
            const num_recv = try self.pring.copy_cqes(&cqes, 1);
            // Recreate the full data buffer
            for (cqes[0..num_recv]) |cqe| {
                const err = cqe.err();
                if (err != .SUCCESS) {
                    std.debug.print("Err = {any}\n", .{err});
                    continue;
                }
                // User data covert back to a ProducerData pointer, to know which producer it received from.
                var pd: *ProducerData = @ptrFromInt(@as(usize, @intCast(cqe.user_data)));
                const port: usize = @intCast(pd.port);
                const num_read: usize = @intCast(cqe.res);
                if (num_read == 0) {
                    // Clean up when stream is broken.
                    // TODO: Cancel the recv_multishot.
                    pd.stream.close(io);
                    gpa.destroy(pd);
                    if (port_data[port]) |sl| {
                        gpa.free(sl);
                        port_data[port] = null;
                    }
                    continue;
                }
                const data_full = try self.pbg.get(cqe); // Get result for this cqe
                // User the port as a way to of storage: each producer port will have a growable buffer that we can just copy data in.
                // Copy outside, put it back to kernel after...
                if (port_data[port]) |current_data| {
                    port_data[port] = try std.mem.concat(gpa, u8, &.{ current_data, data_full });
                } else {
                    port_data[port] = try std.mem.concat(gpa, u8, &.{ "", data_full });
                }
                try self.pbg.put(cqe); // Give it back cuz not needed anymore.
                if (port_data[port]) |cur_data| {
                    // Check if the message is good to be processed.
                    const need_len: usize = @as(usize, @intCast(cur_data[0])) + 1;
                    if (cur_data.len != need_len) {
                        continue;
                    }
                    // Good to be parsed. Ignore the length first byte.
                    if (message_util.parseMessage(cur_data[1..])) |m| {
                        switch (m) {
                            message_util.MessageType.PCM => |pcm| {
                                try pd.topic.addMessage(io, gpa, &pcm); // Copy and Block until can add.
                                var wr = pd.stream.writer(io, &write_buf);
                                try message_util.writeMessageToStream(&wr, message_util.Message{
                                    .R_PCM = 0,
                                });
                            },
                            else => {
                                // Not supported
                                std.debug.print("Not supported message of other type that PCM\n", .{});
                            },
                        }
                    }
                    // Cleanup and make undefined;
                    gpa.free(cur_data);
                    port_data[port] = null;
                }
            }
        }
    }
```
- `port_data` is used to concat all messages on a port stream (`recv` can break a message).
- After copy / concat to the correct `port_data`, we can put the buffer back immediately.
- `copy_cqes` is used to receive many ready event at once (minimum 1 via `wait_nr`).
- Parse the message, add to the topic, write ack back to the stream to complete the event loop.

#### Consumer ready message receive
Very similar to the producer, when a consumer registered, we will queue a `recv_multishot` and submit it:
```zig
    /// Return the position of the consumer in the list
    fn processConsumerRegisterMessage(self: *Self, io: Io, gpa: Allocator, rm: *const message_util.ConsumerRegisterMessage) !void {
        ...
        // Add the port, stream and stream_state
        const cd = try gpa.create(ConsumerData);
        cd.* = ConsumerData.new(rm.port, cg, tp, stream);
        // Each consumer will send a ready to start receiving messages. This is just 1 byte.
        const cd_data = @intFromPtr(cd);
        _ = try self.cbg.recv_multishot(cd_data, cd.stream.socket.handle, 0);
        _ = try self.cring.submit();
    }
```

We have another event loop to handle the `consumer ready` and `ack` from the consumers:
```zig
    /// Handle all consumer messages in a loop of recv_multishot.
    /// These are expected to be C_RD and R_C_PCM (1 byte only)
    pub fn handleConsumersLoop(self: *KAdmin, io: Io, gpa: Allocator) !void {
        var write_buf: [1024]u8 = undefined;
        var cqes: [1 << 3]std.os.linux.io_uring_cqe = undefined;
        // Event loop
        while (true) {
            // Multi recv, this can comes from multiple stream.
            const num_recv = try self.cring.copy_cqes(&cqes, 1);
            // Recreate the full data buffer, these are just 1 bytes so no concat need.
            for (cqes[0..num_recv]) |cqe| {
                const err = cqe.err();
                if (err != .SUCCESS) {
                    std.debug.print("Err = {any}\n", .{err});
                    continue;
                }
                var cd: *ConsumerData = @ptrFromInt(@as(usize, @intCast(cqe.user_data)));
                // Upon receive, it's a ready for sure.
                const num_read: usize = @intCast(cqe.res);
                if (num_read == 0) {
                    cd.stream.close(io);
                    gpa.destroy(cd);
                    continue;
                }
                // Get and put back seems unneeded, but this is for clearing the buffer.
                const data = try self.cbg.get(cqe); // Get result for this cqe
                // Good to be parsed. Ignore the length first byte.
                if (message_util.parseMessage(data[0..])) |m| {
                    switch (m) {
                        message_util.MessageType.C_RD => |_| {
                            // std.debug.print("C_RD got from {}\n", .{cd.port});
                            var stream_wr = cd.stream.writer(io, &write_buf);
                            // consumer is ready, first write the ack (blocking)
                            try message_util.writeMessageToStream(&stream_wr, message_util.Message{
                                .R_C_RD = 0,
                            });
                            // Then write one of the PCM in the topic
                            cd.topic.mq_lock.lockShared();
                            // std.debug.print("Got the lock for {}\n", .{cd.port});
                            if (cd.topic.mq.peek(cd.group.offset)) |pcm| {
                                // Write message to the stream
                                try message_util.writeMessageToStream(&stream_wr, message_util.Message{ .PCM = pcm.* });
                                // std.debug.print("Written to stream for port {}\n", .{cd.port});
                                cd.group.offset += 1;
                            }
                            cd.topic.mq_lock.unlockShared();
                        },
                        message_util.MessageType.R_C_PCM => |_| {
                            // std.debug.print("R_PCM got from {}\n", .{cd.port});
                        },
                        else => {
                            // Not supported
                            std.debug.print("Not supported message of other type that PCM\n", .{});
                        },
                    }
                }

                try self.cbg.put(cqe); // Give it back cuz not needed anymore.
            }
        }
    }
```
- We redesign the `C_RD` and `R_C_PCM` to use only 1 byte (only the type) to avoid having to concat the message.
- Other than that, it works pretty much the same as before.

#### Setting up event loop thread
We setup our ring in the main thread and pass it to the `kadmin` internal structure. These ring can be passed around and used on any thread (pass by pointer only).
```zig
pub fn initKAdmin() !void {
    // TODO: Process terminal signal to clean up.
    const gpa = std.heap.smp_allocator;

    // Set up our I/O implementation.
    var threaded: std.Io.Threaded = .init(gpa, .{ .environ = .empty });
    defer threaded.deinit();
    const io = threaded.io();
    // Init all rings and bg
    var aring = try iou.init(8, 0);
    var pring = try iou.init(8, 0);
    var cring = try iou.init(8, 0);
    var pbg = try iou.BufferGroup.init(&pring, gpa, 10, 1024, 8);
    var cbg = try iou.BufferGroup.init(&cring, gpa, 11, 1024, 8);
    // Start needed threads for event loops
    var admin = try kadmin.KAdmin.init(gpa, &aring, &pring, &cring, &pbg, &cbg);
    // try admin.startAdminServer(io, gpa);
    var th = try std.Thread.spawn(.{}, kadmin.KAdmin.startAdminServer, .{ &admin, io, gpa });
    var th2 = try std.Thread.spawn(.{}, kadmin.KAdmin.handleProducersLoop, .{ &admin, io, gpa });
    var th3 = try std.Thread.spawn(.{}, kadmin.KAdmin.handleConsumersLoop, .{ &admin, io, gpa });
    th.join();
    th2.join();
    th3.join();
}
```

#### Non-blocking stream writing
Another bottleneck is when writing to the stream: Due to network bandwidth and speed, it can be slow and blocking for a while.

To write, we will be using the [send(2)](https://man7.org/linux/man-pages/man2/send.2.html) command:
- Input is a file descriptor to the stream
- Data written from a buffer (`[]u8`)
- Return the number of byte written.

We can queue the command on a new `send` ring:
- To write without corruption, the input buffer needs to live until the result is in.
- We can create the buffer using an allocator and put it in the `user_data`
- On done, we just destroy the buffer to save space.

```zig
    /// Write a message using `write` to fd.
    /// Alloc and put the ptr to the user_data
    fn writeMessageToFD(self: *KAdmin, gpa: Allocator, fd: net.Socket.Handle, message: message_util.Message) !void {
        const buf = try gpa.alloc(u8, 1024);
        const wd = try gpa.create(WriteData);
        wd.full_slice = buf;
        switch (message) {
            message_util.MessageType.PCM => |pcm| {
                const data = try pcm.convertToBytesWithLengthAndType(buf);
                wd.fd = fd;
                wd.cur_written = 0;
                wd.need_written = data.len;
                const user_data = @intFromPtr(wd);
                _ = try self.wring.send(user_data, fd, data, 0);
                _ = try self.wring.submit();
            },
            message_util.MessageType.R_PCM => |ack| {
                buf[0] = ack;
                wd.fd = fd;
                wd.cur_written = 0;
                wd.need_written = 1;
                const user_data = @intFromPtr(wd);
                _ = try self.wring.send(user_data, fd, buf[0..1], 0);
                _ = try self.wring.submit();
            },
            else => {
                // Not supported.
            },
        }
    }
```

We need to create another event loop to handle all write, and also handle broken write by keep writing
```zig
    /// Event loop to handle all write. These just return a buffer to us for now.
    /// If you need to handle different type of write, you can create a struct for it.
    pub fn handleWriteLoop(self: *KAdmin, gpa: Allocator) !void {
        var cqes: [1 << 3]std.os.linux.io_uring_cqe = undefined;
        // Event loop
        while (true) {
            const num_write = try self.wring.copy_cqes(&cqes, 1);
            // Recreate the full data buffer, these are just 1 bytes so no concat need.
            for (cqes[0..num_write]) |cqe| {
                const err = cqe.err();
                if (err != .SUCCESS) {
                    std.debug.print("Err = {any}\n", .{err});
                    continue;
                }
                // Free data
                const wd: *WriteData = @ptrFromInt(@as(usize, @intCast(cqe.user_data)));
                wd.cur_written += @as(usize, @intCast(cqe.res));
                if (wd.cur_written < wd.need_written) {
                    std.debug.print("Written data to fd = {any}, total = {}, need = {}\n", .{ wd.fd, wd.cur_written, wd.need_written });
                    // Need to keep writing.
                    _ = try self.wring.send(cqe.user_data, wd.fd, wd.full_slice[wd.cur_written..], 0);
                    _ = try self.wring.submit();
                } else {
                    gpa.free(wd.full_slice);
                    gpa.destroy(wd);
                }
            }PCM
        }
    }
```

We can change both the producer write `R_PCM` and the consumer write `PCM` to this flow, but not the admin messages processing: We still need the write to be done before closing the stream for a new accept.
- Technically can be done, but there's no bottleneck here so who cares.

#### Exponent back-off retry for writing messages to consumer
In case the consumer is much faster than the producer, we don't want to wait for a message in the consumer loop. We want to only send after we got some new data.

To do this, we spawn a thread while loop to check for messages, join it when we have data. We only do this when we don't have a new message ready.

However, spawning a thread is costly, so we will use a small trick:
- Submit a [timeout](https://ziglang.org/documentation/master/std/#std.os.linux.IoUring.timeout) event with the `ConsumerData` for 10ms to a retry ring
- After timeout, check again to see if there's a message. It not, submit another `timeout` for longer (20ms). Timeout keeps doubling until we have a new message, then it reset to 10ms.

```zig
    /// Retry for the consumer write PCM
    pub fn handleRetryLoop(self: *KAdmin, gpa: Allocator) !void {
        var cqes: [1 << 3]std.os.linux.io_uring_cqe = undefined;
        // Event loop
        while (true) {
            const num_recv = try self.rring.copy_cqes(&cqes, 1);
            for (cqes[0..num_recv]) |cqe| {
                const err = cqe.err();
                // A timeout will return .TIME
                if (err != .TIME) {
                    std.debug.print("Err in retry = {any}\n", .{err});
                    continue;
                }
                var cd: *ConsumerData = @ptrFromInt(@as(usize, @intCast(cqe.user_data)));
                if (cd.topic.mq.peek(cd.group.offset)) |pcm| {
                    cd.topic.mq_lock.lockShared();
                    // Write message to the stream, no wait and just +1
                    try self.writeMessageToFD(gpa, cd.stream.socket.handle, message_util.Message{ .PCM = pcm.* });
                    cd.group.offset += 1;
                    cd.topic.mq_lock.unlockShared();
                    cd.current_timeout.nsec = 10000000;
                } else {
                    // Double the timeout and send to the retry queue
                    cd.current_timeout.nsec *= 2;
                    _ = try self.rring.timeout(cqe.user_data, cd.current_timeout, 0, 0);
                    _ = try self.rring.submit();
                }
            }
        }
    }

    /// Handle all consumer messages in a loop of recv_multishot.
    /// These are expected to be C_RD and R_C_PCM (1 byte only)
    pub fn handleConsumersLoop(self: *KAdmin, io: Io, gpa: Allocator) !void {
        // var write_buf: [1024]u8 = undefined;
        var cqes: [1 << 3]std.os.linux.io_uring_cqe = undefined;
        // Event loop
        while (true) {
            // Multi recv, this can comes from multiple stream.
            const num_recv = try self.cring.copy_cqes(&cqes, 1);
            // Recreate the full data buffer, these are just 1 bytes so no concat need.
            for (cqes[0..num_recv]) |cqe| {
                const err = cqe.err();
                if (err != .SUCCESS) {
                    std.debug.print("Err = {any}\n", .{err});
                    continue;
                }
                var cd: *ConsumerData = @ptrFromInt(@as(usize, @intCast(cqe.user_data)));
                // Upon receive, it's a ready for sure.
                const num_read: usize = @intCast(cqe.res);
                if (num_read == 0) {
                    cd.stream.close(io);
                    gpa.destroy(cd);
                    continue;
                }
                // Get and put back seems unneeded, but this is for clearing the buffer.
                const data = try self.cbg.get(cqe); // Get result for this cqe
                // Good to be parsed. Ignore the length first byte.
                if (message_util.parseMessage(data[0..])) |m| {
                    switch (m) {
                        message_util.MessageType.R_C_PCM => |_| {
                            // Write the PCM to the consumer
                            // std.debug.print("Got the lock for {}\n", .{cd.port});
                            if (cd.topic.mq.peek(cd.group.offset)) |pcm| {
                                cd.topic.mq_lock.lockShared();
                                // Write message to the stream, no wait and just +1
                                try self.writeMessageToFD(gpa, cd.stream.socket.handle, message_util.Message{ .PCM = pcm.* });
                                cd.group.offset += 1;
                                cd.topic.mq_lock.unlockShared();
                            } else {
                                // Send to the retry queue
                                // Create a timespec of 10ms
                                const ts = try gpa.create(std.os.linux.kernel_timespec);
                                ts.sec = 0;
                                ts.nsec = 10000000;
                                cd.current_timeout = ts;
                                _ = try self.rring.timeout(cqe.user_data, ts, 0, 0);
                                _ = try self.rring.submit();
                            }
                        },
                        else => {
                            // Not supported
                            std.debug.print("Not supported message of other type that PCM\n", .{});
                        },
                    }
                }

                try self.cbg.put(cqe); // Give it back cuz not needed anymore.
            }
        }
    }
```

#### Stream shutdown


#### Non-blocking disk writing
To persist data, you need to write to disk. This is also a potential blocking point and need to deal with
TODO: Right now I don't event have a persistent flow yet.

## Language specific
### Memory leak and reduce allocation
#### Valgrind introduction
https://valgrind.org/downloads/current.html#current

We need to switch to the `std.mem.c_allocator` to use Valgrind.

First, build zig with our target OS + lc:
```
zig build-exe src/main.zig -target aarch64-linux-gnu -lc
```

Then call valgrind with the param:
```
valgrind --leak-check=full --show-leak-kinds=all --track-origins=yes ./main mem
```

#### Reduce allocation
TODO: Probably introduce the fix buffer allocation at some point lol?

#### Performance optimization
https://www.brendangregg.com/perf.html#OneLiners

https://docs.redhat.com/en/documentation/red_hat_enterprise_linux/9/html/monitoring_and_managing_system_status_and_performance/monitoring-application-performance-with-perf_monitoring-and-managing-system-status-and-performance

```
perf record -F 1000 --call-graph fp ./main bench
perf report
```

### Index pattern
https://matklad.github.io/2025/12/23/zig-newtype-index-pattern.html


# 12. Next step
- Better message format: Currently only support maximum 255 character in a message in the stream.
- Replicate `kadmin` state to multiple machine, allow one of them to be the `kadmin` upon the old process died (essentially a coordinator program)
- Configuration of cluster + setting via TOML
- Benchmark + improve performance
- Testing
- Compare Kafka with RabbitMQ / RedisPubsub / https://nats.io/ / azure service bus
	- Find good optimization / balance
	- Pick concepts
	- First slide for download / setup projects.
