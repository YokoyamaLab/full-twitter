const socket = io("ws://tokyo004:114580");

socket.on("connect", () => {
  // either with send()
  socket.send("Hello!");
});