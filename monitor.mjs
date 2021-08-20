import { io } from 'socket.io-client'
const socket = io("localhost:14580");

socket.on("connect", () => {
    // either with send()
    console.log("CONNECT");
    socket.send("Hello!");
});

socket.on("error", (error) => {
    console.log(error);
});
console.log("Hi");