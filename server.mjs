import { Server, Socket } from "socket.io";
const io = new Server(14580);

io.on('connection', client => {
    console.log(client);
    client.on('message', data => {
        console.log(data);
    });
    client.on('disconnect', () => {
        console.log("disconnect");
    });
});