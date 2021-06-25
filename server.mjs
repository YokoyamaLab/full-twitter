const io = require('socket.io')();
io.on('connection', client => {
    client.on('event', data => {
        console.log(data);
    });
    client.on('disconnect', () => {
        console.log("disconnect");
    });
});
io.listen(114580);