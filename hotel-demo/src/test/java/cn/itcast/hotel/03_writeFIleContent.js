const fs = require("fs")

fs.writeFile('./file/2.txt', 'Hello, Node.js! one', function(err) {
    if (err) {
        console.log("write file fail, " + err);
        return;
    }
    console.log("write file succeed!");
})
