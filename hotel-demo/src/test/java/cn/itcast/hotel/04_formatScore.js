
const fs = require("fs");

fs.readFile(__dirname + "/materail/score_raw.txt", "utf8", function(err, dataStr) {
    if (err) {
        console.log("read file fail" + err.message);
        return;
    }
    console.log(dataStr);

    const arrOld = dataStr.split(" ");
    const arrNew = [];

    arrOld.forEach(item => {
        arrNew.push(item.replace("=", ":"))
    })

    const newStr = arrNew.join("\n");

    fs.writeFile(__dirname + "/file/score_format.txt", newStr, function (err) {
        if (err) {
            console.log("write file fail" + err.message);
            return;
        }
    })

    console.log("write file succeed!");

})
