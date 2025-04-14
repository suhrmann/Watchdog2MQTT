// Source: https://developer.ibm.com/recipes/tutorials/sending-and-receiving-pictures-from-a-raspberry-pi-via-mqtt/
// Archived: https://web.archive.org/web/20201202115516/https://developer.ibm.com/recipes/tutorials/sending-and-receiving-pictures-from-a-raspberry-pi-via-mqtt/
//
// In this example, the image will be displayed on a web page in the browser.
// A Javascript MQTT client subscribes and receives the image data. When the data comes in,
// the image is reconstructed by putting together the chunks in the correct order using the following function.
// The image labeled “picture_to_show” will show the reconstructed image.

function reconstructBase64String(chunk) {
    pChunk = JSON.parse(chunk["d"]);

    //creates a new picture object if receiving a new picture, else adds incoming strings to an existing picture
    if (pictures[pChunk["pic_id"]]==null) {
        pictures[pChunk["pic_id"]] = {"count":0, "total":pChunk["size"], pieces: {}, "pic_id": pChunk["pic_id"]};

        pictures[pChunk["pic_id"]].pieces[pChunk["pos"]] = pChunk["data"];

    }

    else {
        pictures[pChunk["pic_id"]].pieces[pChunk["pos"]] = pChunk["data"];
        pictures[pChunk["pic_id"]].count += 1;


        if (pictures[pChunk["pic_id"]].count == pictures[pChunk["pic_id"]].total) {
        console.log("Image reception compelete");
        var str_image="";

        for (var i = 0; i <= pictures[pChunk["pic_id"]].total; i++)
            str_image = str_image + pictures[pChunk["pic_id"]].pieces[i];

        //displays image
        var source = 'data:image/jpeg;base64,'+str_image;
        var myImageElement = document.getElementById("picture_to_show");
        myImageElement.href = source;
        }

    }
}
