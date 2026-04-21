'use strict';
declareUpdate();
function randomNumber(max) {
  const min = 0;
    return Math.random() * (max - min) + min;
}
function randomString(length) {
    let result = '';
    const characters = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789';
    const charactersLength = characters.length;
    let counter = 0;
    while (counter < length) {
      result += characters.charAt(Math.floor(Math.random() * charactersLength));
      counter += 1;
    }
    return result;
}
var URI;

for (const uri of URI.split(";")) {
  const ID = sem.uuidString()
  
  /*  
  const doc = { "checkId": ID, 
    "timestamp": new Date(), 
    "lastRecordedStats": 
        { "temp": randomNumber(100), 
          "humidity": randomNumber(100) 
        }, 
    "code": Math.round(Math.random()*10), 
    "message": null, 
  }
  */
  const len = 200
  const doc = { 'Category': randomString(len), 
    'ProcessType': randomString(len), 
    'StartFrequency': randomString(len), 
    'EndFequency': randomString(len), 
    'STD': randomString(len), 
    'RES':randomString(len), 
    'Comments': randomString(len)  
    }
  xdmp.documentInsert("/sensor/"+ID+".json", doc)
}