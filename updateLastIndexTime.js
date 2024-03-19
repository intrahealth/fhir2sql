const axios = require("axios")
const moment = require("moment")
const URI = require('urijs');

function updateLastIndexingTime(start, ended, index) {
  return new Promise((resolve, reject) => {
    console.info('Updating lastIndexingTime')
    axios({
      url: URI("http://localhost:9200").segment('syncdata').segment("_doc").segment(index).toString(),
      method: 'PUT',
      auth: {
        username: "",
        password: ""
      },
      data: {
        "lastBeganIndexingTime": start,
        "lastEndedIndexingTime": ended
      }
    }).then((response) => {
      if(response.status < 200 && response.status > 299) {
        console.error('An error occured while updating lastIndexingTime')
        return reject()
      }
      return resolve(false)
    }).catch((err) => {
      console.error(err)
      console.error('An error occured while updating lastIndexingTime')
      return reject(true)
    })
  })
}

let newLastBeganIndexingTime = moment().format('Y-MM-DDTHH:mm:ss');
setTimeout(() => {
  let newLastEndedIndexingTime = moment().format('Y-MM-DDTHH:mm:ss');
  updateLastIndexingTime(newLastBeganIndexingTime, newLastEndedIndexingTime, 'bhpclicensedprofessionals').then(() => {
    console.log("Done");
  })
}, 2000);