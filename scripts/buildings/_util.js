const fse = require('fs-extra');
require("jsonminify");

module.exports = {  
  readConfigDetails: (configPath) => {
    // https://stackoverflow.com/a/35008327/18450475
    let checkFileExistsSync = function(configPath) {
      let flag = true;
      try{
        fse.accessSync(configPath, fse.constants.F_OK);
      }catch(e){
        flag = false;
      }
      return flag;
    }
    let exists = checkFileExistsSync(configPath);
    if (exists) {
      let fileContent = fse.readFileSync(configPath, "utf8");
      return JSON.parse(JSON.minify(fileContent));
    }
    throw new Error("Config file not found.");
  }
}