const { CacheFhirToES } = require('./reports')
let caching = new CacheFhirToES({
  FHIRBaseURL: 'http://localhost:8081/fhir',
  FHIRUsername: '',
  FHIRPassword: '',
  since: '', //use this to specify last updated time of resources to be processed
  relationshipsIDs: ['ihris-es-report-grievance'], //if not specified then all relationships will be processed
  reset: true, //will pull all resources if set to true
  ESModulesBasePath: "/home/ally/iHRIS/ihris-backend/namibia/modules/es",
  DBConnection: {
    database: "chadihris",
    username: "hapi",
    password: "hapi",
    dialect: "postgres" /* one of 'mysql' | 'postgres' | 'sqlite' | 'mariadb' | 'mssql' | 'db2' | 'snowflake' | 'oracle' */
  }
})
caching.cache().then(() => {
  console.log('Done')
})
