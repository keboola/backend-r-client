library(testthat)

if (!("keboola.sapi.r.client" %in% installed.packages())) {
    library(devtools)
    devtools::install_github("keboola/sapi-r-client")
}
library(keboola.sapi.r.client)

# override with config if any
if (file.exists("config.R")) {
    source("config.R")
}
if (file.exists("config.redshift.R")) {
    source("config.redshift.R")
}
if (file.exists("config.snowflake.R")) {
    source("config.snowflake.R")
}

# override with environment if any
if (nchar(Sys.getenv("KBC_TOKEN")) > 0) {
    KBC_TOKEN <- Sys.getenv("KBC_TOKEN")
}
if (nchar(Sys.getenv("DATA_DIR")) > 0) {
    DATA_DIR <- Sys.getenv("DATA_DIR")  
}

cl <- SapiClient$new(KBC_TOKEN)
tryCatch({
    snowflakeWorkspace <- cl$createWorkspace(backend="snowflake")
    SNFLK_HOST <- snowflakeWorkspace$connection$host
    SNFLK_DB <- snowflakeWorkspace$connection$database
    SNFLK_USER <- snowflakeWorkspace$connection$user
    SNFLK_PASSWORD <- snowflakeWorkspace$connection$password
    SNFLK_SCHEMA <- snowflakeWorkspace$connection$schema
    redshiftWorkspace <- cl$createWorkspace(backend="redshift")
    RS_HOST <- redshiftWorkspace$connection$host
    RS_DB <- redshiftWorkspace$connection$database
    RS_SCHEMA <- redshiftWorkspace$connection$schema
    RS_USER <- redshiftWorkspace$connection$user
    RS_PASSWORD <- redshiftWorkspace$connection$password
    test_check("keboola.backend.r.client")
}, finally = {
    cl$dropWorkspace(snowflakeWorkspace$id)
    cl$dropWorkspace(redshiftWorkspace$id)
})

