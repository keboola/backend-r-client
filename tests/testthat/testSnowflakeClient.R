# Test snowflake connection

library('keboola.backend.r.client')

test_that("connectSnowflake", {
    driver <- BackendDriver$new()     
    expect_equal(
        driver$connectSnowflake(SNFLK_HOST, SNFLK_DB, SNFLK_USER, SNFLK_PASSWORD, SNFLK_SCHEMA), 
        TRUE
    )
#    expect_that(
#        driver$connectSnowflake("invalid", SNFLK_DB, SNFLK_USER, SNFLK_PASSWORD, SNFLK_SCHEMA),
#        throws_error()
#    )
})

test_that("prepare", {
    driver <- BackendDriver$new()     
    expect_equal(
        driver$prepareStatement("SELECT * FROM foo WHERE bar = ?", "baz"), 
        "SELECT * FROM foo WHERE bar = 'baz'"
    )
    expect_equal(
        driver$prepareStatement("SELECT * FROM foo WHERE bar = ?", "ba'z"), 
        "SELECT * FROM foo WHERE bar = 'ba''z'"
    )
    expect_equal(
        driver$prepareStatement("SELECT * FROM foo WHERE bar = ?", 42), 
        "SELECT * FROM foo WHERE bar = '42'"
    )
    expect_equal(
        driver$prepareStatement("SELECT * FROM foo WHERE bar = ? AND baz = ?", list(bar=42,baz=21)), 
        "SELECT * FROM foo WHERE bar = '42' AND baz = '21'"
    )
    expect_equal(
        driver$prepareStatement("SELECT * FROM foo WHERE bar = ? AND baz = ?", 42, 21), 
        "SELECT * FROM foo WHERE bar = '42' AND baz = '21'"
    )
})

test_that("update", {
    driver <- BackendDriver$new()     
    expect_that(
        driver$update("CREATE TABLE foo (bar INTEGER);"), 
        throws_error()
    )
    driver$connectSnowflake(SNFLK_HOST, SNFLK_DB, SNFLK_USER, SNFLK_PASSWORD, SNFLK_SCHEMA)
    driver$update("DROP TABLE IF EXISTS foo CASCADE;")
    
    expect_equal(
        driver$update("CREATE TABLE foo (bar INTEGER);"), 
        TRUE
    )
    expect_that(
        driver$update("CREATE TABLE foobar error;"), 
        throws_error()
    )
})

test_that("update", {
    driver <- BackendDriver$new()     
    expect_that(
        driver$update("CREATE TABLE foo (bar INTEGER);"), 
        throws_error()
    )
    driver$connectSnowflake(SNFLK_HOST, SNFLK_DB, SNFLK_USER, SNFLK_PASSWORD, SNFLK_SCHEMA)
    driver$update("DROP TABLE IF EXISTS foo CASCADE;")
    
    expect_equal(
        driver$update("CREATE TABLE foo (bar INTEGER);"), 
        TRUE
    )
    expect_that(
        driver$update("CREATE TABLE foobar error;"), 
        throws_error()
    )
})

test_that("tableExists", {
    driver <- BackendDriver$new()     
    driver$connectSnowflake(SNFLK_HOST, SNFLK_DB, SNFLK_USER, SNFLK_PASSWORD, SNFLK_SCHEMA)
    driver$update("DROP TABLE IF EXISTS foo CASCADE;")
    driver$update("CREATE TABLE foo (bar INTEGER);")
    
    expect_equal(
        driver$tableExists("foo"), 
        TRUE
    )
    expect_equal(
        driver$tableExists("non-existent-table"), 
        FALSE
    )
})

test_that("columnTypes", {
    driver <- BackendDriver$new()     
    driver$connectSnowflake(SNFLK_HOST, SNFLK_DB, SNFLK_USER, SNFLK_PASSWORD, SNFLK_SCHEMA)
    driver$update(paste0("DROP TABLE IF EXISTS foo CASCADE;"))
    driver$update(paste0("CREATE TABLE foo (bar INTEGER, baz VARCHAR (200));"))
    colTypes <- vector()
    colTypes[["BAR"]] <- "NUMBER(38,0)"
    colTypes[["BAZ"]] <- "VARCHAR(200)"
    
    expect_equal(
        sort(driver$columnTypes('foo')),
        sort(colTypes)
    )
    expect_that(
        driver$columnTypes("non-existent-table"), 
        throws_error()
    )
})

test_that("saveDataFrame1", {
    driver <- BackendDriver$new()     
    driver$connectSnowflake(SNFLK_HOST, SNFLK_DB, SNFLK_USER, SNFLK_PASSWORD, SNFLK_SCHEMA)
    driver$update("DROP TABLE IF EXISTS fooBar CASCADE;")
    df <- data.frame("foo" = c(1,3,5), "bar" = c("one", "three", "five"))
    driver$saveDataFrame(df, "fooBar", rowNumbers = FALSE, incremental = FALSE)
    dfResult <- driver$select("SELECT \"foo\", \"bar\" FROM fooBar ORDER BY \"foo\"")
    df[, "bar"] <- as.character(df[, "bar"])
    
    expect_equal(
        dfResult,
        df
    )

    driver$update("DROP TABLE IF EXISTS fooBar CASCADE;")
    df <- data.frame("foo" = c(1,3,5), "bar" = c("one", "three", "five"))
    driver$saveDataFrame(df, "fooBar", rowNumbers = TRUE, incremental = FALSE)
    dfResult <- driver$select("SELECT \"foo\", \"bar\", \"ROW_NUM\" FROM fooBar ORDER BY \"foo\"")
    df[, "bar"] <- as.character(df[, "bar"])
    df[['ROW_NUM']] <- c(1, 2, 3)

    expect_equal(
        dfResult,
        df
    )
    
    driver$update("DROP TABLE IF EXISTS fooBar CASCADE;")
    driver$update(paste0("CREATE TABLE fooBar (bar INTEGER);"))
    # verify that the old table will get deleted
    df <- data.frame("foo" = c(1,3,5), "bar" = c("one", "three", "five"))
    driver$saveDataFrame(df, "fooBar", rowNumbers = FALSE, incremental = FALSE)
    dfResult <- driver$select("SELECT \"foo\", \"bar\" FROM fooBar ORDER BY \"foo\"")
    df[, "bar"] <- as.character(df[, "bar"])
    
    expect_equal(
        dfResult,
        df
    )
})
    
test_that("saveDataFrame2", {
    driver <- BackendDriver$new()     
    driver$connectSnowflake(SNFLK_HOST, SNFLK_DB, SNFLK_USER, SNFLK_PASSWORD, SNFLK_SCHEMA)    
    driver$update("DROP TABLE IF EXISTS fooBar CASCADE;")
    driver$update(paste0("CREATE TABLE fooBar (bar INTEGER);"))
    driver$update(paste0("CREATE VIEW basBar AS (SELECT * FROM fooBar);"))
    # verify that the old table will get deleted even whant it has dependencies
    df <- data.frame("foo" = c(1,3,5), "bar" = c("one", "three", "five"))
    driver$saveDataFrame(df, "fooBar", rowNumbers = FALSE, incremental = FALSE)
    dfResult <- driver$select("SELECT \"foo\", \"bar\" FROM fooBar ORDER BY \"foo\"")
    df[, "bar"] <- as.character(df[, "bar"])
    
    expect_equal(
        dfResult,
        df
    )
    
    driver$update("DROP TABLE IF EXISTS fooBar CASCADE;")
    driver$update(paste0("CREATE TABLE fooBar (bar INTEGER);"))
    # verify that the old table will not get deleted
    expect_that(
        driver$saveDataFrame(df, "fooBar", rowNumbers = FALSE, incremental = TRUE),
        throws_error()
    )
    
    df <- data.frame(name = c('first', 'second'))
    driver$update("DROP TABLE IF EXISTS fooBar CASCADE;")
    driver$update(paste0("CREATE TABLE fooBar (name VARCHAR(200));"))        
    driver$saveDataFrame(df, "fooBar", rowNumbers = FALSE, incremental = TRUE)
    dfResult <- driver$select("SELECT name FROM fooBar ORDER BY name;")
    dfResult[['name']] <- as.factor(dfResult[['name']])
    expect_equal(
        df,
        dfResult
    )  
})

test_that("saveSingleRow", {
    driver <- BackendDriver$new()     
    driver$connectSnowflake(SNFLK_HOST, SNFLK_DB, SNFLK_USER, SNFLK_PASSWORD, SNFLK_SCHEMA)
    driver$update("DROP TABLE IF EXISTS fooBar CASCADE;")
    df <- data.frame("foo" = c(1), "bar" = c("one"))
    driver$saveDataFrame(df, "fooBar", rowNumbers = FALSE, incremental = FALSE)
    dfResult <- driver$select("SELECT foo, bar FROM fooBar ORDER BY foo")
    df[, "bar"] <- as.character(df[, "bar"])
    
    expect_equal(
        dfResult,
        df
    )
})

test_that("saveDataFrameFile", {
    driver <- BackendDriver$new()     
    driver$connectSnowflake(SNFLK_HOST, SNFLK_DB, SNFLK_USER, SNFLK_PASSWORD, SNFLK_SCHEMA)
    driver$update("DROP TABLE IF EXISTS fooBar CASCADE;")
    df <- read.csv(file.path(DATA_DIR, 'data1.csv'))
    df$timestamp <- as.POSIXlt(df$timestamp, tz = 'UTC')
    driver$saveDataFrame(df, "fooBar", rowNumbers = FALSE, incremental = FALSE)
    dfResult <- driver$select("SELECT \"timestamp\", anoms, expected_value FROM fooBar ORDER BY \"timestamp\";")
    dfResult$timestamp <- as.POSIXlt(df$timestamp, tz = 'UTC')
    expect_equal(nrow(df), nrow(df[which(dfResult$timestamp == df$timestamp),]))
})

test_that("saveDataFrameScientificNA", {
    driver <- BackendDriver$new()
    driver$connectSnowflake(SNFLK_HOST, SNFLK_DB, SNFLK_USER, SNFLK_PASSWORD, SNFLK_SCHEMA)
    driver$update("DROP TABLE IF EXISTS fooBar CASCADE;")
    df <- data.frame(
        id = c(1, 2, 6e+05),
        text = character(3),
        fact = c('fact1', 'fact2', 'fact2'),
        stringsAsFactors = FALSE
    )
    df[1, 'text'] <- 'a'
    df[2, 'text'] <- NA
    df[['fact']] <- factor(df[['fact']])
    driver$saveDataFrame(df, "fooBar", rowNumbers = FALSE, incremental = FALSE, forcedColumnTypes = list(id = "integer", text = "character"))
    dfResult <- driver$select("SELECT id, text FROM fooBar;")
    expect_equal(nrow(df), nrow(dfResult))
    
    driver <- BackendDriver$new()
    driver$connectSnowflake(SNFLK_HOST, SNFLK_DB, SNFLK_USER, SNFLK_PASSWORD, SNFLK_SCHEMA)
    driver$update("DROP TABLE IF EXISTS fooBar CASCADE;")
    df <- data.frame(
        id = c(1, 2, 6e+05, NA),
        fact = c(12, NA, NA, 3),
        stringsAsFactors = FALSE
    )
    driver$saveDataFrame(df, "fooBar", rowNumbers = FALSE, incremental = FALSE, forcedColumnTypes = list(id = "integer", fact = "character"))
    dfResult <- driver$select("SELECT id, fact FROM fooBar;")
    expect_equal(nrow(df), nrow(dfResult))
})

test_that("saveDataFrameLarge", {
    driver <- BackendDriver$new()     
    driver$connectSnowflake(SNFLK_HOST, SNFLK_DB, SNFLK_USER, SNFLK_PASSWORD, SNFLK_SCHEMA)
    driver$update("DROP TABLE IF EXISTS fooBar CASCADE;")
    df <- data.frame(a = rep('a', 10000), b = seq(1, 10000))
    driver$saveDataFrame(df, "fooBar", rowNumbers = FALSE, incremental = FALSE, displayProgress = FALSE)
    dfff <- driver$select("SELECT a,b FROM fooBar ORDER BY b;")
    dfResult <- driver$select("SELECT COUNT(*) AS cnt FROM fooBar;")
    expect_equal(dfResult[1, 'cnt'], nrow(df))
})


test_that("saveDataFrameEscape", {
    driver <- BackendDriver$new()
    driver$connectSnowflake(SNFLK_HOST, SNFLK_DB, SNFLK_USER, SNFLK_PASSWORD, SNFLK_SCHEMA)
    driver$update("DROP TABLE IF EXISTS fooBar CASCADE;")
    df <- data.frame(a = c('a', 'b'), b = c('foo \' bar', 'foo ; bar'))
    driver$saveDataFrame(df, "fooBar", rowNumbers = FALSE, incremental = FALSE, displayProgress = TRUE)
    dfff <- driver$select("SELECT a,b FROM fooBar ORDER BY b;")
    dfResult <- driver$select("SELECT COUNT(*) AS cnt FROM fooBar;")
    expect_equal(dfResult[1, 'cnt'], nrow(df))
})

test_that("saveDataFrameNonScalar1", {
    driver <- BackendDriver$new()
    driver$connectSnowflake(SNFLK_HOST, SNFLK_DB, SNFLK_USER, SNFLK_PASSWORD, SNFLK_SCHEMA)
    driver$update("DROP TABLE IF EXISTS fooBar CASCADE;")
    df <- data.frame(a = c('a', 'b'), stringsAsFactors = FALSE)
    df$b <- list('e', 'f')
    expect_error(
        driver$saveDataFrame(df, "fooBar", rowNumbers = FALSE, incremental = FALSE, displayProgress = TRUE),
        "non-scalar column"
    )
})

test_that("saveDataFrameNonScalar2", {
    driver <- BackendDriver$new()
    driver$connectSnowflake(SNFLK_HOST, SNFLK_DB, SNFLK_USER, SNFLK_PASSWORD, SNFLK_SCHEMA)
    driver$update("DROP TABLE IF EXISTS fooBar CASCADE;")
    df <- data.frame(a = c('a', 'b'), stringsAsFactors = FALSE)
    df$b <- list(c('a1', 'a2', 'a3'), c('b1', 'b2'))
    expect_error(
        driver$saveDataFrame(df, "fooBar", rowNumbers = FALSE, incremental = FALSE, displayProgress = TRUE),
        "non-scalar column"
    )
})
