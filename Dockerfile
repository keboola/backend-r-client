FROM quay.io/keboola/docker-base-r-packages:3.2.1-e
MAINTAINER Ondrej Popelka <ondrej.popelka@keboola.com>

RUN R -e 'library(devtools);devtools::install_github("keboola/backend-r-client")'