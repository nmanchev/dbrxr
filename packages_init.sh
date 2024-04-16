#!/bin/bash

# Installs rpy2 and mlflow on the cluster. It is recommended that you pin those to specific package version.

pip install rpy2

# Install mlflow and any other necessary dependencies from CRAN for your custom R code/package

R -e 'install.packages(c("mlflow"), repos = "https://cloud.r-project.org")'