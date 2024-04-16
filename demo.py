# Author: Nikolay Manchev <nick@manchev.org>
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, version 3.
#
# This program is distributed in the hope that it will be useful, but
# WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
# General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.

import os
import sys
import logging

from dbrxr import DBRXCluster

def main():

    # Set up logging
    LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
    logging_level = logging.getLevelName(LOG_LEVEL)
    logging.basicConfig(level=logging_level)
    log = logging.getLogger(__name__)

    api_token = os.environ.get("DBRX_API_TOKEN")
    databricks_host = os.environ.get("DBRX_HOST")
    cluster_id = os.environ.get("CLUSTER_ID")
    
    api_version = "1.2"
    api_url = databricks_host + "/api/" + api_version

    if (api_token == None):
        log.error("DBRX_API_TOKEN is None. No cluster id specified.")
        log.error("Program can't continue without a valid API token. This is needed to authenticate to the Databricks cluster.")
        sys.exit(1)

    cluster = DBRXCluster(api_url, api_token)
    cluster.cluster_id = cluster_id

    # Create execution context
    context_name = "my_execution_context"
    if not cluster.create_context(context_name):
        log.error("Can't create execution context. Program will terminate.")
        sys.exit(1)

    # Make sure MLflow is in the context
    cluster.install_R_package("mlflow")

    # Let's try a model trained on the Wine Quality dataset, which is already stored in MLflow
    # Update this with a relevant runid for your own model
    wine_quality_runid = "8c4da65d6bb147d2b9bfdff009dbc8eb"
    
    # Now let's write some R code that will generate some "unseen" data and call the model for inference
    r_code = f'''library(mlflow)

    # Create some test data
    column_names <- c("fixed.acidity", "volatile.acidity", "citric.acid", "residual.sugar", "chlorides", "free.sulfur.dioxide", "total.sulfur.dioxide", "density", "pH", "sulphates", "alcohol")
    data_matrix <- matrix(data =  c(7.4,0.7,0,1.9,0.076,11,34,0.9978,3.51,0.56,9.4,
                                    7.8,0.88,0,2.6,0.098,25,67,0.9968,3.2,0.68,9.8,
                                    7.8,0.76,0.04,2.3,0.092,15,54,0.997,3.26,0.65,9.8,
                                    11.2,0.28,0.56,1.9,0.075,17,60,0.998,3.16,0.58,9.8,
                                    7.4,0.7,0,1.9,0.076,11,34,0.9978,3.51,0.56,9.4),
                                    nrow = 5, ncol = length(column_names))
    colnames(data_matrix) <- column_names
    df <- as.data.frame(data_matrix)

    # Connect to MLflow and pull the model. Inject the Databricks host & token for MLflow authentication
    Sys.setenv(DATABRICKS_HOST = "{databricks_host}", "DATABRICKS_TOKEN" = "{api_token}")

    model_uri <- "runs:/{wine_quality_runid}/model"
    best_model <- mlflow_load_model(model_uri = model_uri)

    # Score the data
    predictions <- data.frame(mlflow_predict(best_model, data = df))          
    toString(predictions[[1]])
    '''

    # Run the R code and print the predictions
    predictions = cluster.execute_R(r_code)
    print(predictions["results"]["data"])

    # Destroy execution context
    cluster.destroy_context()

if __name__ == '__main__':
    main()