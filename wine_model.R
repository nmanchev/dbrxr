##########################################################
## Project: dbrxr
## Script purpose: Create a demo MLflow-registered R model
## Date: April 2024
## Author: Nikolay Manchev
## License: GPL 3.0
##########################################################

library(mlflow)
library(httr)
library(SparkR)
library(glmnet)
library(carrier)

# Set the seed for reproducibility
set.seed(1234)

# Load the dataset
reds <- read.csv("https://archive.ics.uci.edu/ml/machine-learning-databases/wine-quality/winequality-red.csv", sep = ";")
whites <- read.csv("https://archive.ics.uci.edu/ml/machine-learning-databases/wine-quality/winequality-white.csv", sep = ";")
wine_quality <- rbind(reds, whites)

# Set some hyperparameters
model_name <- "wine_model"
data <- wine_quality
alpha <- 0.03
lambda <- 0.98
 
# Split the data into training and test sets. (0.75, 0.25) split.
sampled <- base::sample(1:nrow(data), 0.75 * nrow(data))
train <- data[sampled, ]
test <- data[-sampled, ]
 
# The predicted column is "quality" which is a scalar from [3, 9]
train_x <- as.matrix(train[, !(names(train) == "quality")])
test_x <- as.matrix(test[, !(names(train) == "quality")])
train_y <- train[, "quality"]
test_y <- test[, "quality"]

alpha <- mlflow_param("alpha", alpha, "numeric")
lambda <- mlflow_param("lambda", lambda, "numeric")

# Train and log with MLflow 
mlflow_start_run()

model <- glmnet(train_x, train_y, alpha = alpha, lambda = lambda, family= "gaussian", standardize = FALSE)
l1se <- cv.glmnet(train_x, train_y, alpha = alpha)$lambda.1se
predictor <- carrier::crate(~ glmnet::predict.glmnet(!!model, as.matrix(.x)), !!model, s = l1se)
  
predicted <- predictor(test_x)
 
rmse <- sqrt(mean((predicted - test_y) ^ 2))
mae <- mean(abs(predicted - test_y))
r2 <- as.numeric(cor(predicted, test_y) ^ 2)

message("Elasticnet model (alpha=", alpha, ", lambda=", lambda, "):")
message("  RMSE: ", rmse)
message("  MAE: ", mae)
message("  R2: ", mean(r2, na.rm = TRUE))
 
# Log the parameters associated with this run
mlflow_log_param("alpha", alpha)
mlflow_log_param("lambda", lambda)

# Log metrics we define from this run
mlflow_log_metric("rmse", rmse)
mlflow_log_metric("r2", mean(r2, na.rm = TRUE))
mlflow_log_metric("mae", mae)

# Save plot to disk
png(filename = "ElasticNet-CrossValidation.png")
plot(cv.glmnet(train_x, train_y, alpha = alpha), label = TRUE)
dev.off()

# Log that plot as an artifact
mlflow_log_artifact("ElasticNet-CrossValidation.png")

mlflow_log_model(predictor, model_name)

# Grab and print the run_id
run_id <- mlflow_get_run()$run_id
mlflow_end_run()

message(c("Model URI: \n", run_id))