# DBRXR

## Project overview

`dbrxr` is a simple prototype that shows how to host and execute R code in Databricks using API calls. Databricks doesn't currently support serving endpoints for R, and this workarounds enables hosting R code with certain caveats. This functionality relies on an always-on Databricks cluster and uses the `rpy2` Python package, which is exposed via the [Databricks Command Execution API](https://docs.databricks.com/api/workspace/commandexecution) and wraps the R code.





