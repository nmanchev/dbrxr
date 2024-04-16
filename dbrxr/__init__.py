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

"""Init."""
__version__ = "0.0.1"
__author__ = "nick@manchev.org"

import requests
import logging
import json
import time

class ContextNotSetException(Exception):
    pass

class DBRXCluster(object):
    
    def __init__(self, api_url, api_token, polling_int_sec=1, rpy2="check"):

        rpy2_types = ["yes", "no", "check"]
        if rpy2 not in rpy2_types:
           raise ValueError("Invalid rpy2 type. Expected one of: %s" % rpy2_types)

        self._api_url = api_url
        # Set token and headers for API calls
        self._api_token = api_token
        self._headers = {
            "Authorization": f"Bearer {api_token}",
            "Content-Type": "application/json"
        }

        self._cluster_id = None
        self._context = None

        self.polling_int_sec = polling_int_sec
        
        self.log = logging.getLogger(__name__)

        if rpy2 == "yes":
            self.rpy2 = True
        elif rpy2 == "no":
            self.rpy2 = False
        else:
            self.rpy2 = None

    @property
    def api_token(self):
        return self._api_token

    @api_token.setter
    def api_token(self, token):
        self._api_token = token
        self._headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }

    @property
    def cluster_id(self):
        return self._cluster_id

    @cluster_id.setter
    def cluster_id(self, cluster_id):
        self._cluster_id = cluster_id

    @property
    def api_url(self):
        return self._api_url

    @api_url.setter
    def api_token(self, api_url):
        self._api_url = api_url

    def create_context(self, context:str)->bool:

        if self._context:
            self.log.error(f"Context {self._context} already exists. If you want to create a new context destroy the existing context first.")
        else:
            body = {
                "language": "python",
                "clusterId": self._cluster_id,
                "name": context
            }

            self.log.info(f"Creating Python execution context {context} on cluster {self._cluster_id}...")


            # Execute the API call to create the context
            response = requests.post(f"{self._api_url}/contexts/create", headers=self._headers, data=json.dumps(body))

            # Check the response status code
            if response.status_code == 200:
                self._context = json.loads(response.text)["id"]
                self.log.info(f"Execution context created with ID: {self._context}")

                # Do we need to check for rpy2?
                if self.rpy2 is None:
                    self.log.info("rpy2 is set to 'check'. We need to check if the package is installed in the context.")
                    self.rpy2 = self._python_package_installed("rpy2")

                return True
            else:
                self.log.error(f"Error creating execution context: {response.text}")

        return False


    def install_py_package(self, package:str, verbose=False)->bool:

        self.log.info(f"Installing Python package {package} in context {self._context}.")

        if (self._python_package_installed(package)):
        
            return True
        
        else:
            code = f"""
            import subprocess
            subprocess.check_output(['pip', 'install', '{package}'])"""
            
            ex_output = self._execute(code)
            if verbose:
                self.log.info(ex_output)

            self.log.info(f"Checking if Python package {package} installation succeeded.")
            return self._python_package_installed(package)
            
        return False

    def install_R_package(self, package:str, verbose=False)->bool:

        self.log.info(f"Installing R package {package} in context {self._context}.")

        if (self._r_package_installed(package)):
           return True       
        else:
            code = f"""
            import subprocess
            subprocess.check_output(['R', '-e', "install.packages('{package}', dependencies=TRUE, repos='http://cran.rstudio.com/')"])"""
            
            ex_output = self._execute(code)
            if verbose:
                self.log.info(ex_output)
            
            self.log.info(f"Checking if R package {package} installation succeeded.")
            return self._r_package_installed(package)
            
        return False

    def _r_package_installed(self, package:str)->bool:
 
        code = f"""
                import rpy2.robjects as robjects
                res = robjects.r('''"{package}" %in% rownames(installed.packages())''')
                print(res.r_repr())"""

        res = self._execute(code)

        if res:
            res_type = res["results"]["resultType"]
            if (res_type == "text") and (res["results"]["data"] == "TRUE"):
                self.log.info(f"Success. Package {package} is installed in context {self._context}.")
                return True
            elif (res_type == "text") and (res["results"]["data"] == "FALSE"):
                self.log.info(f"{package} is not installed in context {self._context}.")
                return False
            elif res_type == "error":
                self.log.info(f"Failure. Can't check the state of {package} in context {self._context}.")
                self.log.info(res)
                raise 
            else:
                self.log.warn(f"Can't parse the response from the execution.\n {res}")
                raise
        else:
            self.log.warn("Didn't get response from the execution.")
            raise

    def _python_package_installed(self, package:str)->bool:
        
        self.log.info(f"Checking if package {package} is installed in context {self._context}.")

        code = f"""
        try:
            import {package}
            print("Success")
        except ImportError as e:
            print("Failure")
        """

        res = self._execute(code)
        if res:
            res_type = res["results"]["resultType"]
            if (res_type == "text") and (res["results"]["data"] == "Success"):
                self.log.info(f"Success. Package {package} is installed in context {self._context}.")
                return True
            elif (res_type == "text") and (res["results"]["data"] == "Failure"):
                self.log.info(f"{package} is not installed in context {self._context}.")
                return False
            elif res_type == "error":
                self.log.info(f"Failure. Can't check the state of {package} in context {self._context}.")
                self.log.info(res)
                raise 
            else:
                self.log.warn(f"Can't parse the response from the execution.\n {res}")
                raise
        else:
            self.log.warn("Didn't get response from the execution.")
            raise

    def execute_R(self, r_code:str)->str:

        if self._context is None:
            self.log.error(f"Context doesn't exist. Can't execute commands.")
            raise ContextNotSetException

        if not self.rpy2:
            self.log.error("Can't execute R code if rpy2 isn't installed in the context.")
            return False
        
        code = f"""
        import rpy2.robjects as robjects
        res = robjects.r('''{r_code}''')
        res.r_repr()"""

        return self._execute(code)

    def _execute(self, command:str)->str:
        
        if self._context is None:
            self.log.error(f"Context doesn't exist. Can't execute commands.")
            raise ContextNotSetException
        
        if not command:
            self.log.error(f"Command is empty. Can't execute.")

        # Set the request body
        body = {
            "language": "python",
            "clusterId": self._cluster_id,
            "command": command,
            "contextId": self._context
        }

        # Execute the API call
        response = requests.post(f"{self._api_url}/commands/execute", headers=self._headers, data=json.dumps(body))

        # Check the response status code
        if response.status_code == 200:
            run_id = json.loads(response.text)["id"]
            self.log.info(f"Command submitted with run ID: {run_id}")
        else:
            self.log.error(f"Error executing command: {response.text}")

        if run_id is not None:
            
            # Get the status of the command
            body.pop("command")
            body["commandId"] = run_id
            status = "Running"

            while status == "Running" or status == "Queued":

                self.log.info(f"Command {run_id} is in {status} state...")
                time.sleep(self.polling_int_sec)

                response = requests.get(f"{self.api_url}/commands/status", headers=self._headers, params=body)

                if response.status_code == 200:
                    command_info = json.loads(response.text)
                    status = command_info["status"]
                else:
                    self.log.error(f"Error retrieving command {run_id} info: {response.text}")

            # Execution completed. Log the outcome.
            self.log.info(f"Command {run_id} completed with status: {status}")
            
            if status == "Finished":
                # Only return the results on successful completion
                return command_info

        return  None


    def destroy_context(self)->bool:

        if self._context is None:
            self.log.error(f"Context is not set. I can't destroy it.")
            raise ContextNotSetException

        # Set the request body
        body = {
            "clusterId": self._cluster_id,
            "contextId": self._context
        }

        response = requests.post(f"{self._api_url}/contexts/destroy", headers=self._headers, data=json.dumps(body))

        # Check the response status code
        if response.status_code == 200:
            self.log.info(f"Execution context {self._cluster_id} destroyed.")
            return True
        else:
            self.log.error(f"Error creating execution context: {response.text}")

        return False

